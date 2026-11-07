package application

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"proxypool/internal/config"
	"proxypool/internal/domain"
	"proxypool/internal/storage/sqlite"
)

func cachedTestCandidate(t *testing.T) (*IngestService, domain.Candidate) {
	t.Helper()
	s := newTestIngest(t)
	list := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("127.0.0.1:32123\n")) }))
	t.Cleanup(list.Close)
	if _, err := s.CreateSource(context.Background(), "scoped-list", SourceConfig{Static: &StaticConfig{URL: list.URL}}, true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RefreshStaticSource(context.Background(), "scoped-list"); err != nil {
		t.Fatal(err)
	}
	candidates, _, ok := s.CachedStaticCandidates("scoped-list")
	if !ok || len(candidates) != 1 {
		t.Fatal("missing cached fixture")
	}
	return s, candidates[0]
}

func TestStaticEvidenceScopeFailureAndReplacement(t *testing.T) {
	s, c := cachedTestCandidate(t)
	now := s.Now()
	pass := domain.Evidence{Outcome: "PASS", CompletedAt: now.Add(-2 * time.Second), Mode: "tls", Target: "example.com:443", Profile: "target_tls_v1"}
	if err := s.RecordStaticEvidence(context.Background(), c, "scope-a", pass, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := s.StaticRecentEvidence(c, "scope-b", now.Add(-time.Minute)); ok {
		t.Fatal("different target scope reused proof")
	}
	if _, _, ok := s.StaticRecentEvidence(c, "scope-a", now.Add(-time.Minute)); !ok {
		t.Fatal("matching scope lost proof")
	}
	fail := pass
	fail.Outcome = "FAIL"
	fail.CompletedAt = now.Add(-time.Second)
	if err := s.RecordStaticEvidence(context.Background(), c, "scope-a", fail, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := s.StaticRecentEvidence(c, "scope-a", now.Add(-time.Minute)); ok {
		t.Fatal("newer failure left PASS reusable")
	}
	if err := s.RecordStaticEvidence(context.Background(), c, "scope-a", pass, nil); err == nil {
		t.Fatal("out-of-order PASS overwrote failure")
	}
	release, eligible, err := s.LockStaticDelivery(context.Background(), []domain.Candidate{c}, []domain.Evidence{pass}, "scope-a")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if eligible[s.fingerprint(c)] {
		t.Fatal("final guard accepted newer FAIL")
	}
}

func TestStaticDeliveryFenceHasBoundedWriterWait(t *testing.T) {
	s, c := cachedTestCandidate(t)
	pass := domain.Evidence{Outcome: "PASS", CompletedAt: s.Now().Add(-time.Second)}
	if err := s.RecordStaticEvidence(context.Background(), c, "scope", pass, nil); err != nil {
		t.Fatal(err)
	}
	release, eligible, err := s.LockStaticDelivery(context.Background(), []domain.Candidate{c}, []domain.Evidence{pass}, "scope")
	if err != nil || !eligible[s.fingerprint(c)] {
		t.Fatalf("guard failed: %v %v", eligible, err)
	}
	fail := pass
	fail.Outcome = "FAIL"
	fail.CompletedAt = pass.CompletedAt.Add(time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	if err := s.RecordStaticEvidence(ctx, c, "scope", fail, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("writer did not honor deadline: %v", err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("writer blocked beyond request budget")
	}
	release()
	if err := s.RecordStaticEvidence(context.Background(), c, "scope", fail, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := s.StaticRecentEvidence(c, "scope", s.Now().Add(-time.Minute)); ok {
		t.Fatal("failure after commit fence release did not supersede proof")
	}
}

func TestStaticSourceAccessStopsFetchBeforeIO(t *testing.T) {
	s := newTestIngest(t)
	var hits atomic.Int64
	list := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1); _, _ = w.Write([]byte("127.0.0.1:3128\n")) }))
	defer list.Close()
	if _, err := s.CreateSource(context.Background(), "private", SourceConfig{Static: &StaticConfig{URL: list.URL}, Access: &SourceAccessConfig{ProjectIDs: []string{"owner"}}}, true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Scoped(context.Background(), "private", 1, RefillScope{ProjectID: "outsider"}); err == nil || err.Code != "SOURCE_NOT_ALLOWED" {
		t.Fatalf("private source allowed: %v", err)
	}
	if hits.Load() != 0 {
		t.Fatal("unauthorized project triggered private API I/O")
	}
}

func TestIntervalRefreshPersistsCadenceAcrossRestart(t *testing.T) {
	s := newTestIngest(t)
	var hits atomic.Int64
	list := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1); _, _ = w.Write([]byte("127.0.0.1:3128\n")) }))
	defer list.Close()
	now := time.Now()
	s.Now = func() time.Time { return now }
	if _, err := s.CreateSource(context.Background(), "scheduled", SourceConfig{Static: &StaticConfig{URL: list.URL}, Refresh: &RefreshConfig{Mode: "interval", IntervalMS: 1000, CacheTTLMS: 2000}}, true); err != nil {
		t.Fatal(err)
	}
	r := NewSourceRefreshService(s.store, s, config.Default())
	r.Now = s.Now
	if err := r.RunDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 1 {
		t.Fatalf("scheduled fetch count %d", hits.Load())
	}
	if err := r.RunDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 1 {
		t.Fatal("scheduler repeated a not-due download")
	}
	restarted := NewIngestService(s.store, "test-secret", true)
	restarted.Now = func() time.Time { return now }
	r2 := NewSourceRefreshService(s.store, restarted, config.Default())
	r2.Now = restarted.Now
	if err := r2.RunDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 1 {
		t.Fatal("restart erased vendor cadence")
	}
	now = now.Add(1100 * time.Millisecond)
	if err := r2.RunDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 2 {
		t.Fatal("due source did not recover after restart")
	}
	stats, err := s.store.Stats(context.Background())
	if err != nil || stats.Resources != 0 || stats.EnabledOffers != 0 {
		t.Fatalf("refresh leaked persistent stock: %+v %v", stats, err)
	}
}

func TestStaticBindingGuardDoesNotUseAProofAfterEviction(t *testing.T) {
	s, c := cachedTestCandidate(t)
	pass := domain.Evidence{Outcome: "PASS", CompletedAt: s.Now().Add(-time.Second)}
	if err := s.RecordStaticEvidence(context.Background(), c, "scope", pass, nil); err != nil {
		t.Fatal(err)
	}
	release, eligible, err := s.LockStaticBindingDelivery(context.Background(), c.Offer.SourceID,
		c.SourceConfigVersion, s.fingerprint(c), "scope", pass, true)
	if err != nil || !eligible {
		t.Fatalf("current freshly validated binding was rejected: %v %v", eligible, err)
	}
	release()
	s.static.invalidate(c.Offer.SourceID)
	release, eligible, err = s.LockStaticBindingDelivery(context.Background(), c.Offer.SourceID,
		c.SourceConfigVersion, s.fingerprint(c), "scope", pass, true)
	if err != nil {
		t.Fatal(err)
	}
	release()
	if eligible {
		t.Fatal("a proof known to belong to a removed list batch was reused")
	}
	// A used binding may be revalidated independently of disposable list
	// inventory. This permits a real probe, not reuse of its old private PASS.
	release, eligible, err = s.LockStaticBindingDelivery(context.Background(), c.Offer.SourceID,
		c.SourceConfigVersion, s.fingerprint(c), "scope", pass, false)
	if err != nil {
		t.Fatal(err)
	}
	release()
	if !eligible {
		t.Fatal("real bound-connection validation unnecessarily required inventory")
	}
}

func TestStaticAuthRemovalInvalidatesOnlyOwnedBindingSnapshots(t *testing.T) {
	s, c := cachedTestCandidate(t)
	ctx := context.Background()
	other, derr := s.CreateSource(ctx, "independent-source", SourceConfig{Static: &StaticConfig{URL: "https://example.com/list.txt"}}, true)
	if derr != nil {
		t.Fatal(derr)
	}
	for _, source := range []struct {
		id      string
		version int64
	}{
		{c.Offer.SourceID, c.SourceConfigVersion}, {other.ID, other.ConfigVersion},
	} {
		b := domain.StickyBinding{ProjectID: "owner", SessionID: source.id, SourceID: source.id,
			SourceConfigVersion: source.version, Incarnation: c.Incarnation, Fingerprint: s.fingerprint(c),
			ConnectionCiphertext: "synthetic-sealed-snapshot", Deadline: s.Now().Add(time.Minute)}
		if err := s.store.PutBinding(ctx, b, 0, s.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.store.DB().Exec(`INSERT INTO project_proxy_cd(project_id,connection_fingerprint,issued_at,blocked_until) VALUES(?,?,?,?)`,
		"owner", s.fingerprint(c), s.Now().Format(time.RFC3339Nano), s.Now().Add(time.Hour).Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveStaticCandidate(ctx, c); err != nil {
		t.Fatal(err)
	}
	owned, err := s.store.GetBinding(ctx, "owner", c.Offer.SourceID)
	if err != nil || owned.State != "INVALID" || owned.ConnectionCiphertext != "" {
		t.Fatalf("AUTH left the owned secret snapshot reusable: %+v %v", owned, err)
	}
	independent, err := s.store.GetBinding(ctx, "owner", other.ID)
	if err != nil || independent.State != sqlite.BindingActive || independent.ConnectionCiphertext == "" {
		t.Fatalf("list removal invalidated an independent source: %+v %v", independent, err)
	}
	var cdCount int
	if err := s.store.DB().QueryRow(`SELECT COUNT(*) FROM project_proxy_cd WHERE project_id='owner'`).Scan(&cdCount); err != nil || cdCount != 1 {
		t.Fatalf("AUTH reset project CD: %d %v", cdCount, err)
	}
}
