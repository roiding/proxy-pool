package application

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"proxypool/internal/domain"
)

func TestStaticSamplingDeduplicatesBeforeLimitAndCountsAllRejects(t *testing.T) {
	s := newTestIngest(t)
	var b strings.Builder
	for i := 0; i < 120; i++ {
		fmt.Fprintln(&b, "invalid-secret-user:secret-password@bad-line")
	}
	for i := 0; i < 100; i++ {
		fmt.Fprintln(&b, "127.0.0.1:10000")
	}
	for i := 1; i <= 100; i++ {
		fmt.Fprintf(&b, "127.0.0.1:%d\n", 10000+i)
	}
	list := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = fmt.Fprint(w, b.String()) }))
	defer list.Close()
	if _, derr := s.CreateSource(context.Background(), "sampled", SourceConfig{Static: &StaticConfig{URL: list.URL}, Refresh: &RefreshConfig{Mode: "demand", MaxCandidates: 20}}, true); derr != nil {
		t.Fatal(derr)
	}
	result, derr := s.RefreshStaticSource(context.Background(), "sampled")
	if derr != nil {
		t.Fatal(derr)
	}
	if result.Fetched != 200 || result.Unique != 101 || result.Retained != 20 || result.Truncated != 81 || result.RejectedCount != 120 || len(result.Rejected) != 100 || !result.Sampled {
		t.Fatalf("sampling/counts wrong: %+v", result)
	}
	for _, r := range result.Rejected {
		if strings.Contains(r.Error, "secret") || r.Input != "" {
			t.Fatalf("row error leaked credentials: %+v", r)
		}
	}
	state, err := s.store.SourceFetchState(context.Background(), "sampled")
	if err != nil || state.Fetched != 200 || state.Rejected != 120 || state.Retained != 20 {
		t.Fatalf("durable counts differ from parsed counts: %+v %v", state, err)
	}
}

func TestConditionalInstallPreservesInterveningFailAndRemoval(t *testing.T) {
	s, c := cachedTestCandidate(t)
	pass := domain.Evidence{Outcome: "PASS", CompletedAt: s.Now().Add(-2 * time.Second)}
	if err := s.RecordStaticEvidence(context.Background(), c, "scope", pass, nil); err != nil {
		t.Fatal(err)
	}
	src, err := s.store.GetSource(context.Background(), c.Offer.SourceID)
	if err != nil {
		t.Fatal(err)
	}
	f, leader, derr := s.static.beginRefresh(src.ID)
	if derr != nil || !leader {
		t.Fatal(derr)
	}
	res, entry, derr := s.refreshedEntry(src.ID, src, s.Now(), "etag", "")
	if derr != nil {
		t.Fatal(derr)
	}
	fail := pass
	fail.Outcome = "FAIL"
	fail.CompletedAt = pass.CompletedAt.Add(time.Second)
	if err := s.RecordStaticEvidence(context.Background(), c, "scope", fail, nil); err != nil {
		t.Fatal(err)
	}
	if _, derr := s.static.finishRefreshResult(src.ID, f, entry, *res, nil, true, s.Now()); derr != nil {
		t.Fatal(derr)
	}
	if _, _, ok := s.StaticRecentEvidence(c, "scope", s.Now().Add(-time.Minute)); ok {
		t.Fatal("304 overwrite resurrected older PASS")
	}
	f, leader, derr = s.static.beginRefresh(src.ID)
	if derr != nil || !leader {
		t.Fatal(derr)
	}
	res, entry, derr = s.refreshedEntry(src.ID, src, s.Now(), "etag", "")
	if derr != nil {
		t.Fatal(derr)
	}
	if err := s.RemoveStaticCandidate(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	installed, derr := s.static.finishRefreshResult(src.ID, f, entry, *res, nil, true, s.Now())
	if derr != nil {
		t.Fatal(derr)
	}
	candidates, _, _ := s.CachedStaticCandidates(src.ID)
	if installed.Retained != 0 || len(candidates) != 0 {
		t.Fatal("304 revived removed credential candidate")
	}
}

func TestConfigInvalidationFencesAlreadyPreparedCacheInstall(t *testing.T) {
	s, c := cachedTestCandidate(t)
	src, err := s.store.GetSource(context.Background(), c.Offer.SourceID)
	if err != nil {
		t.Fatal(err)
	}
	f, leader, derr := s.static.beginRefresh(src.ID)
	if derr != nil || !leader {
		t.Fatal(derr)
	}
	res, entry, derr := s.refreshedEntry(src.ID, src, s.Now(), "etag", "")
	if derr != nil {
		t.Fatal(derr)
	}
	s.static.invalidate(src.ID)
	if _, derr := s.static.finishRefreshResult(src.ID, f, entry, *res, nil, true, s.Now()); derr == nil || derr.Code != "SOURCE_CONFIG_CHANGED" {
		t.Fatalf("stale prepared batch installed: %v", derr)
	}
	if _, _, fresh := s.CachedStaticCandidates(src.ID); fresh {
		t.Fatal("invalidated flight populated cache")
	}
}

func TestStaticCacheSnapshotCannotMutateSharedState(t *testing.T) {
	s, c := cachedTestCandidate(t)
	c.Offer.Claims["malicious"] = "outside"
	*c.CacheExpiresAt = time.Time{}
	candidates, _, ok := s.CachedStaticCandidates(c.Offer.SourceID)
	if !ok || candidates[0].Offer.Claims["malicious"] != "" || candidates[0].CacheExpiresAt.IsZero() {
		t.Fatal("external candidate mutated live cache")
	}
}

func TestStaticProofDeadlineIsDetachedFromCaller(t *testing.T) {
	s, c := cachedTestCandidate(t)
	deadline := s.Now().Add(time.Minute)
	wantDeadline := deadline
	proof := domain.Evidence{Outcome: "PASS", CompletedAt: s.Now().Add(-time.Second), ValidUntil: &deadline}
	if err := s.RecordStaticEvidence(context.Background(), c, "detached-proof", proof, nil); err != nil {
		t.Fatal(err)
	}
	deadline = time.Time{}
	first, _, ok := s.StaticRecentEvidence(c, "detached-proof", s.Now().Add(-time.Minute))
	if !ok || first.ValidUntil == nil || !first.ValidUntil.Equal(wantDeadline) {
		t.Fatal("put retained the caller's mutable proof pointer")
	}
	*first.ValidUntil = time.Time{}
	second, _, ok := s.StaticRecentEvidence(c, "detached-proof", s.Now().Add(-time.Minute))
	if !ok || second.ValidUntil == nil || !second.ValidUntil.Equal(wantDeadline) {
		t.Fatal("get exposed the live proof pointer")
	}
}

func TestNewStaticPassWithChangedExitCannotAuthorizeOldObservation(t *testing.T) {
	s, c := cachedTestCandidate(t)
	old := domain.Evidence{Outcome: "PASS", CompletedAt: s.Now().Add(-2 * time.Second), ObservedExitIP: "8.8.8.8"}
	latest := old
	latest.CompletedAt = old.CompletedAt.Add(time.Second)
	latest.ObservedExitIP = "1.1.1.1"
	if err := s.RecordStaticEvidence(context.Background(), c, "exit-change", old, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordStaticEvidence(context.Background(), c, "exit-change", latest, nil); err != nil {
		t.Fatal(err)
	}
	release, eligible, err := s.LockStaticDelivery(context.Background(), []domain.Candidate{c}, []domain.Evidence{old}, "exit-change")
	if err != nil {
		t.Fatal(err)
	}
	release()
	if eligible[s.fingerprint(c)] {
		t.Fatal("new exit PASS authorized an older exit/country at delivery")
	}
	old.CacheHit = true
	release, ok, err := s.LockStaticBindingDelivery(context.Background(), c.Offer.SourceID, c.SourceConfigVersion, s.fingerprint(c), "exit-change", old, true)
	if err != nil {
		t.Fatal(err)
	}
	release()
	if ok {
		t.Fatal("new exit PASS authorized an older private binding observation")
	}
}

func TestListTTLStartsAfterFetchAndReplayIsNotHTTP304(t *testing.T) {
	s := newTestIngest(t)
	started := time.Now().UTC()
	// A separate response-time seam avoids mutable time shared with the HTTP
	// server goroutine. After request entry, subsequent clock reads advance.
	var requested atomic.Bool
	lateList := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requested.Store(true)
		_, _ = fmt.Fprint(w, "127.0.0.1:3128\n")
	}))
	defer lateList.Close()
	s.Now = func() time.Time {
		if requested.Load() {
			return started.Add(5 * time.Second)
		}
		return started
	}
	src, derr := s.CreateSource(context.Background(), "ttl-start", SourceConfig{Static: &StaticConfig{URL: lateList.URL},
		Refresh: &RefreshConfig{Mode: "demand", CacheTTLMS: 1000}}, true)
	if derr != nil {
		t.Fatal(derr)
	}
	res, derr := s.RefreshStaticSource(context.Background(), src.ID)
	if derr != nil || res.ExpiresAt == nil || !res.ExpiresAt.Equal(started.Add(5*time.Second+DefaultStaticCacheTTL)) {
		t.Fatalf("download time consumed the new TTL: %+v %v", res, derr)
	}
	replayed, derr := s.RefreshStaticSource(context.Background(), src.ID)
	if derr != nil || !replayed.CacheHit || replayed.NotModified || replayed.BatchID != res.BatchID {
		t.Fatalf("memory replay was reported as an HTTP 304: %+v %v", replayed, derr)
	}
}
