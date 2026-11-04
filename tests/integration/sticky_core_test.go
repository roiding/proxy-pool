package integration

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"proxypool/internal/application"
	"proxypool/internal/config"
	"proxypool/internal/domain"
	"proxypool/internal/storage/sqlite"
)

func TestStickyContractValidation(t *testing.T) {
	j := startJudge(t)
	e := newEnv(t, j, nil)
	cases := []struct {
		name string
		req  application.AcquireRequest
	}{
		{"sticky_without_session", application.AcquireRequest{Count: 1,
			Lease: application.LeasePolicy{Mode: "sticky"}}},
		{"sticky_with_two_connections", application.AcquireRequest{Count: 2,
			Lease: application.LeasePolicy{Mode: "sticky"}, SessionID: "s"}},
		{"ordinary_with_session", application.AcquireRequest{Count: 1, SessionID: "s"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, derr := e.acquire.Acquire(context.Background(), tc.req); derr == nil ||
				derr.Status != 400 {
				t.Fatalf("invalid sticky contract accepted: %v", derr)
			}
		})
	}
}

func stickySession(t *testing.T, e *env, keyID, session string, cd int64) application.AcquireResult {
	t.Helper()
	result, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{
		Count:      1,
		ConsumerID: keyID,
		SessionID:  session,
		Lease:      application.LeasePolicy{Mode: "sticky"},
		CDSeconds:  &cd,
	})
	if derr != nil {
		t.Fatalf("sticky acquire: %v", derr)
	}
	return result
}

func TestStickyReusesOwnConnectionInsideCooldown(t *testing.T) {
	j := startJudge(t)
	proxy := startFakeProxy(t)
	e := newEnv(t, j, nil)
	e.importProxy(t, "manual", proxy.URL)
	key, _, err := e.keys.Create(context.Background(), "sticky-client", 0)
	if err != nil {
		t.Fatal(err)
	}
	first := stickySession(t, e, key.ID, "sess-1", 600)
	if first.Delivered != 1 {
		t.Fatalf("first sticky delivery: %s", first.ShortfallReason)
	}
	fp := e.acquire.FingerprintOf(first.Items[0].Conn)
	blocked, err := e.store.ProjectCooldownBlocked(context.Background(),
		sqlite.DefaultProjectID, time.Now(), []string{fp})
	if err != nil || len(blocked) != 1 {
		t.Fatalf("delivery must open the project window: %+v err=%v", blocked, err)
	}
	windowBefore := blocked[fp]

	// The same session reuses its bound connection inside the CD window.
	second := stickySession(t, e, key.ID, "sess-1", 600)
	if second.Delivered != 1 || second.Items[0].Conn.Port != first.Items[0].Conn.Port {
		t.Fatalf("sticky reuse drifted: first=%+v second=%+v", first.Items[0].Conn, second.Items[0].Conn)
	}
	if !second.Items[0].Validation.CacheHit {
		t.Fatal("sticky reuse must be marked as cached validation")
	}
	still, _ := e.store.ProjectCooldownBlocked(context.Background(),
		sqlite.DefaultProjectID, time.Now(), []string{fp})
	if !still[fp].Equal(windowBefore) {
		t.Fatalf("reuse must not extend the CD window: %s -> %s", windowBefore, still[fp])
	}

	// A different session in the same project is blocked by the window.
	other := stickySession(t, e, key.ID, "sess-2", 600)
	if other.Delivered != 0 || other.ShortfallReason == "" {
		t.Fatalf("another session must not bypass project CD: %+v", other)
	}
}

func TestStickyRebindsWhenBindingExpires(t *testing.T) {
	j := startJudge(t)
	firstProxy, secondProxy := startFakeProxy(t), startFakeProxy(t)
	e := newEnv(t, j, nil)
	if err := e.ingest.EnsureSource(context.Background(), "manual",
		application.SourceConfig{DefaultTransport: domain.TransportHTTP, MaxPoolLeases: 4}, true); err != nil {
		t.Fatal(err)
	}
	if _, derr := e.ingest.ImportLines(context.Background(), "manual",
		firstProxy.URL+"\n"+secondProxy.URL); derr != nil {
		t.Fatal(derr)
	}
	key, _, err := e.keys.Create(context.Background(), "sticky-rebind", 0)
	if err != nil {
		t.Fatal(err)
	}
	first := stickySession(t, e, key.ID, "sess-1", 600)
	if first.Delivered != 1 {
		t.Fatalf("first sticky delivery: %s", first.ShortfallReason)
	}
	ctx := context.Background()
	if _, err := e.store.DB().ExecContext(ctx,
		`UPDATE sticky_bindings SET deadline = ? WHERE session_id = 'sess-1'`,
		e.acquire.Now().Add(-time.Second).Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	second := stickySession(t, e, key.ID, "sess-1", 600)
	if second.Delivered != 1 {
		t.Fatalf("expired binding must rebind within budget: %s", second.ShortfallReason)
	}
	binding, err := e.store.GetBinding(ctx, sqlite.DefaultProjectID, "sess-1")
	if err != nil {
		t.Fatal(err)
	}
	if binding.Deadline.Before(e.acquire.Now()) {
		t.Fatalf("rebinding stored a lapsed deadline: %+v", binding)
	}
	if binding.Fingerprint == "" || binding.ConnectionCiphertext == "" {
		t.Fatalf("binding payload incomplete: %+v", binding)
	}
	// The rebind delivery must have opened its own CD window.
	blocked, _ := e.store.ProjectCooldownBlocked(ctx, sqlite.DefaultProjectID,
		e.acquire.Now(), []string{binding.Fingerprint})
	if len(blocked) == 0 {
		t.Fatalf("rebind delivery must open its own CD window: %+v", binding)
	}
}

func TestCoreStickyReuseRejectsFuturePublicProof(t *testing.T) {
	j := startJudge(t)
	proxy := startFakeProxy(t)
	e := newEnv(t, j, nil)
	e.importProxy(t, "core-sticky-future-proof", proxy.URL)
	key, _, err := e.keys.Create(context.Background(), "core-sticky-future-proof", 0)
	if err != nil {
		t.Fatal(err)
	}
	first := stickySession(t, e, key.ID, "core-future-proof", 600)
	if first.Delivered != 1 {
		t.Fatalf("initial sticky delivery: %s", first.ShortfallReason)
	}
	ctx := context.Background()
	b, err := e.store.GetBinding(ctx, sqlite.DefaultProjectID, "core-future-proof")
	if err != nil {
		t.Fatal(err)
	}
	scope := domain.ValidationScope{
		Mode: b.Validation.Mode, TargetHostPort: b.Validation.Target,
		Profile: b.Validation.Profile, SecurityHash: b.Validation.SecurityHash,
	}
	future := b.Validation
	future.AttemptID = "core-future-public-proof"
	future.StartedAt = time.Now().UTC()
	future.CompletedAt = future.StartedAt.Add(time.Hour)
	if err := e.store.PutEvidence(ctx, sqlite.EvidenceKey{
		ResourceID: b.ResourceID, Fingerprint: b.Fingerprint, SourceID: b.SourceID,
		SourceConfigVersion: b.SourceConfigVersion, Incarnation: b.Incarnation,
	}, domain.EvidenceScopeHash(scope), future); err != nil {
		t.Fatal(err)
	}
	connJSON, err := json.Marshal(first.Items[0].Conn)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	_, err = e.store.CommitStickyReuse(ctx, sqlite.StickyReuseArgs{
		ProjectID: b.ProjectID, SessionID: b.SessionID, ExpectedVersion: b.Version,
		KeyID: key.ID, ConsumerID: key.ID, Fingerprint: b.Fingerprint,
		SourceID: b.SourceID, SourceConfigVersion: b.SourceConfigVersion,
		ResourceID: b.ResourceID, Incarnation: b.Incarnation,
		ScopeHash: domain.EvidenceScopeHash(scope), Proof: b.Validation,
		MaxProofAge: e.cfg.MaxProofAge(), RequestID: "req_core_future_proof",
		LeaseID: "lease_core_future_proof", Mode: "sticky",
		LeaseDeadline: now.Add(e.cfg.LeaseTTL()), ConnJSON: string(connJSON),
		Now: now, MinUsable: e.cfg.MinUsable(),
		BindingCiphertext: b.ConnectionCiphertext,
		DeliverySafety:    e.cfg.ExpirySafetyMargin(),
	})
	if !errors.Is(err, sqlite.ErrNotEligible) {
		t.Fatalf("future public proof must be rejected, got %v", err)
	}
}

func startStickyAuthRaceProxy(t *testing.T) (*httptest.Server, <-chan struct{}, func()) {
	t.Helper()
	arrived, resume := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	var entered, released sync.Once
	unblock := func() { released.Do(func() { close(resume) }) }
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			dst, err := net.DialTimeout("tcp", r.Host, 2*time.Second)
			if err != nil {
				http.Error(w, "upstream unreachable", http.StatusBadGateway)
				return
			}
			hj, ok := w.(http.Hijacker)
			if !ok {
				dst.Close()
				http.Error(w, "hijack unsupported", http.StatusInternalServerError)
				return
			}
			conn, buf, err := hj.Hijack()
			if err != nil {
				dst.Close()
				return
			}
			defer conn.Close()
			defer dst.Close()
			if _, err := conn.Write([]byte(
				"HTTP/1.1 200 Connection Established\r\n\r\n")); err != nil {
				return
			}
			done := make(chan struct{}, 2)
			go func() { _, _ = io.Copy(dst, buf); done <- struct{}{} }()
			go func() { _, _ = io.Copy(conn, dst); done <- struct{}{} }()
			<-done
			return
		}
		entered.Do(func() { close(arrived) })
		select {
		case <-resume:
			w.Header().Set("Proxy-Authenticate", `Basic realm="core"`)
			http.Error(w, "authentication expired", http.StatusProxyAuthRequired)
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(unblock)
	return srv, arrived, unblock
}

func TestCoreStickyAuthRevalidationCannotRetireChangedSource(t *testing.T) {
	j := startJudge(t)
	proxy, arrived, unblock := startStickyAuthRaceProxy(t)
	e := newEnv(t, j, func(c *config.Config) {
		c.MaxProofAgeMS = 300
		c.EvidenceCacheTTLMS = 300
		c.ConnectTimeoutMS = 1000
		c.ProbeTotalTimeoutMS = 2000
	})
	const sourceID = "core-sticky-auth-race"
	e.importProxy(t, sourceID, proxy.URL)
	key, _, err := e.keys.Create(context.Background(), "core-sticky-auth-race", 0)
	if err != nil {
		t.Fatal(err)
	}
	first := stickySession(t, e, key.ID, "core-auth-race", 600)
	if first.Delivered != 1 {
		t.Fatalf("initial sticky delivery: %s", first.ShortfallReason)
	}
	time.Sleep(400 * time.Millisecond)

	type outcome struct {
		result application.AcquireResult
		err    *domain.Error
	}
	done := make(chan outcome, 1)
	go func() {
		result, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{
			Count: 1, ConsumerID: key.ID, SessionID: "core-auth-race",
			Lease: application.LeasePolicy{Mode: "sticky"}, CDSeconds: int64Ptr(600),
		})
		done <- outcome{result: result, err: derr}
	}()
	select {
	case <-arrived:
	case <-time.After(2 * time.Second):
		t.Fatal("sticky proof refresh never reached the proxy")
	}
	ctx := context.Background()
	source, err := e.store.GetSource(ctx, sourceID)
	if err != nil {
		t.Fatal(err)
	}
	source.Enabled = false
	if err := e.store.UpdateSource(ctx, source, source.ConfigVersion, time.Now()); err != nil {
		t.Fatal(err)
	}
	unblock()
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("source change during AUTH probe became a storage error: %v", got.err)
		}
		if got.result.Delivered != 0 {
			t.Fatalf("disabled source delivered after AUTH revalidation: %+v", got.result)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("sticky AUTH revalidation did not complete")
	}
	stats, err := e.store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Resources != 1 || stats.EnabledOffers != 1 {
		t.Fatalf("stale AUTH probe retired the replacement source offer: %+v", stats)
	}
}

func int64Ptr(v int64) *int64 { return &v }
