package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"proxypool/internal/application"
	"proxypool/internal/config"
	"proxypool/internal/domain"
	"proxypool/internal/geoip"
	"proxypool/internal/storage/sqlite"
)

type coreLoadGeo struct {
	location geoip.Location
}

func (g coreLoadGeo) Lookup(netip.Addr) (geoip.Location, bool) {
	return g.location, true
}

func corePrivateStickyEnv(t *testing.T) (*env, *application.AcquireService,
	chan struct{}, string, application.AcquireRequest) {
	t.Helper()
	j := startJudge(t)
	e := newEnv(t, j, func(c *config.Config) {
		c.MaxProofAgeMS = 200
		c.EvidenceCacheTTLMS = 200
		c.ConnectTimeoutMS = 500
		c.ProbeTotalTimeoutMS = 1500
		c.MinUsableMS = 100
	})
	proxy := startFakeProxy(t)
	var fetches atomic.Int32
	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fetches.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 200, "data": map[string]any{"count": 1,
				"proxies": []string{fmt.Sprintf("127.0.0.1:%d", portOf(proxy.URL))}},
		})
	}))
	t.Cleanup(vendor.Close)
	e.ingest.ScdnEndpoint, e.ingest.ScdnHTTPClient = vendor.URL, vendor.Client()
	const sourceID = "core-load-private-sticky"
	if _, err := e.ingest.CreateSource(context.Background(), sourceID,
		application.SourceConfig{
			MaxPoolLeases: 1,
			Provider:      &application.ProviderConfig{Protocol: "http", FetchCount: 1},
		}, true); err != nil {
		t.Fatal(err)
	}
	probeSlots := make(chan struct{}, 1)
	e.acquire = application.NewAcquireService(e.store, e.cfg, e.cfg.JudgeURL,
		j.certPool(), probeSlots, coreLoadGeo{
			location: geoip.Location{CountryCode: "US"},
		})
	e.acquire.Refiller = e.ingest
	cd := int64(600)
	req := application.AcquireRequest{
		Count: 1, ProjectID: application.DefaultAcquireProject,
		ConsumerID: application.DefaultAcquireProject, CDSeconds: &cd,
		DeadlineMS: 3000, SessionID: "core-load-saturated-refresh",
		Lease:   application.LeasePolicy{Mode: "sticky"},
		Filters: application.Filters{SourceIDs: []string{sourceID}, Countries: []string{"US"}},
	}
	first, derr := e.acquire.Acquire(context.Background(), req)
	if derr != nil || first.Delivered != 1 {
		t.Fatalf("initial private sticky delivery: result=%+v err=%v", first, derr)
	}
	time.Sleep(time.Until(first.Items[0].Validation.CompletedAt.Add(
		e.cfg.MaxProofAge() + 50*time.Millisecond)))
	return e, e.acquire, probeSlots, sourceID, req
}

func coreFirstClaimEnv(t *testing.T) (*env, *application.AcquireService,
	chan struct{}, string, application.AcquireRequest) {
	t.Helper()
	j := startJudge(t)
	e := newEnv(t, j, func(c *config.Config) {
		c.ConnectTimeoutMS = 500
		c.ProbeTotalTimeoutMS = 1500
		c.MinUsableMS = 100
	})
	proxy := startFakeProxy(t)
	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 200, "data": map[string]any{"count": 1,
				"proxies": []string{fmt.Sprintf("127.0.0.1:%d", portOf(proxy.URL))}},
		})
	}))
	t.Cleanup(vendor.Close)
	e.ingest.ScdnEndpoint, e.ingest.ScdnHTTPClient = vendor.URL, vendor.Client()
	const sourceID = "core-load-first-claim"
	if _, err := e.ingest.CreateSource(context.Background(), sourceID,
		application.SourceConfig{
			MaxPoolLeases: 1,
			Provider:      &application.ProviderConfig{Protocol: "http", FetchCount: 1},
		}, true); err != nil {
		t.Fatal(err)
	}
	probeSlots := make(chan struct{}, 1)
	e.acquire = application.NewAcquireService(e.store, e.cfg, e.cfg.JudgeURL,
		j.certPool(), probeSlots, coreLoadGeo{
			location: geoip.Location{CountryCode: "US"},
		})
	e.acquire.Refiller = e.ingest
	cd := int64(600)
	req := application.AcquireRequest{
		Count: 1, ProjectID: application.DefaultAcquireProject,
		ConsumerID: application.DefaultAcquireProject, CDSeconds: &cd,
		DeadlineMS: 2000, SessionID: "core-load-canceled-first-claim",
		Lease:   application.LeasePolicy{Mode: "sticky"},
		Filters: application.Filters{SourceIDs: []string{sourceID}, Countries: []string{"US"}},
	}
	return e, e.acquire, probeSlots, sourceID, req
}

func TestCoreCanceledFirstClaimReleasesClaimBounded(t *testing.T) {
	e, acquire, probeSlots, _, req := coreFirstClaimEnv(t)
	probeSlots <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	result, derr := acquire.Acquire(ctx, req)
	if derr != nil || result.Delivered != 0 || result.ShortfallReason == "" {
		t.Fatalf("canceled first claim must be an explained business shortfall: result=%+v err=%v", result, derr)
	}
	deadline := time.Now().Add(1500 * time.Millisecond)
	state := ""
	for {
		binding, err := e.store.GetBinding(context.Background(), req.ProjectID, req.SessionID)
		if err != nil {
			t.Fatal(err)
		}
		state = binding.State
		if state != sqlite.BindingClaiming {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("canceled claim kept the dead CLAIMING owner beyond the bounded cleanup window: state=%s", state)
		}
		time.Sleep(25 * time.Millisecond)
	}
	if state != sqlite.BindingExpired {
		t.Fatalf("released claim must be claimable again: state=%s", state)
	}
	<-probeSlots
	var recovered application.AcquireResult
	var rderr *domain.Error
	recoverDeadline := time.Now().Add(4 * time.Second)
	for {
		recovered, rderr = acquire.Acquire(context.Background(), req)
		if rderr == nil && recovered.Delivered == 1 {
			break
		}
		// The demand refill honors the per-source fetch interval, so the
		// first bounded retries may legitimately return an empty result.
		if time.Now().After(recoverDeadline) {
			t.Fatalf("session did not recover after the canceled claim was released: result=%+v err=%v", recovered, rderr)
		}
		time.Sleep(250 * time.Millisecond)
	}
	binding, err := e.store.GetBinding(context.Background(), req.ProjectID, req.SessionID)
	if err != nil || binding.State != sqlite.BindingActive {
		t.Fatalf("recovery did not activate the binding: binding=%+v err=%v", binding, err)
	}
}

func TestCoreConcurrentStickyRefreshUnderSaturation(t *testing.T) {
	e, acquire, probeSlots, _, req := corePrivateStickyEnv(t)
	ctx := context.Background()
	before, err := e.store.GetBinding(ctx, req.ProjectID, req.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	probeSlots <- struct{}{}
	type answer struct {
		result application.AcquireResult
		derr   *domain.Error
	}
	answers := make(chan answer, 2)
	for i := 0; i < 2; i++ {
		go func() {
			result, derr := acquire.Acquire(ctx, req)
			answers <- answer{result, derr}
		}()
	}
	for i := 0; i < 2; i++ {
		out := <-answers
		if out.derr != nil || out.result.Delivered != 0 || out.result.ShortfallReason == "" {
			t.Fatalf("saturated concurrent refresh must return explained shortfalls: result=%+v err=%v", out.result, out.derr)
		}
	}
	after, err := e.store.GetBinding(ctx, req.ProjectID, req.SessionID)
	if err != nil || after.State != sqlite.BindingActive || after.Version != before.Version ||
		after.Fingerprint != before.Fingerprint ||
		after.ConnectionCiphertext != before.ConnectionCiphertext ||
		!after.Deadline.Equal(before.Deadline) {
		t.Fatalf("concurrent saturation damaged the recoverable binding: before=%+v after=%+v err=%v", before, after, err)
	}
	blocked, err := e.store.ProjectCooldownBlocked(ctx, req.ProjectID, time.Now(),
		[]string{before.Fingerprint})
	if err != nil || len(blocked) != 1 {
		t.Fatalf("project CD was lost under concurrent saturation: %+v err=%v", blocked, err)
	}
	<-probeSlots
	recovered, derr := acquire.Acquire(ctx, req)
	recoverDeadline := time.Now().Add(6 * time.Second)
	for {
		if derr == nil && recovered.Delivered == 1 && len(recovered.Items) == 1 &&
			recovered.BindingVersion == before.Version && !recovered.SessionChanged &&
			e.acquire.FingerprintOf(recovered.Items[0].Conn) == before.Fingerprint {
			break
		}
		// Under load the fresh proof probe can miss one request's budget;
		// the binding must recover on a bounded business retry either way.
		if time.Now().After(recoverDeadline) {
			t.Fatalf("binding did not recover once the probe slot freed: result=%+v err=%v", recovered, derr)
		}
		time.Sleep(250 * time.Millisecond)
		recovered, derr = acquire.Acquire(ctx, req)
	}
}

func TestCoreSaturatedStickyRefreshPreservesRecoverableBinding(t *testing.T) {
	e, acquire, probeSlots, sourceID, req := corePrivateStickyEnv(t)
	ctx := context.Background()
	before, err := e.store.GetBinding(ctx, req.ProjectID, req.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	circuitBefore := coreCircuitSnapshot(t, e.store, sourceID)
	probeSlots <- struct{}{}

	cancelCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	result, derr := acquire.Acquire(cancelCtx, req)
	if derr != nil {
		t.Fatalf("saturated/canceled refresh returned a transport/storage error: %v", derr)
	}
	if result.Delivered != 0 || result.Missing != 1 ||
		result.ShortfallReason != "VALIDATION_BUDGET_EXHAUSTED" {
		t.Fatalf("saturated/canceled refresh did not return a bounded business shortfall: %+v", result)
	}
	after, err := e.store.GetBinding(ctx, req.ProjectID, req.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if after.State != sqlite.BindingActive || after.Version != before.Version ||
		after.Fingerprint != before.Fingerprint ||
		after.ConnectionCiphertext != before.ConnectionCiphertext ||
		!after.Deadline.Equal(before.Deadline) {
		t.Fatalf("budget exhaustion destroyed or rewrote the recoverable binding: before=%+v after=%+v", before, after)
	}
	circuitAfter := coreCircuitSnapshot(t, e.store, sourceID)
	if circuitAfter != circuitBefore {
		t.Fatalf("request cancellation mutated source health: before=%+v after=%+v",
			circuitBefore, circuitAfter)
	}
	blocked, err := e.store.ProjectCooldownBlocked(ctx, req.ProjectID,
		time.Now(), []string{before.Fingerprint})
	if err != nil || len(blocked) != 1 {
		t.Fatalf("original project CD was lost across saturated refresh: %+v err=%v", blocked, err)
	}
}

type coreCircuitSnapshotValue struct {
	exists        bool
	state         string
	failures      int
	openCount     int
	cooldownUntil string
	lastOutcome   string
	lastSuccess   string
	lastFailure   string
	updatedAt     string
}

func coreCircuitSnapshot(t *testing.T, store *sqlite.Store, sourceID string) coreCircuitSnapshotValue {
	t.Helper()
	var snapshot coreCircuitSnapshotValue
	err := store.DB().QueryRow(`
		SELECT state, consecutive_failures, open_count,
		       COALESCE(cooldown_until, ''), last_outcome,
		       COALESCE(last_success_at, ''), COALESCE(last_failure_at, ''), updated_at
		FROM source_circuits WHERE source_id = ?`, sourceID).Scan(
		&snapshot.state, &snapshot.failures, &snapshot.openCount,
		&snapshot.cooldownUntil, &snapshot.lastOutcome,
		&snapshot.lastSuccess, &snapshot.lastFailure, &snapshot.updatedAt)
	if err == sql.ErrNoRows {
		return coreCircuitSnapshotValue{}
	}
	if err != nil {
		t.Fatal(err)
	}
	snapshot.exists = true
	return snapshot
}
