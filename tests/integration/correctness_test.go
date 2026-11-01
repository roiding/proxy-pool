package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"proxypool/internal/application"
	"proxypool/internal/config"
	"proxypool/internal/domain"
	"proxypool/internal/geoip"
	"proxypool/internal/storage/sqlite"
)

func firstCandidate(t *testing.T, e *env, now time.Time) domain.Candidate {
	t.Helper()
	cands, err := e.store.SelectCandidates(context.Background(), sqlite.SelectionFilters{
		Profile: "https_basic_v1", Now: now, UsefulCutoff: now, Limit: 1,
	})
	if err != nil || len(cands) != 1 {
		t.Fatalf("select candidate: %v %+v", err, cands)
	}
	return cands[0]
}

func TestLeaseOwnershipEnforced(t *testing.T) {
	j := startJudge(t)
	proxy := startFakeProxy(t)
	e := newEnv(t, j, nil)
	e.importProxy(t, "manual", proxy.URL)
	k := newKeyAPI(t, e)
	defer k.srv.Close()
	tokenA := k.createKey(t, "client-a", 0)
	tokenB := k.createKey(t, "client-b", 0)

	code, body := k.do(t, http.MethodPost, "/v1/proxies/acquire", tokenA, `{"count":1}`)
	var result application.AcquireResult
	if code != 200 || json.Unmarshal(body, &result) != nil || len(result.Items) != 1 {
		t.Fatalf("acquire as A: %d %s", code, body)
	}
	leaseID := result.Items[0].LeaseID

	// Client B must not see or change client A's lease.
	for _, action := range []string{"release", "feedback"} {
		payload := ""
		if action == "feedback" {
			payload = `{"outcome":"failure","stage":"business"}`
		}
		code, body = k.do(t, http.MethodPost, "/v1/leases/"+leaseID+"/"+action, tokenB, payload)
		if code != 404 {
			t.Fatalf("client B %s status=%d body=%s, want 404", action, code, body)
		}
	}
	code, _ = k.do(t, http.MethodPost, "/v1/leases/"+leaseID+"/release", tokenA, "")
	if code != 200 {
		t.Fatalf("owner release status=%d", code)
	}
}

func TestAcquireStopsAfterTarget(t *testing.T) {
	j := startJudge(t)
	e := newEnv(t, j, nil)
	if err := e.ingest.EnsureSource(context.Background(), "manual",
		application.SourceConfig{DefaultTransport: domain.TransportHTTP, MaxPoolLeases: 8}, true); err != nil {
		t.Fatal(err)
	}
	var lines []string
	for i := 0; i < 16; i++ {
		inner := startFakeProxy(t)
		handler := inner.Config.Handler
		slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(100 * time.Millisecond)
			handler.ServeHTTP(w, r)
		}))
		t.Cleanup(slow.Close)
		lines = append(lines, slow.URL)
	}
	if _, derr := e.ingest.ImportLines(context.Background(), "manual",
		strings.Join(lines, "\n")+"\n"); derr != nil {
		t.Fatalf("import: %v", derr)
	}

	result, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{
		Count: 2, DeadlineMS: 1500,
	})
	// Already-started probes may finish after the target is reached, so the
	// batch can slightly overshoot; the regression is stalling until the
	// deadline with zero delivered.
	if derr != nil || result.Delivered < 2 || result.Delivered > 4 {
		t.Fatalf("healthy batch stalled or overshot: %v delivered=%d", derr, result.Delivered)
	}
}

func TestAcquireUpdatesHealthOnPass(t *testing.T) {
	j := startJudge(t)
	proxy := startFakeProxy(t)
	e := newEnv(t, j, nil)
	e.importProxy(t, "manual", proxy.URL)
	result, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{Count: 1})
	if derr != nil || result.Delivered != 1 {
		t.Fatalf("acquire: %v", derr)
	}
	health, err := e.store.CurrentHealth(context.Background(),
		result.Items[0].ResourceID, "|static", "https_basic_v1")
	if err != nil || health.LastOutcome != "PASS" || health.LastPassAt == nil {
		t.Fatalf("successful delivery did not refresh health: %+v (%v)", health, err)
	}
}

func TestAcquireFailureRecordedAsFail(t *testing.T) {
	j := startJudge(t)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusProxyAuthRequired)
	}))
	defer proxy.Close()
	e := newEnv(t, j, nil)
	e.importProxy(t, "manual", proxy.URL)
	if _, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{Count: 1}); derr == nil {
		t.Fatal("expected auth failure")
	}
	stats, err := e.store.DailySourceStats(context.Background(),
		time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	if err != nil || len(stats) != 1 || stats[0].Fail != 1 {
		t.Fatalf("failed acquire not counted: err=%v stats=%+v", err, stats)
	}
}

func TestReserveRechecksEligibility(t *testing.T) {
	for _, change := range []string{"source_disabled", "source_cooling", "proxy_cooling"} {
		t.Run(change, func(t *testing.T) {
			j := startJudge(t)
			proxy := startFakeProxy(t)
			e := newEnv(t, j, nil)
			e.importProxy(t, "manual", proxy.URL)
			ctx := context.Background()
			now := time.Now()
			cand := firstCandidate(t, e, now)
			switch change {
			case "source_disabled":
				if err := e.ingest.EnsureSource(ctx, "manual",
					application.SourceConfig{MaxPoolLeases: 1}, false); err != nil {
					t.Fatal(err)
				}
			case "source_cooling":
				if _, err := e.store.RecordSourceOutcome(ctx, "manual", "FAIL", now,
					sqlite.SourceCircuitPolicy{FailureThreshold: 1, Base: time.Minute, Max: time.Minute}); err != nil {
					t.Fatal(err)
				}
			case "proxy_cooling":
				until := now.Add(time.Minute)
				if err := e.store.UpdateHealth(ctx, sqlite.HealthUpdate{
					ResourceID: cand.Resource.ID, ContextKey: "|static",
					Profile: "https_basic_v1", Outcome: "FAIL", CooldownUntil: &until, Now: now,
				}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := e.store.ReserveSlot(ctx, cand, sqlite.ReserveArgs{
				Owner: "audit", Profile: "https_basic_v1",
				Now: now, ReservationDeadline: now.Add(time.Second),
			}); err == nil {
				t.Fatalf("cached candidate reserved after %s", change)
			}
		})
	}
}

func TestSharedCapacityAllowsConcurrentCommits(t *testing.T) {
	j := startJudge(t)
	proxy := startFakeProxy(t)
	e := newEnv(t, j, nil)
	ctx := context.Background()
	if err := e.ingest.EnsureSource(ctx, "manual",
		application.SourceConfig{DefaultTransport: domain.TransportHTTP, MaxPoolLeases: 2}, true); err != nil {
		t.Fatal(err)
	}
	if _, derr := e.ingest.ImportLines(ctx, "manual", proxy.URL+"\n"); derr != nil {
		t.Fatalf("import: %v", derr)
	}
	now := time.Now()
	cand := firstCandidate(t, e, now)
	var fences []sqlite.Reserved
	for i := 0; i < 2; i++ {
		res, err := e.store.ReserveSlot(ctx, cand, sqlite.ReserveArgs{
			Owner: fmt.Sprintf("audit-%d", i), Profile: "https_basic_v1",
			Now: now, ReservationDeadline: now.Add(time.Minute),
		})
		if err != nil {
			t.Fatalf("reserve %d: %v", i, err)
		}
		fences = append(fences, res)
	}
	for i, res := range fences {
		err := e.store.CommitLease(ctx, sqlite.CommitArgs{
			ReservationID: res.ReservationID, Owner: fmt.Sprintf("audit-%d", i),
			LeaseID: fmt.Sprintf("audit-lease-%d", i), RequestID: fmt.Sprintf("audit-%d", i),
			Tenant: "default", Mode: "shared", Deadline: now.Add(30 * time.Second),
			ConnJSON: "{}", ValidationJSON: "{}",
			ExpectedFence: res.Fence, ExpectedEpoch: res.FailureEpoch,
			Profile: "https_basic_v1", Now: now,
		})
		if err != nil {
			t.Fatalf("commit %d invalidated a valid reservation: %v", i, err)
		}
	}
}

func TestDialGuardBlocksPrivateHost(t *testing.T) {
	j := startJudge(t)
	proxy := startFakeProxy(t)
	e := newEnv(t, j, func(c *config.Config) { c.AllowPrivateAddresses = false })
	e.importProxy(t, "manual", fmt.Sprintf("http://localhost:%d", portOf(proxy.URL)))
	if _, derr := e.acquire.Acquire(context.Background(),
		application.AcquireRequest{Count: 1}); derr == nil {
		t.Fatal("private host behind a DNS name must not be dialable")
	}
}

func TestCountryFilterColdStartAndFreshMatch(t *testing.T) {
	t.Run("cold_start_probes_unobserved", func(t *testing.T) {
		j := startJudge(t)
		e := newEnv(t, j, nil)
		e.importProxy(t, "manual", startFakeProxy(t).URL)
		acq := application.NewAcquireService(e.store, e.cfg, e.cfg.JudgeURL, j.certPool(),
			make(chan struct{}, e.cfg.ProbeSlots), fakeGeo{loc: geoip.Location{CountryCode: "CN"}})
		result, derr := acq.Acquire(context.Background(), application.AcquireRequest{
			Count: 1, Filters: application.Filters{Countries: []string{"CN"}},
		})
		if derr != nil || result.Delivered != 1 {
			t.Fatalf("unobserved candidate never probed: %v", derr)
		}
	})
	t.Run("fresh_country_must_match", func(t *testing.T) {
		j := startJudge(t)
		e := newEnv(t, j, nil)
		e.importProxy(t, "manual", startFakeProxy(t).URL)
		ctx := context.Background()
		cand := firstCandidate(t, e, time.Now())
		if err := e.store.UpsertObservation(ctx, cand.Resource.ID, domain.Observation{
			ExitIP: "203.0.113.99", CountryCode: "CN", ObservedAt: time.Now(),
		}); err != nil {
			t.Fatal(err)
		}
		acq := application.NewAcquireService(e.store, e.cfg, e.cfg.JudgeURL, j.certPool(),
			make(chan struct{}, e.cfg.ProbeSlots), fakeGeo{loc: geoip.Location{CountryCode: "US"}})
		if _, derr := acq.Acquire(ctx, application.AcquireRequest{
			Count: 1, Filters: application.Filters{Countries: []string{"CN"}},
		}); derr == nil {
			t.Fatal("stale CN observation must not satisfy countries=[CN] when fresh exit is US")
		}
	})
}

func TestAttemptBudgetIsHardLimit(t *testing.T) {
	j := startJudge(t)
	e := newEnv(t, j, func(c *config.Config) { c.MaxCandidateAttempts = 1; c.CandidatePage = 8 })
	for i := 0; i < 8; i++ {
		proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusBadGateway)
		}))
		t.Cleanup(proxy.Close)
		e.importProxy(t, "manual", proxy.URL)
	}
	_, _ = e.acquire.Acquire(context.Background(), application.AcquireRequest{Count: 1})
	stats, err := e.store.DailySourceStats(context.Background(),
		time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	if err != nil || len(stats) != 1 || stats[0].Proxies > 1 {
		t.Fatalf("attempt limit=1 but probed: err=%v stats=%+v", err, stats)
	}
}

func TestCooldownWindowTimestampOrdering(t *testing.T) {
	j := startJudge(t)
	proxy := startFakeProxy(t)
	e := newEnv(t, j, nil)
	e.importProxy(t, "manual", proxy.URL)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)
	cand := firstCandidate(t, e, base)
	if err := e.store.RecordConsumerDeliveries(ctx, "client-a", []string{cand.Resource.ID}, base); err != nil {
		t.Fatal(err)
	}
	now := base.Add(5500 * time.Millisecond)
	cands, err := e.store.SelectCandidates(ctx, sqlite.SelectionFilters{
		Profile: "https_basic_v1", Now: now, UsefulCutoff: now,
		ConsumerID: "client-a", ConsumerCooldown: 5 * time.Second, Limit: 1,
	})
	if err != nil || len(cands) != 1 {
		t.Fatalf("5s CD still excludes the proxy after 5.5s: err=%v cands=%d", err, len(cands))
	}
}
