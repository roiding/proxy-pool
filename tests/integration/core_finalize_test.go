package integration

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"proxypool/internal/application"
	"proxypool/internal/config"
	"proxypool/internal/domain"
)

// TestCoreFinalizeBudgetKeepsValidatedPartial pins the finalize-budget
// contract: a probe that cannot finish inside the work window never discards
// a delivery that already committed, and the whole request still ends close
// to its total timeout.
func TestCoreFinalizeBudgetKeepsValidatedPartial(t *testing.T) {
	j := startJudge(t)
	e := newEnv(t, j, func(c *config.Config) {
		c.ParallelPerRequest = 2
		c.ConnectTimeoutMS = 2500
		c.ProbeTotalTimeoutMS = 3000
	})
	good := startFakeProxy(t)
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
		http.Error(w, "slow upstream", http.StatusBadGateway)
	}))
	t.Cleanup(slow.Close)
	if err := e.ingest.EnsureSource(context.Background(), "finalize-local", application.SourceConfig{
		DefaultTransport: domain.TransportHTTP, MaxPoolLeases: 1,
	}, true); err != nil {
		t.Fatal(err)
	}
	if _, derr := e.ingest.ImportLines(context.Background(), "finalize-local",
		good.URL+"\n"+slow.URL+"\n"); derr != nil {
		t.Fatal(derr)
	}
	deadlineMS := int64(1000)
	started := time.Now()
	result, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{
		Count: 2, DeadlineMS: deadlineMS,
	})
	elapsed := time.Since(started)
	if derr != nil {
		t.Fatalf("partial delivery must be a business result, got derr: %v", derr)
	}
	if result.Delivered != 1 || result.Missing != 1 || !result.Partial || len(result.Items) != 1 {
		t.Fatalf("committed delivery was discarded at the deadline: %+v", result)
	}
	if elapsed > 1600*time.Millisecond {
		t.Fatalf("request outlived its budget: %s", elapsed)
	}
}

// TestCoreStickyFirstClaimNoDuplicateFetch pins the claim contract: two
// concurrent first requests for one project+session resolve to one fetch,
// both delivered on the same connection.
func TestCoreStickyFirstClaimNoDuplicateFetch(t *testing.T) {
	j := startJudge(t)
	proxy := startFakeProxy(t)
	e := newEnv(t, j, func(c *config.Config) { c.MaxActiveAcquires = 8 })
	e.importProxy(t, "sticky-claim", proxy.URL)
	key, _, err := e.keys.Create(context.Background(), "sticky-race", 0)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	results := make([]application.AcquireResult, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			res, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{
				Count:      1,
				ConsumerID: key.ID,
				SessionID:  "race-sess",
				Lease:      application.LeasePolicy{Mode: "sticky"},
			})
			mu.Lock()
			defer mu.Unlock()
			if derr != nil {
				t.Errorf("concurrent sticky acquire: %v", derr)
				return
			}
			results[slot] = res
		}(i)
	}
	wg.Wait()
	for _, res := range results {
		if res.Delivered != 1 {
			t.Fatalf("both requesters must be served: %+v %+v", results[0], results[1])
		}
	}
	if results[0].Items[0].Conn.Port != results[1].Items[0].Conn.Port ||
		results[0].Items[0].Conn.Host != results[1].Items[0].Conn.Host {
		t.Fatalf("claim fan-out delivered different connections: %+v vs %+v",
			results[0].Items[0].Conn, results[1].Items[0].Conn)
	}
}

// TestCoreProjectCDScopesAndZeroCannotBypass pins the fixed CD contract:
// cd=0 cannot bypass an open window, windows survive key deletion, and a
// different project's scope stays independent.
func TestCoreProjectCDScopesAndZeroCannotBypass(t *testing.T) {
	j := startJudge(t)
	proxy := startFakeProxy(t)
	e := newEnv(t, j, func(c *config.Config) { c.MaxActiveAcquires = 8 })
	e.importProxy(t, "cd-scope", proxy.URL)
	now := time.Now()
	for _, pid := range []string{"proj-a", "proj-b"} {
		if err := e.store.EnsureProject(context.Background(), domain.Project{
			ID: pid, Name: pid, Enabled: true,
		}, now); err != nil {
			t.Fatal(err)
		}
	}
	keyA, _, err := e.keys.CreateForProject(context.Background(), "proj-a", "cd-a", 0)
	if err != nil {
		t.Fatal(err)
	}
	keyB, _, err := e.keys.CreateForProject(context.Background(), "proj-b", "cd-b", 0)
	if err != nil {
		t.Fatal(err)
	}
	cd := int64(600)
	first, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{
		Count: 1, ConsumerID: keyA.ID, CDSeconds: &cd,
	})
	if derr != nil || first.Delivered != 1 {
		t.Fatalf("first delivery failed: %v %s", derr, first.ShortfallReason)
	}
	fp := e.acquire.FingerprintOf(first.Items[0].Conn)
	// Capacity is one: the lease must be released before the replay
	// attempts, exactly like a downstream client that finished using it.
	if ok, derr := e.leases.Release(context.Background(), first.Items[0].LeaseID, keyA.ID, false); derr != nil || !ok {
		t.Fatalf("release: ok=%v err=%v", ok, derr)
	}

	zero := int64(0)
	replay, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{
		Count: 1, ConsumerID: keyA.ID, CDSeconds: &zero,
	})
	if derr != nil {
		t.Fatalf("cd=0 replay must be an empty business result, got derr: %v", derr)
	}
	if replay.Delivered != 0 {
		t.Fatalf("cd=0 bypassed an open window: %+v", replay)
	}

	other, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{
		Count: 1, ConsumerID: keyB.ID, CDSeconds: &zero,
	})
	if derr != nil || other.Delivered != 1 {
		t.Fatalf("a different project must not be locked by proj-a's window: %v %s",
			derr, other.ShortfallReason)
	}

	if err := e.keys.Delete(context.Background(), keyA.ID); err != nil {
		t.Fatal(err)
	}
	blocked, err := e.store.ProjectCooldownBlocked(context.Background(), "proj-a", now, []string{fp})
	if err != nil || len(blocked) != 1 {
		t.Fatalf("key deletion must not clear the project window: %+v err=%v", blocked, err)
	}
}
