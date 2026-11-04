package application

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// TestStaticRefreshAfterFlightServesFreshBatch reproduces the reported
// TestStaticRefreshSingleFlight SOURCE_RATE_LIMITED failure: a caller whose
// beginRefresh lands after the previous flight completed (a straggler
// follower under race scheduling, or a sequential retry inside the vendor
// window) previously became a new leader, hit the just-stamped rate window,
// and received SOURCE_RATE_LIMITED even though a fresh batch was cached.
func TestStaticRefreshAfterFlightServesFreshBatch(t *testing.T) {
	svc := newTestIngest(t)
	ctx := context.Background()
	var hits atomic.Int64
	list := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("203.0.113.97:8080\n"))
	}))
	defer list.Close()
	if _, derr := svc.CreateSource(ctx, "static-straggler", SourceConfig{
		Static: &StaticConfig{URL: list.URL},
	}, true); derr != nil {
		t.Fatal(derr)
	}
	first, derr := svc.RefreshStaticSource(ctx, "static-straggler")
	if derr != nil {
		t.Fatal(derr)
	}
	// The second refresh lands inside the vendor rate window but the cache
	// holds a fresh batch: it must be served, not rejected.
	second, derr := svc.RefreshStaticSource(ctx, "static-straggler")
	if derr != nil {
		t.Fatalf("fresh batch must satisfy a refresh inside the rate window: %v", derr)
	}
	if second.BatchID != first.BatchID || second.Fetched != first.Fetched {
		t.Fatalf("fresh-cache refresh returned a different batch: %+v vs %+v", second, first)
	}
	if hits.Load() != 1 {
		t.Fatalf("serving the fresh batch must not redownload, hits=%d", hits.Load())
	}
}

// TestStaticRefreshRateLimitedWithoutFreshBatch keeps the vendor window
// meaningful: with no cached batch (invalidated or never installed) the
// refresh still reports SOURCE_RATE_LIMITED instead of silently downloading.
func TestStaticRefreshRateLimitedWithoutFreshBatch(t *testing.T) {
	svc := newTestIngest(t)
	ctx := context.Background()
	var hits atomic.Int64
	list := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("203.0.113.98:8080\n"))
	}))
	defer list.Close()
	src, derr := svc.CreateSource(ctx, "static-window", SourceConfig{
		Static: &StaticConfig{URL: list.URL},
	}, true)
	if derr != nil {
		t.Fatal(derr)
	}
	if _, derr = svc.RefreshStaticSource(ctx, src.ID); derr != nil {
		t.Fatal(derr)
	}
	svc.static.invalidate(src.ID)
	if _, derr = svc.RefreshStaticSource(ctx, src.ID); derr == nil ||
		derr.Code != "SOURCE_RATE_LIMITED" {
		t.Fatalf("rate window without a fresh batch must be reported: err=%v hits=%d",
			derr, hits.Load())
	}
	if hits.Load() != 1 {
		t.Fatalf("rate-limited refresh must not download, hits=%d", hits.Load())
	}
}

// TestStaticRefreshFollowerStragglerJoinsFreshBatch drives the exact
// single-flight interleaving with a deterministic straggler: the follower
// starts while the leader downloads but only reaches beginRefresh after the
// flight has finished, which under the old code produced SOURCE_RATE_LIMITED.
func TestStaticRefreshFollowerStragglerJoinsFreshBatch(t *testing.T) {
	svc := newTestIngest(t)
	ctx := context.Background()
	list := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("203.0.113.99:8080\n"))
	}))
	defer list.Close()
	if _, derr := svc.CreateSource(ctx, "static-join", SourceConfig{
		Static: &StaticConfig{URL: list.URL},
	}, true); derr != nil {
		t.Fatal(derr)
	}
	// Pre-install a flight that looks in-flight to the follower, finish it,
	// then let the follower begin: it must serve the installed fresh batch.
	f, leader, derr := svc.static.beginRefresh("static-join")
	if derr != nil || !leader {
		t.Fatalf("begin refresh: leader=%v err=%v", leader, derr)
	}
	src, err := svc.store.GetSource(ctx, "static-join")
	if err != nil {
		t.Fatal(err)
	}
	_, entry, derr := svc.buildStaticEntry(src,
		SourceConfig{Static: &StaticConfig{URL: list.URL}}, "static-join",
		svc.Now(), nil, nil, 0, 0, "", "")
	if derr != nil {
		t.Fatal(derr)
	}
	svc.static.finishRefreshResult("static-join", f, entry, StaticRefreshResult{}, nil, true, svc.Now())
	if _, derr = svc.RefreshStaticSource(ctx, "static-join"); derr != nil {
		t.Fatalf("straggler follower must serve the fresh batch: %v", derr)
	}
}
