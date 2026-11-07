package application

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"proxypool/internal/domain"
)

func TestStaticURLRefreshAndCache(t *testing.T) {
	svc := newTestIngest(t)
	ctx := context.Background()
	var derr *domain.Error
	var hits atomic.Int64
	list := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("203.0.113.70:8080\n203.0.113.71:3128\nbad-line\n"))
	}))
	defer list.Close()
	src, derr := svc.CreateSource(ctx, "static-1", SourceConfig{
		Static: &StaticConfig{URL: list.URL},
	}, true)
	if derr != nil {
		t.Fatal(derr)
	}
	res, derr := svc.RefreshStaticSource(ctx, "static-1")
	if derr != nil {
		t.Fatal(derr)
	}
	if res.Fetched != 2 || len(res.Rejected) != 1 || res.BatchID == "" {
		t.Fatalf("refresh result: %+v", res)
	}
	cached, batchID, ok := svc.CachedStaticCandidates("static-1")
	if !ok || len(cached) != 2 || batchID != res.BatchID {
		t.Fatalf("cache mismatch: ok=%v batch=%s want=%s", ok, batchID, res.BatchID)
	}
	for _, cand := range cached {
		if !cand.Ephemeral || cand.SourceConfigVersion != src.ConfigVersion ||
			cand.Offer.LocalRetentionDeadline != nil ||
			cand.Offer.ExpiryBasis != "static_cache" || cand.Incarnation != batchID {
			t.Fatalf("candidate shape: %+v", cand)
		}
	}
	stats, err := svc.store.Stats(ctx)
	if err != nil || stats.EnabledOffers != 0 || stats.Resources != 0 {
		t.Fatalf("static cache leaked into pool: err=%v stats=%+v", err, stats)
	}
	if hits.Load() != 1 {
		t.Fatalf("downloads: %d", hits.Load())
	}
}

func TestStaticURLFreshCacheAvoidsRepeatedUpstreamFetch(t *testing.T) {
	svc := newTestIngest(t)
	ctx := context.Background()
	var derr *domain.Error
	var sawValidator atomic.Bool
	var hits atomic.Int64
	list := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if hits.Load() > 1 {
			if r.Header.Get("If-None-Match") != `"v1"` {
				t.Errorf("revalidation must send If-None-Match, got %q", r.Header.Get("If-None-Match"))
			}
			sawValidator.Store(true)
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"v1"`)
		_, _ = w.Write([]byte("203.0.113.80:8080\n"))
	}))
	defer list.Close()
	if _, derr := svc.CreateSource(ctx, "static-2", SourceConfig{
		Static: &StaticConfig{URL: list.URL},
	}, true); derr != nil {
		t.Fatal(derr)
	}
	if _, derr = svc.RefreshStaticSource(ctx, "static-2"); derr != nil {
		t.Fatal(derr)
	}
	svc.Now = func() time.Time { return time.Now().Add(5 * time.Second) }
	res, derr := svc.RefreshStaticSource(ctx, "static-2")
	if derr != nil {
		t.Fatal(derr)
	}
	if !res.CacheHit || res.NotModified || res.Fetched != 1 {
		t.Fatalf("revalidation result: %+v", res)
	}
	if sawValidator.Load() {
		t.Fatal("fresh cache unexpectedly called the upstream again")
	}
	// A 304 keeps the previous batch and its candidates.
	_, batchID, _ := svc.CachedStaticCandidates("static-2")
	if batchID != res.BatchID {
		t.Fatalf("304 must preserve batch: %s vs %s", batchID, res.BatchID)
	}
}

func TestStaticURLTTLExpiry(t *testing.T) {
	svc := newTestIngest(t)
	ctx := context.Background()
	var derr *domain.Error
	var hits atomic.Int64
	list := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("203.0.113.90:8080\n"))
	}))
	defer list.Close()
	if _, derr := svc.CreateSource(ctx, "static-3", SourceConfig{
		Static: &StaticConfig{URL: list.URL},
	}, true); derr != nil {
		t.Fatal(derr)
	}
	if _, derr = svc.RefreshStaticSource(ctx, "static-3"); derr != nil {
		t.Fatal(derr)
	}
	svc.Now = func() time.Time { return time.Now().Add(DefaultStaticCacheTTL + time.Second) }
	if _, _, ok := svc.CachedStaticCandidates("static-3"); ok {
		t.Fatal("expired cache must not serve")
	}
	if _, derr = svc.RefreshStaticSource(ctx, "static-3"); derr != nil {
		t.Fatal(derr)
	}
	if hits.Load() != 2 {
		t.Fatalf("expired refresh must redownload, hits=%d", hits.Load())
	}
}

func TestStaticRefreshSingleFlight(t *testing.T) {
	svc := newTestIngest(t)
	ctx := context.Background()
	var derr *domain.Error
	var hits atomic.Int64
	list := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		time.Sleep(120 * time.Millisecond)
		_, _ = w.Write([]byte("203.0.113.95:8080\n"))
	}))
	defer list.Close()
	if _, derr = svc.CreateSource(ctx, "static-4", SourceConfig{
		Static: &StaticConfig{URL: list.URL},
	}, true); derr != nil {
		t.Fatal(derr)
	}
	const workers = 4
	results := make(chan *domain.Error, workers)
	batches := make(chan string, workers)
	done := make(chan struct{})
	go func() {
		res, derr := svc.RefreshStaticSource(ctx, "static-4")
		results <- derr
		batches <- res.BatchID
		close(done)
	}()
	time.Sleep(40 * time.Millisecond)
	for i := 0; i < workers; i++ {
		go func() {
			res, derr := svc.RefreshStaticSource(ctx, "static-4")
			results <- derr
			batches <- res.BatchID
		}()
	}
	seen := map[string]int{}
	for i := 0; i < workers+1; i++ {
		if derr := <-results; derr != nil {
			t.Fatal(derr)
		}
		batch := <-batches
		if batch != "" {
			seen[batch]++
		}
	}
	if hits.Load() != 1 {
		t.Fatalf("concurrent refreshes must collapse to one download, hits=%d", hits.Load())
	}
	if len(seen) != 1 {
		t.Fatalf("workers saw different batches: %v", seen)
	}
}

func TestStaticRefreshGuards(t *testing.T) {
	svc := newTestIngest(t)
	ctx := context.Background()
	var derr *domain.Error
	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":200,"data":{"proxies":["127.0.0.1:3128"],"count":1}}`))
	}))
	defer vendor.Close()
	svc.ScdnEndpoint, svc.ScdnHTTPClient = vendor.URL, vendor.Client()
	if _, derr = svc.CreateSource(ctx, "guard-provider", SourceConfig{
		Provider: &ProviderConfig{Protocol: "http"},
	}, true); derr != nil {
		t.Fatal(derr)
	}
	if result, err := svc.RefreshStaticSource(ctx, "guard-provider"); err != nil || result.Retained != 1 {
		t.Fatalf("provider must use the shared network cache: %+v %v", result, err)
	}
	if _, derr = svc.CreateSource(ctx, "guard-upload", SourceConfig{}, true); derr != nil {
		t.Fatal(derr)
	}
	if _, derr = svc.RefreshStaticSource(ctx, "guard-upload"); derr == nil || derr.Code != "INVALID_REQUEST" {
		t.Fatalf("uploaded inventory must not use network refresh: %v", derr)
	}
	if _, derr = svc.CreateSource(ctx, "guard-static", SourceConfig{
		Static: &StaticConfig{URL: "http://127.0.0.1:1/none"},
	}, false); derr != nil {
		t.Fatal(derr)
	}
	if _, derr = svc.RefreshStaticSource(ctx, "guard-static"); derr == nil {
		t.Fatal("disabled source must refuse refresh")
	}
	if _, derr = svc.RefreshStaticSource(ctx, "missing-source"); derr == nil {
		t.Fatal("unknown source must 404")
	}
}

func TestStaticCacheByteAndFlightBounds(t *testing.T) {
	svc := newTestIngest(t)
	if err := svc.ConfigureStaticCache(time.Minute, 2, 1024); err != nil {
		t.Fatal(err)
	}
	entry := &staticEntry{batchID: "static_bound"}
	for i := 0; i < 20; i++ {
		entry.candidates = append(entry.candidates, domain.Candidate{
			Resource: domain.Resource{Conn: domain.Connection{Host: "203.0.113.1",
				Username: strings.Repeat("u", 40), Password: strings.Repeat("p", 40)}},
			Incarnation: "static_bound",
		})
	}
	now := time.Now()
	svc.static.finishRefreshResult("bound-a", &staticFlight{done: make(chan struct{})},
		entry, StaticRefreshResult{}, nil, true, now)
	if svc.static.bytes > svc.static.maxBytes || len(svc.static.entries) > svc.static.maxEntries {
		t.Fatalf("byte/entry cap violated: bytes=%d entries=%d max=%d/%d",
			svc.static.bytes, len(svc.static.entries), svc.static.maxBytes, svc.static.maxEntries)
	}
	// Every flight has a done channel and the cache remains unlocked.
	flights := []*staticFlight{}
	for i := 0; i < 2; i++ {
		f, leader, _ := svc.static.beginRefresh(string(rune('a' + i)))
		if !leader || f == nil {
			t.Fatalf("flight %d not admitted", i)
		}
		flights = append(flights, f)
	}
	if f, _, derr := svc.static.beginRefresh("third"); f != nil || derr == nil {
		t.Fatal("in-flight cache must have a hard cap")
	}
	for _, f := range flights {
		delete(svc.static.inFlight, "a")
		svc.static.mu.Lock()
		for key, cur := range svc.static.inFlight {
			if cur == f {
				delete(svc.static.inFlight, key)
			}
		}
		svc.static.mu.Unlock()
		svc.static.finishRefreshResult("", f, nil, StaticRefreshResult{}, nil, false, now)
	}
}

func TestStaticCacheInvalidationAndReplacementReleaseAccountedState(t *testing.T) {
	svc := newTestIngest(t)
	// Full candidate structs are now included in cache accounting. This test
	// exercises replacement/release, not capacity eviction, so give both
	// initial batches enough room under the corrected measurement.
	if err := svc.ConfigureStaticCache(time.Minute, 3, 65536); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	makeEntry := func(batch string, size int) *staticEntry {
		entry := &staticEntry{batchID: batch}
		for i := 0; i < size; i++ {
			entry.candidates = append(entry.candidates, domain.Candidate{
				Resource: domain.Resource{Conn: domain.Connection{
					Host: "203.0.113.1", Username: "user", Password: strings.Repeat("p", 64),
				}},
				Fingerprint: strings.Repeat("f", i+1),
				Incarnation: batch,
			})
		}
		return entry
	}
	install := func(sourceID string, entry *staticEntry) {
		t.Helper()
		f, leader, derr := svc.static.beginRefresh(sourceID)
		if derr != nil || !leader {
			t.Fatalf("begin refresh %s: leader=%t err=%v", sourceID, leader, derr)
		}
		svc.static.finishRefreshResult(sourceID, f, entry, StaticRefreshResult{}, nil, true, now)
	}
	assertCoherent := func() {
		t.Helper()
		svc.static.mu.Lock()
		defer svc.static.mu.Unlock()
		var wantBytes int64
		for id, entry := range svc.static.entries {
			wantBytes += staticEntryBytes(entry)
			count := 0
			for _, queued := range svc.static.order {
				if queued == id {
					count++
				}
			}
			if count != 1 {
				t.Fatalf("entry %q appears %d times in order %v", id, count, svc.static.order)
			}
		}
		if svc.static.bytes != wantBytes {
			t.Fatalf("bytes=%d, entries account for %d", svc.static.bytes, wantBytes)
		}
		for _, id := range svc.static.order {
			if _, ok := svc.static.entries[id]; !ok {
				t.Fatalf("order retains missing entry %q: order=%v", id, svc.static.order)
			}
		}
	}
	install("a", makeEntry("a1", 20))
	install("b", makeEntry("b1", 20))
	assertCoherent()

	svc.static.invalidate("b")
	assertCoherent()
	wantAfterInvalidate := staticEntryBytes(makeEntry("a1", 20))
	if _, ok := svc.static.entries["b"]; ok || svc.static.bytes != wantAfterInvalidate {
		t.Fatal("invalidate retained entry bytes")
	}

	// A 304 reinstall replaces the same source using fresh cachedAt but the
	// same batch identity. It must not retain the old order position or bytes.
	install("a", makeEntry("a1", 1))
	assertCoherent()
	if got := svc.static.order[len(svc.static.order)-1]; got != "a" {
		t.Fatalf("replacement was not moved to newest order: %v", svc.static.order)
	}
	if svc.static.bytes != staticEntryBytes(makeEntry("a1", 1)) {
		t.Fatalf("replacement bytes=%d", svc.static.bytes)
	}
}

func TestStaticCacheInvalidationPreservesDownloadRateWindow(t *testing.T) {
	svc := newTestIngest(t)
	now := time.Now()
	f, leader, derr := svc.static.beginRefresh("toggle")
	if derr != nil || !leader {
		t.Fatalf("begin refresh: leader=%t err=%v", leader, derr)
	}
	svc.static.finishRefreshResult("toggle", f, &staticEntry{
		batchID: "before-toggle",
		candidates: []domain.Candidate{{
			Resource: domain.Resource{Conn: domain.Connection{Host: "203.0.113.1"}},
		}},
	}, StaticRefreshResult{}, nil, true, now)
	if !svc.static.rateLimited("toggle", time.Minute, now.Add(30*time.Second)) {
		t.Fatal("rate limit must trigger inside the minInterval")
	}

	// Disabling a source removes its cached batch, but the source's download
	// rate window remains a real vendor constraint while it is toggled back on.
	svc.static.invalidate("toggle")
	if !svc.static.rateLimited("toggle", time.Minute, now.Add(500*time.Millisecond)) {
		t.Fatal("invalidate reset the source download rate window")
	}
	if svc.static.rateLimited("toggle", time.Minute, now.Add(time.Minute)) {
		t.Fatal("rate window did not expire at minInterval")
	}
}

func TestStaticSourceTogglePreservesRateLimit(t *testing.T) {
	svc := newTestIngest(t)
	var hits atomic.Int64
	list := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("203.0.113.120:8080\n"))
	}))
	defer list.Close()
	now := time.Now()
	svc.Now = func() time.Time { return now }
	ctx := context.Background()
	src, derr := svc.CreateSource(ctx, "toggle-rate", SourceConfig{
		Static: &StaticConfig{URL: list.URL},
	}, true)
	if derr != nil {
		t.Fatal(derr)
	}
	if _, derr = svc.RefreshStaticSource(ctx, src.ID); derr != nil {
		t.Fatal(derr)
	}
	if src, derr = svc.SetSourceEnabled(ctx, src.ID, false, src.ConfigVersion); derr != nil {
		t.Fatal(derr)
	}
	if src, derr = svc.SetSourceEnabled(ctx, src.ID, true, src.ConfigVersion); derr != nil {
		t.Fatal(derr)
	}
	now = now.Add(500 * time.Millisecond)
	if _, derr = svc.RefreshStaticSource(ctx, src.ID); derr == nil ||
		derr.Code != "SOURCE_RATE_LIMITED" || hits.Load() != 1 {
		t.Fatalf("same-URL toggle bypassed rate limit: err=%v hits=%d", derr, hits.Load())
	}
}

func TestStaticCacheLimitChangeFlushesStaleOrderAndEnforcesCaps(t *testing.T) {
	svc := newTestIngest(t)
	now := time.Now()
	makeEntry := func(batch string, bytes int) *staticEntry {
		return &staticEntry{
			batchID: batch,
			candidates: []domain.Candidate{{
				Resource: domain.Resource{Conn: domain.Connection{
					Host: "203.0.113.1", Password: strings.Repeat("x", bytes),
				}},
				Incarnation: batch,
			}},
		}
	}
	install := func(sourceID string, entry *staticEntry) {
		t.Helper()
		f, leader, _ := svc.static.beginRefresh(sourceID)
		if !leader {
			t.Fatalf("begin refresh %s was not a leader", sourceID)
		}
		svc.static.finishRefreshResult(sourceID, f, entry, StaticRefreshResult{}, nil, true, now)
	}
	install("kept", makeEntry("kept-batch", 200))
	install("removed", makeEntry("removed-batch", 1200))
	svc.static.invalidate("removed")
	if len(svc.static.order) != 1 || svc.static.order[0] != "kept" {
		t.Fatalf("invalidate left stale order: %v", svc.static.order)
	}

	install("second", makeEntry("second-batch", 200))
	if err := svc.ConfigureStaticCache(time.Minute, 1, 2048); err != nil {
		t.Fatal(err)
	}
	if len(svc.static.entries) != 1 || len(svc.static.order) != 1 {
		t.Fatalf("limit change did not flush to one entry: entries=%v order=%v",
			svc.static.entries, svc.static.order)
	}
	if svc.static.bytes > svc.static.maxBytes {
		t.Fatalf("limit change exceeded byte cap: %d > %d", svc.static.bytes, svc.static.maxBytes)
	}
	if _, ok := svc.static.entries[svc.static.order[0]]; !ok {
		t.Fatalf("order points at absent entry: %v", svc.static.order)
	}
}

func TestStaticRefreshFollowerCancellation(t *testing.T) {
	svc := newTestIngest(t)
	arrived, release := make(chan struct{}), make(chan struct{})
	list := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(arrived)
		<-release
		_, _ = w.Write([]byte("203.0.113.121:8080\n"))
	}))
	defer list.Close()
	if _, derr := svc.CreateSource(context.Background(), "follower-cancel", SourceConfig{
		Static: &StaticConfig{URL: list.URL},
	}, true); derr != nil {
		t.Fatal(derr)
	}
	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		_, _ = svc.RefreshStaticSource(context.Background(), "follower-cancel")
	}()
	<-arrived
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, derr := svc.RefreshStaticSource(ctx, "follower-cancel")
	if derr == nil || derr.Code != "REQUEST_CANCELED" {
		t.Fatalf("follower cancellation result: %v", derr)
	}
	if elapsed := time.Since(started); elapsed > 300*time.Millisecond {
		t.Fatalf("follower ignored its deadline for %v", elapsed)
	}
	close(release)
	<-leaderDone
}
