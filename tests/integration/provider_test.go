package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"proxypool/internal/application"
)

// newProviderEnv creates a channel backed by a fake vendor that answers with
// the SCDN envelope shape. The endpoint hook keeps tests offline.
func newProviderEnv(t *testing.T, e *env, proxies []string) *atomic.Int32 {
	t.Helper()
	var hits atomic.Int32
	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 200, "message": "",
			"data": map[string]any{"proxies": proxies, "count": len(proxies)},
		})
	}))
	t.Cleanup(vendor.Close)
	e.ingest.ScdnEndpoint = vendor.URL
	if _, derr := e.ingest.CreateSource(context.Background(), "vendor",
		application.SourceConfig{Provider: &application.ProviderConfig{Protocol: "http"}}, true); derr != nil {
		t.Fatalf("create provider channel: %v", derr)
	}
	return &hits
}

func TestProviderFetchUsesMemoryCacheWithoutImportingPool(t *testing.T) {
	j := startJudge(t)
	e := newEnv(t, j, nil)
	hits := newProviderEnv(t, e, []string{"203.0.113.100:8080", "203.0.113.101:3128"})

	res, derr := e.ingest.FetchSource(context.Background(), "vendor")
	if derr != nil {
		t.Fatalf("diagnostic fetch: %v", derr)
	}
	if !res.Diagnostic || res.CacheHit || res.Fetched != 2 || res.Added != 0 || res.BatchID == "" {
		t.Fatalf("diagnostic result: %+v", res)
	}
	cached, derr := e.ingest.FetchSource(context.Background(), "vendor")
	if derr != nil {
		t.Fatalf("cached fetch: %v", derr)
	}
	if !cached.CacheHit || cached.BatchID != res.BatchID || hits.Load() != 1 {
		t.Fatalf("second provider fetch did not reuse the memory cache: first=%+v second=%+v upstream_hits=%d",
			res, cached, hits.Load())
	}
	stats, err := e.store.Stats(context.Background())
	if err != nil || stats.EnabledOffers != 0 || stats.Resources != 0 {
		t.Fatalf("diagnostic fetch must not touch inventory: err=%v stats=%+v", err, stats)
	}
}

func TestFetchOnDemandEphemeralNotPooled(t *testing.T) {
	j := startJudge(t)
	e := newEnv(t, j, nil)
	newProviderEnv(t, e, []string{"203.0.113.110:8080", "203.0.113.110:8080", "203.0.113.111:8080"})

	cands, derr := e.ingest.FetchOnDemand(context.Background(), "vendor", 5)
	if derr != nil {
		t.Fatalf("on-demand fetch: %v", derr)
	}
	if len(cands) != 2 {
		t.Fatalf("duplicate vendor entries must dedupe: %d", len(cands))
	}
	src, err := e.store.GetSource(context.Background(), "vendor")
	if err != nil {
		t.Fatalf("get source: %v", err)
	}
	batch := cands[0].Incarnation
	for _, c := range cands {
		if !c.Ephemeral || c.SourceConfigVersion != src.ConfigVersion ||
			c.Incarnation != batch || !strings.HasPrefix(c.Incarnation, "static_") {
			t.Fatalf("on-demand candidate must bind its config version and batch: %+v", c)
		}
		if c.Offer.LocalRetentionDeadline != nil || c.Offer.ExpiryBasis != "static_cache" ||
			c.CacheExpiresAt == nil || !c.CacheExpiresAt.After(time.Now()) {
			t.Fatalf("legacy on-demand candidate must bind the shared static cache: %+v", c)
		}
	}
	// Unused candidates die with the request: nothing may persist.
	stats, err := e.store.Stats(context.Background())
	if err != nil || stats.EnabledOffers != 0 || stats.Resources != 0 {
		t.Fatalf("on-demand supply leaked into the pool: err=%v stats=%+v", err, stats)
	}
}

func TestProviderFetchAndLegacyOnDemandShareMemoryCache(t *testing.T) {
	j := startJudge(t)
	e := newEnv(t, j, nil)
	hits := newProviderEnv(t, e, []string{"203.0.113.120:8080"})

	first, derr := e.ingest.FetchSource(context.Background(), "vendor")
	if derr != nil {
		t.Fatalf("first fetch: %v", derr)
	}
	second, derr := e.ingest.FetchSource(context.Background(), "vendor")
	if derr != nil {
		t.Fatalf("second fetch: %v", derr)
	}
	if !second.CacheHit || second.BatchID != first.BatchID || hits.Load() != 1 {
		t.Fatalf("sequential fetch did not reuse cache: first=%+v second=%+v hits=%d",
			first, second, hits.Load())
	}
	cands, derr := e.ingest.FetchOnDemand(context.Background(), "vendor", 1)
	if derr != nil || len(cands) != 1 || cands[0].Incarnation != first.BatchID || hits.Load() != 1 {
		t.Fatalf("legacy on-demand wrapper did not reuse cache: candidates=%+v err=%v hits=%d",
			cands, derr, hits.Load())
	}
	// Memory cache is scoped by source instance, not shared globally by vendor.
	if _, derr := e.ingest.CreateSource(context.Background(), "vendor2",
		application.SourceConfig{Provider: &application.ProviderConfig{Protocol: "http"}}, true); derr != nil {
		t.Fatalf("create second instance: %v", derr)
	}
	if _, derr := e.ingest.FetchSource(context.Background(), "vendor2"); derr != nil || hits.Load() != 2 {
		t.Fatalf("second instance did not maintain its own cache: err=%v hits=%d", derr, hits.Load())
	}
}

func TestProviderVendorErrorExplained(t *testing.T) {
	j := startJudge(t)
	e := newEnv(t, j, nil)
	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 401, "message": "bad key"})
	}))
	defer vendor.Close()
	e.ingest.ScdnEndpoint = vendor.URL
	if _, derr := e.ingest.CreateSource(context.Background(), "vendor",
		application.SourceConfig{Provider: &application.ProviderConfig{Protocol: "http"}}, true); derr != nil {
		t.Fatalf("create: %v", derr)
	}
	if _, derr := e.ingest.FetchSource(context.Background(), "vendor"); derr == nil ||
		derr.Code != "SOURCE_AUTH_FAILED" {
		t.Fatalf("vendor auth failure must be a channel error, not a proxy death: %v", derr)
	}
	stats, err := e.store.Stats(context.Background())
	if err != nil || stats.EnabledOffers != 0 || stats.Resources != 0 {
		t.Fatalf("vendor errors must not touch inventory: %+v", stats)
	}
}
