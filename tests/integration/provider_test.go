package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"proxypool/internal/application"
)

// newProviderEnv creates a channel backed by a fake vendor that answers with
// the SCDN envelope shape. The endpoint hook keeps tests offline.
func newProviderEnv(t *testing.T, e *env, proxies []string) {
	t.Helper()
	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
}

func TestProviderDiagnosticFetchNeverImportsPool(t *testing.T) {
	j := startJudge(t)
	e := newEnv(t, j, nil)
	newProviderEnv(t, e, []string{"203.0.113.100:8080", "203.0.113.101:3128"})

	res, derr := e.ingest.FetchSource(context.Background(), "vendor")
	if derr != nil {
		t.Fatalf("diagnostic fetch: %v", derr)
	}
	if !res.Diagnostic || res.Fetched != 2 || res.Added != 0 {
		t.Fatalf("diagnostic result: %+v", res)
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
			c.Incarnation != batch || c.Incarnation == "" {
			t.Fatalf("on-demand candidate must bind its config version and batch: %+v", c)
		}
		if c.Offer.LocalRetentionDeadline != nil || c.Offer.ExpiryBasis != "ondemand" {
			t.Fatalf("on-demand candidate must be request-local: %+v", c)
		}
	}
	// Unused candidates die with the request: nothing may persist.
	stats, err := e.store.Stats(context.Background())
	if err != nil || stats.EnabledOffers != 0 || stats.Resources != 0 {
		t.Fatalf("on-demand supply leaked into the pool: err=%v stats=%+v", err, stats)
	}
}

func TestProviderFetchRateLimitedPerInstance(t *testing.T) {
	j := startJudge(t)
	e := newEnv(t, j, nil)
	newProviderEnv(t, e, []string{"203.0.113.120:8080"})

	if _, derr := e.ingest.FetchSource(context.Background(), "vendor"); derr != nil {
		t.Fatalf("first fetch: %v", derr)
	}
	if _, derr := e.ingest.FetchSource(context.Background(), "vendor"); derr == nil ||
		derr.Code != "SOURCE_RATE_LIMITED" {
		t.Fatalf("immediate second fetch must be rate limited: %v", derr)
	}
	// The guard is per channel instance: another instance may fetch.
	if _, derr := e.ingest.CreateSource(context.Background(), "vendor2",
		application.SourceConfig{Provider: &application.ProviderConfig{Protocol: "http"}}, true); derr != nil {
		t.Fatalf("create second instance: %v", derr)
	}
	if _, derr := e.ingest.FetchSource(context.Background(), "vendor2"); derr != nil {
		t.Fatalf("second instance must have its own interval: %v", derr)
	}
	e.ingest.Now = func() time.Time { return time.Now().Add(2 * time.Second) }
	if _, derr := e.ingest.FetchSource(context.Background(), "vendor"); derr != nil {
		t.Fatalf("fetch after interval: %v", derr)
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
		derr.Code != "SOURCE_PROVIDER_ERROR" {
		t.Fatalf("vendor auth failure must be a channel error, not a proxy death: %v", derr)
	}
	stats, err := e.store.Stats(context.Background())
	if err != nil || stats.EnabledOffers != 0 || stats.Resources != 0 {
		t.Fatalf("vendor errors must not touch inventory: %+v", stats)
	}
}
