package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"proxypool/internal/api"
	"proxypool/internal/application"
	"proxypool/internal/maintenance"
)

// startScdnVendor simulates the proxy.scdn.io get_proxy.php API with a
// canned envelope and records the query it received.
func startScdnVendor(t *testing.T, proxies []string) (*httptest.Server, func() string) {
	t.Helper()
	var lastQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lastQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code":    200,
			"message": "success",
			"data": map[string]any{
				"proxies": proxies,
				"count":   len(proxies),
			},
		})
	}))
	t.Cleanup(srv.Close)
	return srv, func() string { return lastQuery }
}

func TestProviderAPIFetchChannel(t *testing.T) {
	j := startJudge(t)
	goodProxy := startFakeProxy(t)
	vendor, lastQuery := startScdnVendor(t, []string{
		fmt.Sprintf("127.0.0.1:%d", portOf(goodProxy.URL)),
		"127.0.0.1:1",
	})
	e := newEnv(t, j, nil)
	e.ingest.ScdnEndpoint = vendor.URL
	httpSrv := httptest.NewServer(api.NewServer(e.cfg, testLogger(),
		e.acquire, e.ingest, e.leases, e.store, e.nightly, e.prewarm, e.keys).Handler())
	defer httpSrv.Close()
	bearer := func(method, path string, body string) (int, []byte) {
		var reader io.Reader
		if body != "" {
			reader = strings.NewReader(body)
		}
		req, _ := http.NewRequest(method, httpSrv.URL+path, reader)
		req.Header.Set("Authorization", "Bearer "+e.cfg.APIToken)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return resp.StatusCode, data
	}

	code, data := bearer(http.MethodPost, "/v1/sources",
		`{"source_id":"scdn","adapter_type":"provider_api","protocol":"http","fetch_count":5}`)
	if code != http.StatusCreated {
		t.Fatalf("create provider source status=%d body=%s", code, data)
	}
	code, data = bearer(http.MethodPost, "/v1/sources/scdn/fetch", "")
	if code != http.StatusOK {
		t.Fatalf("fetch status=%d body=%s", code, data)
	}
	var fetched struct {
		Requested int `json:"requested"`
		Fetched   int `json:"fetched"`
		Added     int `json:"added"`
	}
	if err := json.Unmarshal(data, &fetched); err != nil {
		t.Fatalf("decode fetch response: %v", err)
	}
	if fetched.Requested != 5 || fetched.Fetched != 2 || fetched.Added != 2 {
		t.Fatalf("fetch result: %s", data)
	}
	if q := lastQuery(); !strings.Contains(q, "protocol=http") || !strings.Contains(q, "count=5") {
		t.Fatalf("vendor query: %s", q)
	}

	overviews, err := e.store.SourcesOverview(context.Background())
	if err != nil || len(overviews) != 1 {
		t.Fatalf("sources overview: %v %+v", err, overviews)
	}
	if overviews[0].AdapterType != "provider_api" || overviews[0].ParserType != "json_mapping_v1" {
		t.Fatalf("overview: %+v", overviews[0])
	}

	// The in-memory cadence guard must reject an immediate second fetch.
	code, data = bearer(http.MethodPost, "/v1/sources/scdn/fetch", "")
	if code != http.StatusTooManyRequests {
		t.Fatalf("second fetch status=%d body=%s", code, data)
	}

	// Provider channels are validated at request time; manual sweeps reject.
	code, _ = bearer(http.MethodPost, "/v1/sources/scdn/validate", "")
	if code != http.StatusBadRequest {
		t.Fatalf("validate on provider source status=%d", code)
	}

	// Both offers go through acquire-time validation. The dead endpoint can
	// never deliver, so the request reports a shortfall — but its attributed
	// failure evidence is still recorded, which is what retirement needs.
	acquired, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{Count: 2})
	if derr == nil {
		t.Fatalf("acquire from provider channel: %v %+v", derr, acquired)
	}
	stats, err := e.store.Stats(context.Background())
	if err != nil || stats.EnabledOffers != 2 {
		t.Fatalf("failed acquire must not remove offers by itself: err=%v stats=%+v", err, stats)
	}

	// Maintenance retires provider resources that reached the failure
	// threshold via acquire evidence; the good one must survive.
	maintenance.RunOnce(context.Background(), e.store, e.cfg.AttemptRetention(), 1, testLogger())
	stats, err = e.store.Stats(context.Background())
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.EnabledOffers != 1 || stats.Resources != 1 {
		t.Fatalf("provider retirement left: %+v", stats)
	}

	// Re-fetch after the cadence window: the tombstoned dead proxy must not
	// revive, the good proxy updates in place.
	e.ingest.Now = func() time.Time { return time.Now().Add(2 * time.Second) }
	code, data = bearer(http.MethodPost, "/v1/sources/scdn/fetch", "")
	if code != http.StatusOK {
		t.Fatalf("re-fetch status=%d body=%s", code, data)
	}
	var refetched struct {
		Added   int `json:"added"`
		Updated int `json:"updated"`
		Skipped int `json:"skipped"`
	}
	if err := json.Unmarshal(data, &refetched); err != nil {
		t.Fatalf("decode re-fetch response: %v", err)
	}
	if refetched.Added != 0 || refetched.Updated != 1 || refetched.Skipped != 1 {
		t.Fatalf("re-fetch after retirement: %s", data)
	}
	stats, err = e.store.Stats(context.Background())
	if err != nil || stats.EnabledOffers != 1 || stats.Resources != 1 {
		t.Fatalf("tombstone did not hold: err=%v stats=%+v", err, stats)
	}
}

func TestProviderAPIRejectsUnmappedProtocols(t *testing.T) {
	j := startJudge(t)
	e := newEnv(t, j, nil)
	err := e.ingest.EnsureSource(context.Background(), "scdn", application.SourceConfig{
		MaxPoolLeases: 1,
		Provider:      &application.ProviderConfig{Protocol: "all"},
	}, true)
	if err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("protocol all must be rejected, got %v", err)
	}
	err = e.ingest.EnsureSource(context.Background(), "scdn", application.SourceConfig{
		MaxPoolLeases: 1,
		Provider:      &application.ProviderConfig{Protocol: "socks4"},
	}, true)
	if err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("socks4 must be rejected, got %v", err)
	}
	if _, derr := e.ingest.FetchSource(context.Background(), "missing"); derr == nil {
		t.Fatal("fetch of unknown source must fail")
	}
}

func TestAcquireDemandRefillsProviderSource(t *testing.T) {
	j := startJudge(t)
	goodProxy := startFakeProxy(t)
	vendor, lastQuery := startScdnVendor(t, []string{
		fmt.Sprintf("127.0.0.1:%d", portOf(goodProxy.URL)),
	})
	e := newEnv(t, j, nil)
	e.ingest.ScdnEndpoint = vendor.URL
	if err := e.ingest.EnsureSource(context.Background(), "scdn", application.SourceConfig{
		MaxPoolLeases: 1,
		Provider: &application.ProviderConfig{
			Protocol: "http", FetchCount: 3, MinFetchIntervalMS: 1,
		},
	}, true); err != nil {
		t.Fatalf("ensure provider source: %v", err)
	}
	// No manual fetch: acquire must pull one provider batch on demand.
	acquired, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{Count: 1})
	if derr != nil || acquired.Delivered != 1 || acquired.Items[0].SourceID != "scdn" {
		t.Fatalf("demand refill acquire: %v %+v", derr, acquired)
	}
	if q := lastQuery(); !strings.Contains(q, "protocol=http") {
		t.Fatalf("vendor was not called for demand refill: %q", q)
	}
	stats, err := e.store.Stats(context.Background())
	if err != nil || stats.EnabledOffers != 1 {
		t.Fatalf("refilled inventory: err=%v stats=%+v", err, stats)
	}
}
