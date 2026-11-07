package integration

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"proxypool/internal/api"
	"proxypool/internal/integrations/sites"
)

func TestSiteFetchChannelViaAPI(t *testing.T) {
	j := startJudge(t)
	var vendorHits atomic.Int64
	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		vendorHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Query().Get("page") {
		case "1":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{
				"list": []map[string]any{
					{"ip": "203.0.113.60", "port": 8080, "protocol": 1},
					{"ip": "203.0.113.61", "port": 3128, "protocol": 2},
				}, "page_count": 2}})
		case "2":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{
				"list": []map[string]any{
					{"ip": "203.0.113.60", "port": 8080, "protocol": 1},
					{"ip": "203.0.113.62", "port": 80, "protocol": 1},
				}}})
		default:
			_, _ = w.Write([]byte("no data"))
		}
	}))
	defer vendor.Close()

	e := newEnv(t, j, nil)
	e.ingest.SiteOverrides = map[string]sites.Site{
		"lumiproxy-elite": {
			ID: "lumiproxy-elite", Name: "LumiProxy(高匿) test", Anonymity: sites.AnonymityElite,
			Kind: sites.ParserJSONList, URL: vendor.URL + "/?page={page}", AutoPages: true, Pages: 1, DelayMS: 10,
		},
	}
	httpSrv := httptest.NewServer(api.NewServer(e.cfg, testLogger(),
		e.acquire, e.ingest, e.leases, e.store, e.nightly, e.prewarm, e.keys).Handler())
	defer httpSrv.Close()
	bearer := func(method, path, body string) (int, []byte) {
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

	// The catalog endpoint lists every built-in site.
	code, data := bearer(http.MethodGet, "/api/v1/admin/sites", "")
	if code != http.StatusOK || !strings.Contains(string(data), "lumiproxy-elite") {
		t.Fatalf("sites catalog: %d %s", code, data)
	}

	code, data = bearer(http.MethodPost, "/api/v1/admin/sources",
		`{"source_id":"lumiproxy-elite","adapter_type":"site_fetch","config":{"site":{"site_id":"lumiproxy-elite"},"max_pool_leases":1}}`)
	if code != http.StatusCreated {
		t.Fatalf("create site source status=%d body=%s", code, data)
	}
	code, data = bearer(http.MethodPost, "/api/v1/admin/sources", `{"source_id":"nope","adapter_type":"site_fetch","config":{"site":{"site_id":"missing"}}}`)
	if code != http.StatusBadRequest {
		t.Fatalf("unknown site must be rejected: %d %s", code, data)
	}

	code, data = bearer(http.MethodPost, "/api/v1/admin/sources/lumiproxy-elite/fetch", "")
	if code != http.StatusOK || strings.Contains(string(data), "not_modified") {
		t.Fatalf("first fetch status=%d body=%s", code, data)
	}
	var first struct {
		Fetched    int `json:"fetched"`
		PagesDone  int `json:"pages_done"`
		PagesTotal int `json:"pages_total"`
	}
	if err := json.Unmarshal(data, &first); err != nil {
		t.Fatalf("decode fetch: %v", err)
	}
	if first.Fetched != 4 || first.PagesDone < 2 || first.PagesTotal < 2 {
		t.Fatalf("first fetch: %s", data)
	}

	// Site entries live in the bounded static cache, never the pool.
	cached, batchID, ok := e.ingest.CachedStaticCandidates("lumiproxy-elite")
	if !ok || len(cached) != 3 || batchID == "" {
		t.Fatalf("static cache after fetch: ok=%v batch=%q entries=%d", ok, batchID, len(cached))
	}
	for _, cand := range cached {
		if !cand.Ephemeral || cand.SourceConfigVersion != 1 ||
			cand.Offer.LocalRetentionDeadline != nil ||
			cand.Offer.ExpiryBasis != "static_cache" || cand.Incarnation != batchID {
			t.Fatalf("static cache candidate shape: %+v", cand)
		}
	}

	// Under the R30 import semantics an immediate second fetch no longer
	// 429s when the cache is fresh: the vendor window blocks real downloads,
	// and the response explicitly replays the same batch with no additional
	// vendor calls.
	code, data = bearer(http.MethodPost, "/api/v1/admin/sources/lumiproxy-elite/fetch", "")
	var second struct {
		Fetched     int    `json:"fetched"`
		BatchID     string `json:"batch_id"`
		NotModified bool   `json:"not_modified"`
		CacheHit    bool   `json:"cache_hit"`
	}
	if code != http.StatusOK || json.Unmarshal(data, &second) != nil {
		t.Fatalf("second fetch status=%d body=%s", code, data)
	}
	if second.NotModified || !second.CacheHit || second.BatchID != batchID || second.Fetched != 4 {
		t.Fatalf("second fetch must replay the fresh batch without a download: %+v", second)
	}
	if vendorHits.Load() != 2 {
		t.Fatalf("both pages must be fetched exactly once, hits=%d", vendorHits.Load())
	}
	// A read path inside the 30-minute cache TTL may keep serving this batch
	// even after the 15-second vendor refresh window has elapsed.
	e.ingest.Now = func() time.Time { return time.Now().Add(20 * time.Second) }
	if _, fetchBatch, derr := e.ingest.FetchStaticBatch(context.Background(),
		"lumiproxy-elite", 0); derr != nil || fetchBatch != batchID {
		t.Fatalf("fresh cache read after the vendor window: batch=%q err=%v", fetchBatch, derr)
	}
	if vendorHits.Load() != 2 {
		t.Fatalf("cache-TTL read must not trigger a download, hits=%d", vendorHits.Load())
	}
	// Manual diagnostics may refresh after the vendor window even while the
	// cache remains inside its longer serving TTL.
	code, data = bearer(http.MethodPost, "/api/v1/admin/sources/lumiproxy-elite/fetch", "")
	var again struct {
		Fetched     int    `json:"fetched"`
		BatchID     string `json:"batch_id"`
		NotModified bool   `json:"not_modified"`
	}
	if code != http.StatusOK || json.Unmarshal(data, &again) != nil {
		t.Fatalf("re-fetch status=%d body=%s", code, data)
	}
	if again.NotModified || again.Fetched != 4 || again.BatchID == "" ||
		again.BatchID != batchID {
		t.Fatalf("manual refresh inside the cache window must reuse the same batch: %s", data)
	}
	if vendorHits.Load() != 2 {
		t.Fatalf("manual refresh must not repeat the same upstream fetch, hits=%d", vendorHits.Load())
	}
	batchID = again.BatchID
	// Once the cache TTL expires, the read path must also refresh.
	expires := *e.ingest.CacheView("lumiproxy-elite").ExpiresAt
	e.ingest.Now = func() time.Time { return expires.Add(time.Second) }
	third, thirdBatch, derr := e.ingest.FetchStaticBatch(context.Background(),
		"lumiproxy-elite", 0)
	if derr != nil || len(third) != 3 || thirdBatch == "" || thirdBatch == batchID {
		t.Fatalf("TTL-expired batch fetch must download a fresh batch: entries=%d batch=%q err=%v",
			len(third), thirdBatch, derr)
	}
	if vendorHits.Load() != 4 {
		t.Fatalf("TTL-expired batch fetch must download both pages once, hits=%d",
			vendorHits.Load())
	}

	overviews, err := e.store.SourcesOverview(context.Background())
	if err != nil || len(overviews) != 1 || overviews[0].AdapterType != "site_fetch" {
		t.Fatalf("overview: %v %+v", err, overviews)
	}
	stats, err := e.store.Stats(context.Background())
	if err != nil || stats.EnabledOffers != 0 || stats.Resources != 0 {
		t.Fatalf("site entries must not enter the pool: err=%v stats=%+v", err, stats)
	}
}
