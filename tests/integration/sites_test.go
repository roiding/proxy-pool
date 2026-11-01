package integration

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"proxypool/internal/api"
	"proxypool/internal/integrations/sites"
)

func TestSiteFetchChannelViaAPI(t *testing.T) {
	j := startJudge(t)
	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	code, data := bearer(http.MethodGet, "/v1/admin/sites", "")
	if code != http.StatusOK || !strings.Contains(string(data), "lumiproxy-elite") {
		t.Fatalf("sites catalog: %d %s", code, data)
	}

	code, data = bearer(http.MethodPost, "/v1/sources",
		`{"source_id":"lumiproxy-elite","adapter_type":"site_fetch","site_id":"lumiproxy-elite","max_pool_leases":1}`)
	if code != http.StatusCreated {
		t.Fatalf("create site source status=%d body=%s", code, data)
	}
	code, data = bearer(http.MethodPost, "/v1/sources", `{"source_id":"nope","adapter_type":"site_fetch","site_id":"missing"}`)
	if code != http.StatusBadRequest {
		t.Fatalf("unknown site must be rejected: %d %s", code, data)
	}

	code, data = bearer(http.MethodPost, "/v1/sources/lumiproxy-elite/fetch", "")
	if code != http.StatusOK {
		t.Fatalf("fetch status=%d body=%s", code, data)
	}
	var first struct {
		Fetched    int `json:"fetched"`
		Added      int `json:"added"`
		PagesDone  int `json:"pages_done"`
		PagesTotal int `json:"pages_total"`
	}
	if err := json.Unmarshal(data, &first); err != nil {
		t.Fatalf("decode fetch: %v", err)
	}
	if first.Fetched != 3 || first.Added != 3 || first.PagesDone < 2 || first.PagesTotal < 2 {
		t.Fatalf("first fetch: %s", data)
	}

	// The in-memory per-source guard rejects an immediate second fetch.
	code, _ = bearer(http.MethodPost, "/v1/sources/lumiproxy-elite/fetch", "")
	if code != http.StatusTooManyRequests {
		t.Fatalf("second fetch status=%d, want 429", code)
	}
	e.ingest.Now = func() time.Time { return time.Now().Add(20 * time.Second) }
	code, data = bearer(http.MethodPost, "/v1/sources/lumiproxy-elite/fetch", "")
	if code != http.StatusOK {
		t.Fatalf("re-fetch status=%d body=%s", code, data)
	}
	var again struct {
		Added   int `json:"added"`
		Updated int `json:"updated"`
	}
	if err := json.Unmarshal(data, &again); err != nil || again.Added != 0 || again.Updated != 3 {
		t.Fatalf("re-fetch result: %s (%v)", data, err)
	}

	overviews, err := e.store.SourcesOverview(context.Background())
	if err != nil || len(overviews) != 1 || overviews[0].AdapterType != "site_fetch" {
		t.Fatalf("overview: %v %+v", err, overviews)
	}
	stats, err := e.store.Stats(context.Background())
	if err != nil || stats.EnabledOffers != 3 || stats.Resources != 3 {
		t.Fatalf("site import into pool: err=%v stats=%+v", err, stats)
	}
}
