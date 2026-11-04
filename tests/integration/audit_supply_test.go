package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"proxypool/internal/application"
	"proxypool/internal/domain"
	"proxypool/internal/geoip"
	"proxypool/internal/integrations/sites"
)

func TestAuditDefaultSupplyPrefersOnDemandAPI(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	e.importProxy(t, "audit-upload-fallback", startFakeProxy(t).URL)
	apiProxy := startFakeProxy(t)
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 200, "data": map[string]any{"count": 1,
				"proxies": []string{fmt.Sprintf("127.0.0.1:%d", portOf(apiProxy.URL))}},
		})
	}))
	t.Cleanup(provider.Close)
	e.ingest.ScdnEndpoint, e.ingest.ScdnHTTPClient = provider.URL, provider.Client()
	if err := e.ingest.EnsureSource(context.Background(), "audit-api-preferred", application.SourceConfig{
		MaxPoolLeases: 1, Provider: &application.ProviderConfig{Protocol: "http", FetchCount: 1},
	}, true); err != nil {
		t.Fatal(err)
	}
	srv := auditServer(t, e)
	status, data := auditPost(t, srv, e.cfg.APIToken, "/api/v1/proxies/acquire", `{"count":1}`)
	var result application.AcquireResult
	if status != http.StatusOK || json.Unmarshal(data, &result) != nil || result.Delivered != 1 {
		t.Fatalf("healthy supply did not deliver: status=%d body=%s", status, data)
	}
	if result.Items[0].SourceID != "audit-api-preferred" {
		t.Fatalf("default scheduler queued uploaded inventory ahead of the eligible API: %s", data)
	}
}

func TestAuditFailedAPICandidatesDoNotEndSupplySearch(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "audit proxy authentication failure", http.StatusProxyAuthRequired)
	}))
	t.Cleanup(bad.Close)
	good := startFakeProxy(t)
	var badCalls, goodCalls atomic.Int32
	var calls atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxy := bad
		// Source selection may be randomized. Whichever source is first gets
		// the bad candidate, so only continued supply search can succeed.
		if calls.Add(1) > 1 {
			proxy = good
			goodCalls.Add(1)
		} else {
			badCalls.Add(1)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 200, "data": map[string]any{"count": 1,
				"proxies": []string{fmt.Sprintf("127.0.0.1:%d", portOf(proxy.URL))}},
		})
	}))
	t.Cleanup(provider.Close)
	e.ingest.ScdnEndpoint, e.ingest.ScdnHTTPClient = provider.URL, provider.Client()
	for _, item := range []struct{ id, country string }{{"audit-api-a-bad", "US"}, {"audit-api-z-good", "CN"}} {
		if err := e.ingest.EnsureSource(context.Background(), item.id, application.SourceConfig{
			MaxPoolLeases: 1, Provider: &application.ProviderConfig{Protocol: "http", CountryCode: item.country, FetchCount: 1},
		}, true); err != nil {
			t.Fatal(err)
		}
	}
	srv := auditServer(t, e)
	status, data := auditPost(t, srv, e.cfg.APIToken, "/api/v1/proxies/acquire", `{"count":1,"timeout":2}`)
	var result application.AcquireResult
	if status != http.StatusOK || json.Unmarshal(data, &result) != nil || result.Delivered != 1 ||
		len(result.Items) != 1 || result.Items[0].Conn.Port != portOf(good.URL) ||
		badCalls.Load() == 0 || goodCalls.Load() == 0 {
		t.Fatalf("a fetched-but-invalid API batch prevented the next eligible source: status=%d body=%s bad_calls=%d good_calls=%d",
			status, data, badCalls.Load(), goodCalls.Load())
	}
}

func TestAuditCountryRefillAllowsFreshExitVerification(t *testing.T) {
	for _, kind := range []string{"api", "static"} {
		t.Run(kind, func(t *testing.T) {
			j := startJudge(t)
			e := newEnv(t, j, nil)
			proxy := startFakeProxy(t)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if kind == "static" {
					_, _ = fmt.Fprintf(w, "%s\n", proxy.URL)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"code": 200, "data": map[string]any{"count": 1,
						"proxies": []string{fmt.Sprintf("127.0.0.1:%d", portOf(proxy.URL))}},
				})
			}))
			t.Cleanup(upstream.Close)
			source := application.SourceConfig{MaxPoolLeases: 1}
			if kind == "api" {
				e.ingest.ScdnEndpoint, e.ingest.ScdnHTTPClient = upstream.URL, upstream.Client()
				source.Provider = &application.ProviderConfig{Protocol: "http", CountryCode: "CN", FetchCount: 1}
			} else {
				source.Static = &application.StaticConfig{URL: upstream.URL, DefaultTransport: domain.TransportHTTP}
			}
			if err := e.ingest.EnsureSource(context.Background(), "audit-country-refill", source, true); err != nil {
				t.Fatal(err)
			}
			e.acquire = application.NewAcquireService(e.store, e.cfg, j.url(), j.certPool(),
				make(chan struct{}, e.cfg.ProbeSlots), fakeGeo{loc: geoip.Location{CountryCode: "CN"}})
			e.acquire.Refiller = e.ingest
			srv := auditServer(t, e)
			status, data := auditPost(t, srv, e.cfg.APIToken, "/api/v1/proxies/acquire",
				`{"count":1,"country":["CN"],"source_ids":["audit-country-refill"]}`)
			var result application.AcquireResult
			if status != http.StatusOK || json.Unmarshal(data, &result) != nil || result.Delivered != 1 ||
				len(result.Items) != 1 || result.Items[0].Observed == nil || result.Items[0].Observed.CountryCode != "CN" {
				t.Fatalf("%s refill discarded an unobserved candidate before fresh exit verification: status=%d body=%s", kind, status, data)
			}
		})
	}
}

func TestAuditNonUploadSourcesCannotImportPersistentInventory(t *testing.T) {
	for _, kind := range []string{"api", "static", "site"} {
		t.Run(kind, func(t *testing.T) {
			e := newEnv(t, startJudge(t), nil)
			cfg := application.SourceConfig{MaxPoolLeases: 1}
			switch kind {
			case "api":
				cfg.Provider = &application.ProviderConfig{Protocol: "http", FetchCount: 1}
			case "static":
				cfg.Static = &application.StaticConfig{URL: "https://example.com/audit-list.txt", DefaultTransport: domain.TransportHTTP}
			case "site":
				cfg.Site = &application.SiteConfig{SiteID: sites.Catalog[0].ID, MaxPages: 1}
			}
			id := "audit-non-upload-" + kind
			if err := e.ingest.EnsureSource(context.Background(), id, cfg, true); err != nil {
				t.Fatal(err)
			}
			srv := auditServer(t, e)
			status, data := auditPost(t, srv, e.cfg.APIToken, "/api/v1/admin/sources/"+id+"/imports",
				`{"content":"http://127.0.0.1:8081\n"}`)
			if status < 400 || status >= 500 {
				t.Errorf("%s source accepted persistent upload: status=%d body=%s", kind, status, data)
			}
			stats, err := e.store.Stats(context.Background())
			if err != nil || stats.Resources != 0 || stats.EnabledOffers != 0 {
				t.Fatalf("%s source gained public inventory through the import endpoint: stats=%+v err=%v", kind, stats, err)
			}
		})
	}
}

func TestAuditStaticURLSuppliesAcquisitionWithoutPublicInventory(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(fmt.Sprintf("cached-%t", cached), func(t *testing.T) {
			e := newEnv(t, startJudge(t), nil)
			proxy := startFakeProxy(t)
			list := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = fmt.Fprintf(w, "%s\n", proxy.URL)
			}))
			t.Cleanup(list.Close)
			if _, derr := e.ingest.CreateSource(context.Background(), "audit-static", application.SourceConfig{
				MaxPoolLeases: 1, Static: &application.StaticConfig{URL: list.URL, DefaultTransport: domain.TransportHTTP},
			}, true); derr != nil {
				t.Fatal(derr)
			}
			if cached {
				if _, derr := e.ingest.RefreshStaticSource(context.Background(), "audit-static"); derr != nil {
					t.Fatal(derr)
				}
			}
			srv := auditServer(t, e)
			status, data := auditPost(t, srv, e.cfg.APIToken, "/api/v1/proxies/acquire",
				`{"count":1,"source_ids":["audit-static"]}`)
			var result application.AcquireResult
			if status != http.StatusOK || json.Unmarshal(data, &result) != nil || result.Delivered != 1 {
				t.Fatalf("registered static cache is disconnected from acquisition: status=%d body=%s", status, data)
			}
			stats, err := e.store.Stats(context.Background())
			if err != nil || stats.Resources != 0 || stats.EnabledOffers != 0 {
				t.Fatalf("static download became persistent inventory: stats=%+v err=%v", stats, err)
			}
		})
	}
}

func TestAuditExpiredStaticCacheCanRevalidateWith304(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	var hits atomic.Int64
	list := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("ETag", `"audit-v1"`)
		if r.Header.Get("If-None-Match") == `"audit-v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		_, _ = w.Write([]byte("203.0.113.12:8080\n"))
	}))
	t.Cleanup(list.Close)
	if _, derr := e.ingest.CreateSource(context.Background(), "audit-static-304", application.SourceConfig{
		Static: &application.StaticConfig{URL: list.URL},
	}, true); derr != nil {
		t.Fatal(derr)
	}
	now := time.Now()
	e.ingest.Now = func() time.Time { return now }
	first, derr := e.ingest.RefreshStaticSource(context.Background(), "audit-static-304")
	if derr != nil || first.Fetched != 1 {
		t.Fatalf("initial list download: result=%+v err=%v", first, derr)
	}
	now = now.Add(application.DefaultStaticCacheTTL + time.Second)
	if _, _, ok := e.ingest.CachedStaticCandidates("audit-static-304"); ok {
		t.Fatal("expired cache was directly offered to consumers")
	}
	second, derr := e.ingest.RefreshStaticSource(context.Background(), "audit-static-304")
	if derr != nil || !second.NotModified || second.Fetched != 1 || second.BatchID != first.BatchID {
		t.Fatalf("304 after TTL could not refresh retained content: result=%+v err=%v", second, derr)
	}
	if _, _, ok := e.ingest.CachedStaticCandidates("audit-static-304"); !ok || hits.Load() != 2 {
		t.Fatalf("successful revalidation did not refresh the bounded cache: ready=%t hits=%d", ok, hits.Load())
	}
}

func TestAuditStaticRefreshFollowerHonorsItsOwnCancellation(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	arrived, release := make(chan struct{}), make(chan struct{})
	list := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(arrived)
		<-release
		_, _ = w.Write([]byte("203.0.113.12:8080\n"))
	}))
	t.Cleanup(list.Close)
	if _, derr := e.ingest.CreateSource(context.Background(), "audit-static-cancel", application.SourceConfig{
		Static: &application.StaticConfig{URL: list.URL},
	}, true); derr != nil {
		t.Fatal(derr)
	}
	leader := make(chan struct{})
	go func() {
		_, _ = e.ingest.RefreshStaticSource(context.Background(), "audit-static-cancel")
		close(leader)
	}()
	<-arrived
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	follower := make(chan *domain.Error, 1)
	go func() {
		_, derr := e.ingest.RefreshStaticSource(ctx, "audit-static-cancel")
		follower <- derr
	}()
	select {
	case derr := <-follower:
		if derr == nil {
			t.Error("canceled refresh follower returned a false success")
		}
	case <-time.After(300 * time.Millisecond):
		t.Error("refresh follower ignored its deadline while another request owned the download")
	}
	close(release)
	<-leader
}
