package application

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"proxypool/internal/integrations/sites"
)

func TestDebugSiteSequentialRefresh(t *testing.T) {
	svc := newTestIngest(t)
	ctx := context.Background()
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("<table><tr><td>IP</td><td>PORT</td></tr><tr><td>203.0.113.200</td><td>8080</td></tr></table>"))
	}))
	defer srv.Close()
	if _, derr := svc.CreateSource(ctx, "site-seq", SourceConfig{
		Site: &SiteConfig{SiteID: "ip3366-normal", MaxPages: 1},
	}, true); derr != nil {
		t.Fatal(derr)
	}
	override, _ := sites.ByID("ip3366-normal")
	override.URL = srv.URL
	svc.SiteOverrides = map[string]sites.Site{"ip3366-normal": override}
	res1, derr := svc.RefreshStaticSource(ctx, "site-seq")
	t.Logf("first: %+v err=%v hits=%d", res1, derr, hits.Load())
	if derr != nil {
		t.Fatal(derr)
	}
	res2, derr := svc.RefreshStaticSource(ctx, "site-seq")
	t.Logf("second: %+v err=%v hits=%d", res2, derr, hits.Load())
	if derr != nil || !res2.CacheHit || res2.NotModified || hits.Load() != 1 {
		t.Fatalf("sequential refresh must reuse memory snapshot: %+v %v hits=%d", res2, derr, hits.Load())
	}
}
