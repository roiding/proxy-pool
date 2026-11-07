package sites

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"proxypool/internal/domain"
)

// Every response in this file is synthetic; it verifies adapter contracts,
// not current vendor availability or proxy/Judge liveness.
func TestCatalogCompatibilityAndLocalFetchMatrix(t *testing.T) {
	legacy := []string{"ip3366-normal", "jiliuip-elite", "qiyunip-elite", "ip89", "lumiproxy-transparent", "lumiproxy-anon", "lumiproxy-elite", "proxyscrape-anon", "proxyscrape-elite", "openproxylist-http"}
	// Original HTTPS ID is a compatibility contract too.
	legacy = append(legacy, "openproxylist-https")
	for _, id := range legacy {
		if _, ok := ByID(id); !ok {
			t.Fatalf("removed legacy ID %s", id)
		}
	}
	if len(Catalog) != 37 {
		t.Fatalf("catalog size changed: %d", len(Catalog))
	}
	ids := map[string]bool{}
	active, paused := 0, 0
	for _, site := range Catalog {
		if ids[site.ID] {
			t.Fatalf("duplicate ID %s", site.ID)
		}
		ids[site.ID] = true
		if site.State.Runnable() {
			active++
		} else {
			paused++
		}
		t.Run(site.ID, func(t *testing.T) {
			var hits atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hits.Add(1)
				if site.Method == http.MethodPost {
					if r.Method != http.MethodPost {
						t.Error("POST site was fetched as GET")
					}
					if err := r.ParseForm(); err != nil {
						t.Error(err)
					}
				}
				fmt.Fprint(w, fixtureFor(site, "normal"))
			}))
			defer srv.Close()
			local := site
			local.URL = srv.URL + "/?page={page}"
			local.DelayMS = 0
			out, err := FetchSite(context.Background(), nil, local, 1, true)
			if !site.State.Runnable() {
				var e *Error
				if !errors.As(err, &e) || e.Code != "SOURCE_NOT_RUNNABLE" || hits.Load() != 0 {
					t.Fatalf("paused site ran: %+v %v hits=%d", out, err, hits.Load())
				}
				return
			}
			if err != nil || len(out.Entries) == 0 || hits.Load() != 1 {
				t.Fatalf("template contract: %+v %v hits=%d", out, err, hits.Load())
			}
			for _, entry := range out.Entries {
				if entry.Conn.Transport != site.Protocol {
					t.Fatalf("protocol changed: %+v", entry)
				}
			}
			if site.VendorProtocol == "https" && (out.Entries[0].Conn.Transport != domain.TransportHTTP || out.Entries[0].Claims["http_connect"] != "true") {
				t.Fatal("vendor HTTPS claim lost or relabeled")
			}
			if site.Kind == ParserGeonodeJSON && (out.Complete || !out.Truncated) {
				t.Fatal("rolling window reported full snapshot")
			}
		})
	}
	if active != 30 || paused != 7 {
		t.Fatalf("state matrix changed: active=%d paused=%d", active, paused)
	}
	for _, id := range []string{"openproxylist-http", "openproxylist-https"} {
		s, _ := ByID(id)
		suffix := strings.TrimPrefix(id, "openproxylist-")
		if s.URL != "https://api.openproxylist.xyz/"+suffix+".txt" || s.Kind != ParserLines {
			t.Fatalf("OpenProxyList contract changed: %+v", s)
		}
	}
}

func TestFetchLaterAccessFailureDoesNotReturnPartialBatch(t *testing.T) {
	for _, status := range []int{401, 403, 429, 200} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var hits atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := hits.Add(1)
				if n == 1 {
					fmt.Fprint(w, "93.184.216.34:8080\n")
					return
				}
				w.Header().Set("Retry-After", "120")
				w.WriteHeader(status)
				if status == 200 {
					fmt.Fprint(w, `<!doctype html><html><title>Just a moment...</title><body>cf-chl-private-key 93.184.216.34:80</body></html>`)
				} else {
					fmt.Fprint(w, "vendor-secret-body")
				}
			}))
			defer srv.Close()
			out, err := FetchSite(context.Background(), nil, Site{ID: "private-query-secret", Kind: ParserLines, Protocol: domain.TransportHTTP, URL: srv.URL + "/?key=query-secret&page={page}", Pages: 3}, 0, true)
			var e *Error
			if !errors.As(err, &e) || e.Kind != KindAccess || e.RetryAfter != 120*time.Second || hits.Load() != 2 || len(out.Entries) != 0 {
				t.Fatalf("access failure returned partial data: %+v %v hits=%d", out, err, hits.Load())
			}
			if strings.Contains(e.Detail, "secret") || e.Status == 401 {
				t.Fatalf("vendor error leaked or logs admin out: %+v", e)
			}
		})
	}
}

func TestFetchPartialOrdinaryFailureAndCredentialIdentity(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		if n == 2 {
			w.WriteHeader(503)
			return
		}
		fmt.Fprint(w, "http://u:first@proxy.example:8080\nhttp://u:second@proxy.example:8080\nhttp://u:first@proxy.example:8080\n")
	}))
	defer srv.Close()
	out, err := FetchSite(context.Background(), nil, Site{Kind: ParserLines, Protocol: domain.TransportHTTP, URL: srv.URL, Pages: 3}, 0, true)
	if err != nil || len(out.Entries) != 2 || out.Fetched != 6 || out.Unique != 2 || out.Complete || out.PagesDone != 2 || out.PagesTotal != 3 {
		t.Fatalf("partial/identity contract: %+v %v", out, err)
	}
}

func TestFetchLimitsAndCancellation(t *testing.T) {
	t.Run("bytes", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, strings.Repeat(" ", MaxPageBytes+1)) }))
		defer srv.Close()
		_, err := FetchSite(context.Background(), nil, Site{Kind: ParserLines, URL: srv.URL, Pages: 1}, 0, true)
		var e *Error
		if !errors.As(err, &e) || e.Code != "SOURCE_RESPONSE_TOO_LARGE" {
			t.Fatalf("oversize success: %v", err)
		}
	})
	t.Run("pages", func(t *testing.T) {
		var hits atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			fmt.Fprint(w, `{"data":{"list":[{"ip":"93.184.216.34","port":80,"protocol":1}],"page_count":999999}}`)
		}))
		defer srv.Close()
		out, err := FetchSite(context.Background(), nil, Site{Kind: ParserJSONList, URL: srv.URL, AutoPages: true}, 0, true)
		if err != nil || hits.Load() != MaxPages || out.Complete || !out.Truncated || out.Unique != 1 {
			t.Fatalf("page cap failed: %+v %v hits=%d", out, err, hits.Load())
		}
	})
	t.Run("records", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, strings.Repeat("93.184.216.34:8080\n", DefaultMaxRecords+1))
		}))
		defer srv.Close()
		out, err := FetchSite(context.Background(), nil, Site{Kind: ParserLines, URL: srv.URL, Pages: 2}, 0, true)
		if err != nil || out.Fetched != DefaultMaxRecords || out.Unique != 1 || !out.Truncated || out.Complete {
			t.Fatalf("record cap failed: %+v %v", out, err)
		}
	})
	t.Run("cancel body", func(t *testing.T) {
		started := make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(200)
			w.(http.Flusher).Flush()
			close(started)
			<-r.Context().Done()
		}))
		defer srv.Close()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			_, err := FetchSite(ctx, nil, Site{Kind: ParserLines, URL: srv.URL, Pages: 2}, 0, true)
			done <- err
		}()
		<-started
		cancel()
		select {
		case err := <-done:
			var e *Error
			if !errors.As(err, &e) || (e.Code != "SOURCE_CANCELLED" && e.Code != "REQUEST_CANCELED") {
				t.Fatalf("cancel not propagated: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("fetch stuck after cancel")
		}
	})
}
