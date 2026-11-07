package staticdl

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func installResolver(t *testing.T, addrs map[string][]string) *atomic.Int64 {
	t.Helper()
	orig := resolveHost
	var lookups atomic.Int64
	resolveHost = func(ctx context.Context, host string) ([]string, error) {
		if a, ok := addrs[host]; ok {
			lookups.Add(1)
			return a, nil
		}
		return orig(ctx, host)
	}
	t.Cleanup(func() { resolveHost = orig })
	return &lookups
}

func TestDownloadBasicAndPinnedHost(t *testing.T) {
	var sawHost atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawHost.Store(r.Host)
		w.Header().Set("ETag", `"abc"`)
		_, _ = w.Write([]byte("1.2.3.4:8080\n"))
	}))
	defer srv.Close()
	addr := srv.Listener.Addr().String()
	_, port, _ := strings.Cut(addr, ":")
	lookups := installResolver(t, map[string][]string{"list.example": {"127.0.0.1"}})
	res, err := Download(context.Background(), "http://list.example:"+port+"/list.txt",
		Conditional{}, Options{AllowPrivate: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != 200 || string(res.Body) != "1.2.3.4:8080\n" || res.ETag != `"abc"` {
		t.Fatalf("download result: %+v", res)
	}
	if sawHost.Load() != "list.example:"+port {
		t.Fatalf("host header must keep the original name: %v", sawHost.Load())
	}
	if n := lookups.Load(); n != 1 {
		t.Fatalf("resolve count %d, want exactly 1 per download", n)
	}
}

func TestDownloadConditionalNotModified(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") == `"v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"v1"`)
		_, _ = w.Write([]byte("1.2.3.4:8080\n"))
	}))
	defer srv.Close()
	res, err := Download(context.Background(), srv.URL, Conditional{ETag: `"v1"`},
		Options{AllowPrivate: true})
	if err != nil {
		t.Fatal(err)
	}
	if !res.NotModified || res.Status != 304 {
		t.Fatalf("expected 304, got %+v", res)
	}
}

func TestDownloadFollowsAndCapsRedirects(t *testing.T) {
	mux := http.NewServeMux()
	hops := 0
	mux.HandleFunc("/hop", func(w http.ResponseWriter, r *http.Request) {
		hops++
		http.Redirect(w, r, "/hop", http.StatusFound)
	})
	mux.HandleFunc("/final", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("5.6.7.8:80\n"))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	if _, err := Download(context.Background(), srv.URL+"/hop", Conditional{},
		Options{AllowPrivate: true}); err == nil {
		t.Fatal("redirect loop must hit the hop cap")
	}
	res, err := Download(context.Background(), srv.URL+"/final", Conditional{},
		Options{AllowPrivate: true})
	if err != nil || string(res.Body) != "5.6.7.8:80\n" {
		t.Fatalf("final download: %+v err=%v", res, err)
	}
	if hops < MaxRedirects {
		t.Fatalf("loop server saw %d hops", hops)
	}
}

func TestDownloadBodyCap(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(make([]byte, 4096))
	}))
	defer srv.Close()
	if _, err := Download(context.Background(), srv.URL, Conditional{}, Options{MaxBytes: 1024, AllowPrivate: true}); err == nil {
		t.Fatal("oversized body must fail")
	}
}

func TestDownloadRejectsBadURLsAndPolicies(t *testing.T) {
	installResolver(t, map[string][]string{})
	cases := []struct {
		name, url   string
		allowPrivat bool
	}{
		{"ftp scheme", "ftp://example.com/list", true},
		{"missing host", "http:///list", true},
		{"credentials", "http://user:pass@example.com/list", true},
		{"private literal", "http://127.0.0.1:1/list", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Download(context.Background(), tc.url, Conditional{},
				Options{AllowPrivate: tc.allowPrivat}); err == nil {
				t.Fatal("bad URL accepted")
			}
		})
	}
}

func TestDownloadRedirectPolicyRevalidation(t *testing.T) {
	// Hop 1 is a loopback fixture; the redirect target switches to ftp and
	// the per-hop scheme check must reject it.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "ftp://10.0.0.9/list", http.StatusFound)
	}))
	defer srv.Close()
	_, err := Download(context.Background(), srv.URL, Conditional{}, Options{AllowPrivate: true})
	if err == nil || !strings.Contains(err.Error(), "scheme") {
		t.Fatalf("scheme-switching redirect must be rejected, got %v", err)
	}
}

func TestDownloadTimeoutBounds(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
	}))
	defer srv.Close()
	started := time.Now()
	_, err := Download(context.Background(), srv.URL, Conditional{}, Options{
		Timeout: 50 * time.Millisecond, AllowPrivate: true,
	})
	if err == nil {
		t.Fatal("slow server must time out")
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("timeout not bounded: %s", elapsed)
	}
}
