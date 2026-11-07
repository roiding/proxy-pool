package sourcehttp

import (
	"compress/gzip"
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func assertError(t *testing.T, err error, code string) *Error {
	t.Helper()
	var ferr *Error
	if !errors.As(err, &ferr) || ferr.Code != code {
		t.Fatalf("want %s, got %v", code, err)
	}
	return ferr
}
func TestPinnedDNSOriginalHostAndNoEnvironmentProxy(t *testing.T) {
	var lookups, proxied atomic.Int64
	environmentProxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { proxied.Add(1); w.WriteHeader(502) }))
	defer environmentProxy.Close()
	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"} {
		t.Setenv(key, environmentProxy.URL)
	}
	var host atomic.Value
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { host.Store(r.Host); _, _ = fmt.Fprint(w, "8.8.8.8:80\n") }))
	defer server.Close()
	u, _ := url.Parse(server.URL)
	target := "http://list.example:" + u.Port() + "/"
	result, err := Download(context.Background(), target, Conditional{}, Options{AllowPrivate: true, ResolveHost: func(ctx context.Context, name string) ([]string, error) {
		lookups.Add(1)
		if name != "list.example" {
			t.Errorf("unexpected DNS name %s", name)
		}
		return []string{"127.0.0.1"}, nil
	}})
	if err != nil || string(result.Body) != "8.8.8.8:80\n" || host.Load() != "list.example:"+u.Port() || lookups.Load() != 1 || proxied.Load() != 0 {
		t.Fatalf("pinned/no-proxy contract: result=%+v host=%v lookups=%d proxied=%d err=%v", result, host.Load(), lookups.Load(), proxied.Load(), err)
	}
}
func TestRedirectRevalidatesPolicyAndDropsAllSensitiveHeaders(t *testing.T) {
	var secondHits atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondHits.Add(1)
		for _, key := range []string{"Authorization", "Proxy-Authorization", "Cookie", "X-Vendor-Api-Key", "If-None-Match", "Referer"} {
			if r.Header.Get(key) != "" {
				t.Errorf("forwarded %s", key)
			}
		}
		if r.Header.Get("Accept") != "text/plain" {
			t.Error("harmless Accept header lost")
		}
		_, _ = fmt.Fprint(w, "ok")
	}))
	defer target.Close()
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", target.URL)
		w.WriteHeader(302)
	}))
	defer first.Close()
	req, _ := http.NewRequest(http.MethodGet, first.URL, nil)
	for _, key := range []string{"Authorization", "Proxy-Authorization", "Cookie", "X-Vendor-Api-Key", "If-None-Match", "Referer"} {
		req.Header.Set(key, "synthetic-secret")
	}
	req.Header.Set("Accept", "text/plain")
	resp, err := NewHTTPClient(Options{AllowPrivate: true}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if secondHits.Load() != 1 {
		t.Fatal("redirect did not reach second origin")
	}
	private := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "ftp://user:private-password@host.invalid/?key=private-query")
		w.WriteHeader(302)
	}))
	defer private.Close()
	_, err = Download(context.Background(), private.URL, Conditional{}, Options{AllowPrivate: true})
	ferr := assertError(t, err, "SOURCE_INVALID_URL")
	if strings.Contains(ferr.Error(), "private-") {
		t.Fatalf("Location leaked: %v", err)
	}
}
func TestPost307IsRejectedRatherThanRewritten(t *testing.T) {
	var called atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called.Add(1) }))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", target.URL)
		w.WriteHeader(307)
	}))
	defer server.Close()
	req, _ := http.NewRequest(http.MethodPost, server.URL, strings.NewReader("synthetic-body"))
	_, err := NewHTTPClient(Options{AllowPrivate: true}).Do(req)
	assertError(t, err, "SOURCE_REDIRECT_BLOCKED")
	if called.Load() != 0 {
		t.Fatal("POST/body was sent through a redirect")
	}
}
func TestNoRedirectAndRateHeadersArriveBeforeErrorBody(t *testing.T) {
	for _, status := range []int{302, 401, 403, 429} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			aborted := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", "http://do-not-follow.invalid/")
				w.Header().Set("Retry-After", "120")
				w.WriteHeader(status)
				w.(http.Flusher).Flush()
				<-r.Context().Done()
				close(aborted)
			}))
			defer server.Close()
			started := time.Now()
			resp, err := NewHTTPClient(Options{AllowPrivate: true, NoRedirects: true, Timeout: time.Second}).Get(server.URL)
			if err != nil || resp.StatusCode != status || resp.Header.Get("Retry-After") != "120" || time.Since(started) > 500*time.Millisecond {
				t.Fatalf("status/header was swallowed: %+v %v", resp, err)
			}
			_ = resp.Body.Close()
			select {
			case <-aborted:
			case <-time.After(time.Second):
				t.Fatal("error body close did not release I/O")
			}
		})
	}
}
func TestTotalDeadlineIncludesAllHopsAndBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(35 * time.Millisecond)
		w.Header().Set("Location", "/again")
		w.WriteHeader(302)
	}))
	defer server.Close()
	started := time.Now()
	_, err := Download(context.Background(), server.URL, Conditional{}, Options{AllowPrivate: true, Timeout: 60 * time.Millisecond})
	assertError(t, err, "SOURCE_TIMEOUT")
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > 300*time.Millisecond {
		t.Fatalf("deadline restarted each hop: %s %v", time.Since(started), err)
	}
	bodyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer bodyServer.Close()
	_, err = Download(context.Background(), bodyServer.URL, Conditional{}, Options{AllowPrivate: true, Timeout: 40 * time.Millisecond})
	assertError(t, err, "SOURCE_TIMEOUT")
}
func TestDecodedByteCapAndTruncatedBodyAreNotPartialSuccess(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(fmt.Sprint(compressed), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if compressed {
					w.Header().Set("Content-Encoding", "gzip")
					writer := gzip.NewWriter(w)
					_, _ = io.WriteString(writer, strings.Repeat("x", 4096))
					_ = writer.Close()
				} else {
					_, _ = io.WriteString(w, strings.Repeat("x", 4096))
				}
			}))
			defer server.Close()
			result, err := Download(context.Background(), server.URL, Conditional{}, Options{AllowPrivate: true, MaxBytes: 1024})
			assertError(t, err, "SOURCE_RESPONSE_TOO_LARGE")
			if len(result.Body) != 0 {
				t.Fatal("oversize response returned partial body")
			}
		})
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "100")
		_, _ = fmt.Fprint(w, "short")
	}))
	defer server.Close()
	result, err := Download(context.Background(), server.URL, Conditional{}, Options{AllowPrivate: true})
	assertError(t, err, "SOURCE_NETWORK_ERROR")
	if len(result.Body) != 0 {
		t.Fatal("truncated response returned partial success")
	}
}
func TestTLSVerificationAndHTTPSDowngradeStayEnabled(t *testing.T) {
	var plainHits atomic.Int64
	plain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { plainHits.Add(1) }))
	defer plain.Close()
	secure := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", plain.URL)
		w.WriteHeader(302)
	}))
	defer secure.Close()
	_, err := Download(context.Background(), secure.URL, Conditional{}, Options{AllowPrivate: true})
	assertError(t, err, "SOURCE_TLS_ERROR")
	client := NewHTTPClient(Options{AllowPrivate: true})
	roots := x509.NewCertPool()
	roots.AddCert(secure.Certificate())
	client.Transport.(*roundTripper).rootCAs = roots
	_, err = client.Get(secure.URL)
	assertError(t, err, "SOURCE_REDIRECT_BLOCKED")
	if plainHits.Load() != 0 {
		t.Fatal("HTTPS downgraded to HTTP")
	}
}
func TestUnsafeURLsErrorsAndRetryAfterAreBounded(t *testing.T) {
	for _, raw := range []string{"ftp://host.invalid/", "http://user:private-password@host.invalid/?key=private-query", "http://host.invalid/%ZZ?key=private-query", "http://[fe80::1%25en0]:80/", "http://127.0.0.1:0/"} {
		_, err := Download(context.Background(), raw, Conditional{}, Options{AllowPrivate: true})
		if err == nil || strings.Contains(err.Error(), "private-") {
			t.Fatalf("unsafe URL accepted or leaked: %s: %v", raw, err)
		}
	}
	_, err := Download(context.Background(), "http://127.0.0.1:1/", Conditional{}, Options{})
	assertError(t, err, "SOURCE_ADDRESS_REJECTED")
	now := time.Now().UTC().Truncate(time.Second)
	for _, raw := range []string{"900000", now.Add(48 * time.Hour).Format(http.TimeFormat)} {
		if got := ParseRetryAfter(raw, now); got != 24*time.Hour {
			t.Fatalf("unbounded retry duration: %q %s", raw, got)
		}
	}
	if got := ParseRetryAfter(now.Add(30*time.Second).Format(http.TimeFormat), now); got != 30*time.Second {
		t.Fatalf("HTTP date retry: %s", got)
	}
}
