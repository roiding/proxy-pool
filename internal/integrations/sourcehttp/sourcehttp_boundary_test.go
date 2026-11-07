package sourcehttp

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestBoundedBodyEOFIsTerminalEvenWithExpiredContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), -time.Second)
	defer cancel()
	canceled := 0
	body := newBoundedBody(io.NopCloser(strings.NewReader("complete")), &http.Transport{},
		func() { canceled++ }, ctx, 1024, "")
	data, err := io.ReadAll(body)
	if err != nil || string(data) != "complete" {
		t.Fatalf("complete body under expired timer: %q %v", data, err)
	}
	for range 3 {
		if n, err := body.Read(make([]byte, 8)); n != 0 || !errors.Is(err, io.EOF) {
			t.Fatalf("EOF must stay terminal, got %d %v", n, err)
		}
	}
	if canceled != 1 {
		t.Fatalf("close must release the hop exactly once, got %d", canceled)
	}
}

func TestBoundedBodyOversizeTerminalIsStableWithoutNegativeRemaining(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	body := newBoundedBody(io.NopCloser(strings.NewReader(strings.Repeat("x", 64))),
		&http.Transport{}, cancel, ctx, 16, "")
	if _, err := io.ReadAll(body); !assertCode(err, "SOURCE_RESPONSE_TOO_LARGE") {
		t.Fatalf("oversize read: %v", err)
	}
	for range 3 {
		if n, err := body.Read(make([]byte, 8)); n != 0 || !assertCode(err, "SOURCE_RESPONSE_TOO_LARGE") {
			t.Fatalf("terminal state changed: %d %v", n, err)
		}
	}
}

func TestBoundedBodyGzipTerminalStates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var raw bytes.Buffer
	writer := gzip.NewWriter(&raw)
	_, _ = io.WriteString(writer, "decoded")
	_ = writer.Close()
	body := newBoundedBody(io.NopCloser(bytes.NewReader(raw.Bytes())), &http.Transport{},
		cancel, ctx, 1024, "gzip")
	data, err := io.ReadAll(body)
	if err != nil || string(data) != "decoded" {
		t.Fatalf("gzip body: %q %v", data, err)
	}
	if n, err := body.Read(make([]byte, 4)); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("gzip EOF must stay terminal, got %d %v", n, err)
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	broken := newBoundedBody(io.NopCloser(strings.NewReader("not gzip")), &http.Transport{},
		cancel2, ctx2, 1024, "gzip")
	for range 2 {
		if _, err := broken.Read(make([]byte, 8)); !assertCode(err, "SOURCE_FORMAT_ERROR") {
			t.Fatalf("broken gzip terminal: %v", err)
		}
	}
}

func TestNon200GzipErrorBodyDecodesAndUnknownEncodingIsRejected(t *testing.T) {
	gzipServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(http.StatusTooManyRequests)
		writer := gzip.NewWriter(w)
		_, _ = io.WriteString(writer, "rate limited")
		_ = writer.Close()
	}))
	defer gzipServer.Close()
	resp, err := NewHTTPClient(Options{AllowPrivate: true}).Get(gzipServer.URL)
	if err != nil || resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status must survive lazy error body: %+v %v", resp, err)
	}
	payload, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil || string(payload) != "rate limited" {
		t.Fatalf("gzip error body: %q %v", payload, err)
	}
	unknown := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "br")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("brotli"))
	}))
	defer unknown.Close()
	resp, err = NewHTTPClient(Options{AllowPrivate: true}).Get(unknown.URL)
	if err != nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("unsupported encoding must keep status: %+v %v", resp, err)
	}
	if _, err = io.ReadAll(resp.Body); !assertCode(err, "SOURCE_FORMAT_ERROR") {
		t.Fatalf("unsupported encoding: %v", err)
	}
	_ = resp.Body.Close()
}

func TestIPv6LiteralOriginIsAllowedAndPinned(t *testing.T) {
	var sawHost atomic.Value
	listener, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Skipf("no tcp6 support: %v", err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawHost.Store(r.Host)
		_, _ = fmt.Fprint(w, "1.2.3.4:80\n")
	}))
	server.Listener = listener
	server.Start()
	defer server.Close()
	_, port, _ := net.SplitHostPort(listener.Addr().String())
	var lookups int
	result, err := Download(context.Background(), "http://[::1]:"+port+"/", Conditional{},
		Options{AllowPrivate: true, ResolveHost: func(context.Context, string) ([]string, error) {
			lookups++
			return []string{"::1"}, nil
		}})
	if err != nil || string(result.Body) != "1.2.3.4:80\n" || lookups != 0 {
		t.Fatalf("IPv6 literal must bypass the DNS seam: %+v lookups=%d err=%v", result, lookups, err)
	}
	if sawHost.Load() != "[::1]:"+port {
		t.Fatalf("IPv6 Host header changed: %v", sawHost.Load())
	}
}

func TestParseRetryAfterRejectsInvalidValues(t *testing.T) {
	now := time.Now()
	for _, raw := range []string{"", "-5", "abc", "0", "tomorrow", "Tue, 31 Feb 2026 00:00:00 GMT"} {
		if got := ParseRetryAfter(raw, now); got != 0 {
			t.Fatalf("ParseRetryAfter(%q) = %s, want 0", raw, got)
		}
	}
}

func assertCode(err error, code string) bool {
	var ferr *Error
	return errors.As(err, &ferr) && ferr.Code == code
}

func TestBodylessResponsesDoNotDecodeDeclaredGzip(t *testing.T) {
	for _, tc := range []struct {
		method string
		status int
	}{{"HEAD", 200}, {"GET", 304}, {"GET", 204}} {
		t.Run(tc.method+strconv.Itoa(tc.status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Encoding", "gzip")
				w.Header().Set("ETag", `"v1"`)
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()
			req, _ := http.NewRequest(tc.method, srv.URL, nil)
			resp, err := NewHTTPClient(Options{AllowPrivate: true}).Do(req)
			if err != nil {
				t.Fatalf("bodyless encoded response failed: %v", err)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil || len(body) != 0 || resp.StatusCode != tc.status {
				t.Fatalf("bodyless encoding decoded: %d %q %v", resp.StatusCode, body, err)
			}
		})
	}
}
