package application

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
	"proxypool/internal/integrations/sourcehttp"
)

func TestStaticDownloaderPersistsRetryAfterThroughControlledClient(t *testing.T) {
	s := newTestIngest(t)
	var hits atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = fmt.Fprint(w, "vendor-key-do-not-echo")
	}))
	defer upstream.Close()
	ctx := context.Background()
	src, derr := s.CreateSource(ctx, "static-retry-after", SourceConfig{Static: &StaticConfig{URL: upstream.URL + "/?key=query-key-do-not-echo"}}, true)
	if derr != nil {
		t.Fatal(derr)
	}
	before := s.Now()
	_, derr = s.RefreshStaticSource(ctx, src.ID)
	if derr == nil || derr.Code != "SOURCE_RATE_LIMITED" || derr.RetryAfter < 120*time.Second || strings.Contains(derr.Message, "do-not-echo") {
		t.Fatalf("upstream rate limit was lost or leaked: %v", derr)
	}
	state, err := s.store.SourceFetchState(ctx, src.ID)
	if err != nil || state.NextFetchAt == nil || state.NextFetchAt.Before(before.Add(120*time.Second)) || state.LastErrorCode != "SOURCE_RATE_LIMITED" {
		t.Fatalf("source backoff was not durable: %+v %v", state, err)
	}
	restarted := NewIngestService(s.store, s.secret, true)
	if _, derr := restarted.RefreshStaticSource(ctx, src.ID); derr == nil || derr.Code != "SOURCE_RATE_LIMITED" || hits.Load() != 1 {
		t.Fatalf("restart ignored vendor retry window: hits=%d error=%v", hits.Load(), derr)
	}
}

func TestProviderDownloaderPersistsRetryAfterThroughControlledClient(t *testing.T) {
	s := newTestIngest(t)
	var hits atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = fmt.Fprint(w, "vendor-private-key-do-not-echo")
	}))
	defer upstream.Close()
	s.ScdnEndpoint = upstream.URL
	ctx := context.Background()
	src, derr := s.CreateSource(ctx, "provider-retry-after", SourceConfig{Provider: &ProviderConfig{Protocol: "http"}}, true)
	if derr != nil {
		t.Fatal(derr)
	}
	before := s.Now()
	_, derr = s.FetchOnDemand(ctx, src.ID, 1)
	if derr == nil || derr.Code != "SOURCE_RATE_LIMITED" || derr.RetryAfter < 120*time.Second || strings.Contains(derr.Message, "do-not-echo") {
		t.Fatalf("provider lost the controlled downloader's status or retry header: %v", derr)
	}
	state, err := s.store.SourceFetchState(ctx, src.ID)
	if err != nil || state.NextFetchAt == nil || state.NextFetchAt.Before(before.Add(120*time.Second)) || state.LastErrorCode != "SOURCE_RATE_LIMITED" {
		t.Fatalf("provider retry window was not persisted: %+v %v", state, err)
	}
	restarted := NewIngestService(s.store, s.secret, true)
	restarted.ScdnEndpoint = upstream.URL
	if _, derr := restarted.FetchSource(ctx, src.ID); derr == nil || derr.Code != "SOURCE_RATE_LIMITED" || hits.Load() != 1 {
		t.Fatalf("provider restart ignored retry window: hits=%d error=%v", hits.Load(), derr)
	}
}

func TestStaticDownloadErrorFallbackNeverLeaksUntrustedError(t *testing.T) {
	for _, err := range []error{
		errors.New(`Get "https://user:private-password@example.invalid/?key=private-query": redirect https://secret.invalid/?key=redirect-secret`),
		fmt.Errorf("private-query: %w", context.DeadlineExceeded),
		fmt.Errorf("private-password: %w", context.Canceled),
	} {
		derr := mapStaticDownloadError(err)
		if strings.Contains(derr.Message, "private-") || strings.Contains(derr.Message, "redirect-secret") {
			t.Fatalf("raw download error leaked into API: %v", derr)
		}
	}
	typed := mapStaticDownloadError(&sourcehttp.Error{Code: "SOURCE_RATE_LIMITED", Status: 502, Detail: "upstream is rate limited", RetryAfter: 2 * time.Minute})
	if typed.Code != "SOURCE_RATE_LIMITED" || typed.RetryAfter != 2*time.Minute {
		t.Fatalf("typed upstream error lost retry guidance: %+v", typed)
	}
}

func TestGenericStaticLinesPreserveTransportCredentialsAndRejectURIExtras(t *testing.T) {
	s := newTestIngest(t)
	body := "http://first:user%40pass@proxy.example:8080\n" +
		"socks5://second:pass@[2606:4700::1111]:1080\n" +
		"127.0.0.1:9000\n" +
		"http://secret-user:secret-password@8.8.8.8:80/?key=secret-query\n" +
		"https://8.8.4.4:443\n" +
		"socks4://1.1.1.1:1080\n"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = fmt.Fprint(w, body) }))
	defer upstream.Close()
	ctx := context.Background()
	src, derr := s.CreateSource(ctx, "generic-transport", SourceConfig{Static: &StaticConfig{URL: upstream.URL, DefaultTransport: domain.TransportSOCKS5}}, true)
	if derr != nil {
		t.Fatal(derr)
	}
	res, derr := s.RefreshStaticSource(ctx, src.ID)
	if derr != nil || res.Fetched != 3 || res.Unique != 3 || res.Retained != 3 || res.RejectedCount != 3 {
		t.Fatalf("strict generic line parsing: %+v %v", res, derr)
	}
	rows, _, fresh := s.CachedStaticCandidates(src.ID)
	if !fresh || len(rows) != 3 || rows[0].Resource.Conn.Transport != domain.TransportHTTP || rows[0].Resource.Conn.Username != "first" || rows[0].Resource.Conn.Password != "user@pass" || rows[0].Resource.Conn.Host != "proxy.example" {
		t.Fatalf("HTTP credential/hostname was lost: %+v", rows)
	}
	if rows[1].Resource.Conn.Transport != domain.TransportSOCKS5 || rows[2].Resource.Conn.Transport != domain.TransportSOCKS5 {
		t.Fatal("explicit or default SOCKS5 was relabeled as HTTP")
	}
	for _, rej := range res.Rejected {
		if rej.Input != "" || strings.Contains(rej.Error, "secret-") {
			t.Fatalf("malformed row leaked URI credentials: %+v", rej)
		}
	}
}

func TestOversizedCacheBatchDoesNotPersistFictitiousRetainedCount(t *testing.T) {
	s := newTestIngest(t)
	if err := s.ConfigureStaticCache(time.Minute, 2, 1024); err != nil {
		t.Fatal(err)
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for i := 0; i < 4; i++ {
			_, _ = fmt.Fprintf(w, "127.0.0.1:%d\n", 12340+i)
		}
	}))
	defer upstream.Close()
	ctx := context.Background()
	src, derr := s.CreateSource(ctx, "cache-too-small", SourceConfig{Static: &StaticConfig{URL: upstream.URL}}, true)
	if derr != nil {
		t.Fatal(derr)
	}
	if _, derr := s.RefreshStaticSource(ctx, src.ID); derr == nil || derr.Code != "STATIC_CACHE_CAPACITY" {
		t.Fatalf("oversized cache entry did not fail: %v", derr)
	}
	state, err := s.store.SourceFetchState(ctx, src.ID)
	if err != nil || state.Retained != 0 || state.LastErrorCode != "STATIC_CACHE_CAPACITY" {
		t.Fatalf("cache rejection persisted a misleading success: %+v %v", state, err)
	}
	if _, _, fresh := s.CachedStaticCandidates(src.ID); fresh {
		t.Fatal("rejected batch became available")
	}
}
