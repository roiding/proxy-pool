package webshare

import (
	"context"
	"encoding/json"
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

func proxyFixture(id any, valid bool) map[string]any {
	return map[string]any{"id": id, "username": "proxy-user", "password": "proxy-pass", "proxy_address": "8.8.8.8", "port": 8080, "valid": valid, "country_code": "US", "last_verification": "2026-11-04T01:02:03Z"}
}

func TestDirectListCompletePaginationAndCredentials(t *testing.T) {
	var hits atomic.Int64
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.Header.Get("Authorization") != "Token account-api-key" || r.URL.Query().Get("mode") != "direct" || r.URL.Query().Get("page_size") != "2" {
			t.Errorf("wrong fixed direct-list request")
		}
		var next any
		rows := []map[string]any{proxyFixture("one", true), proxyFixture(2, false)}
		if r.URL.Query().Get("page") == "1" {
			next = server.URL + "?mode=direct&page=2&page_size=2"
		} else {
			row := proxyFixture("three", true)
			row["port"] = 3128
			rows = []map[string]any{row}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"count": 3, "next": next, "previous": nil, "results": rows})
	}))
	defer server.Close()
	result, err := Fetch(context.Background(), server.Client(), server.URL, "account-api-key", Config{Protocol: domain.TransportSOCKS5, PageSize: 2, MaxPages: 2}, false)
	if err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 2 || !result.Complete || result.PagesDone != 2 || result.PagesTotal != 2 || result.Fetched != 2 || result.Rejected != 1 || len(result.Entries) != 2 {
		t.Fatalf("wrong full snapshot metadata: %+v hits=%d", result, hits.Load())
	}
	first := result.Entries[0]
	if first.Conn.Transport != domain.TransportSOCKS5 || first.Conn.Username != "proxy-user" || first.Conn.Password != "proxy-pass" || first.RecordID != "one" || first.Claims["vendor_country"] != "US" || first.Claims["vendor_last_verification"] == "" {
		t.Fatalf("connection/claims were lost: %+v", first)
	}
}

func TestAccountEnvelopeErrorsAreSanitized(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		want   string
	}{
		{"auth", 401, `{"detail":"secret-token-value"}`, "SOURCE_AUTH_FAILED"},
		{"rate", 429, `{"detail":"secret-token-value"}`, "SOURCE_RATE_LIMITED"},
		{"html", 200, `<html>secret-token-value login</html>`, "SOURCE_FORMAT_ERROR"},
		{"error-200", 200, `{"detail":"secret-token-value"}`, "SOURCE_FORMAT_ERROR"},
		{"missing-results", 200, `{"count":0,"next":null}`, "SOURCE_FORMAT_ERROR"},
		{"null-results", 200, `{"count":0,"next":null,"results":null}`, "SOURCE_FORMAT_ERROR"},
		{"short-snapshot", 200, `{"count":1,"next":null,"results":[]}`, "SOURCE_SNAPSHOT_INCOMPLETE"},
		{"bad-connection", 200, `{"count":1,"next":null,"results":[{"id":"one","valid":true,"username":"secret-token-value"}]}`, "SOURCE_FORMAT_ERROR"},
		{"too-large", 200, strings.Repeat("x", MaxPageBytes+1), "SOURCE_FORMAT_ERROR"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Retry-After", "120")
				w.WriteHeader(tc.status)
				_, _ = fmt.Fprint(w, tc.body)
			}))
			defer srv.Close()
			result, err := Fetch(context.Background(), srv.Client(), srv.URL, "account-api-key", Config{Protocol: domain.TransportHTTP}, false)
			var ferr *Error
			if !errors.As(err, &ferr) || ferr.Code != tc.want || len(result.Entries) != 0 || strings.Contains(err.Error(), "secret-token-value") {
				t.Fatalf("wrong/sensitive error: %+v %v", result, err)
			}
			if tc.status == 429 && ferr.RetryAfter != 120*time.Second {
				t.Fatalf("retry-after lost: %v", ferr.RetryAfter)
			}
		})
	}
}

func TestExplicitEmptyAndInvalidAllocatedRecords(t *testing.T) {
	for _, body := range []string{`{"count":0,"next":null,"results":[]}`, `{"count":1,"next":null,"results":[{"id":"one","valid":false}]}`} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = fmt.Fprint(w, body) }))
		result, err := Fetch(context.Background(), srv.Client(), srv.URL, "account-api-key", Config{Protocol: domain.TransportHTTP}, false)
		srv.Close()
		if err != nil || !result.Complete || len(result.Entries) != 0 {
			t.Fatalf("explicit empty not recognized: %+v %v", result, err)
		}
	}
}

func TestUnsafePaginationAndRedirectNeverForwardAccountKey(t *testing.T) {
	var destinationHits atomic.Int64
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		destinationHits.Add(1)
		t.Error("unsafe destination requested")
	}))
	defer other.Close()
	for _, redirect := range []bool{false, true} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if redirect {
				http.Redirect(w, r, other.URL, http.StatusFound)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"count": 2, "next": other.URL + "?mode=direct&page=2&page_size=1", "results": []map[string]any{proxyFixture("one", true)}})
		}))
		result, err := Fetch(context.Background(), srv.Client(), srv.URL, "account-api-key", Config{Protocol: domain.TransportHTTP, PageSize: 1, MaxPages: 2}, false)
		srv.Close()
		if err == nil || len(result.Entries) != 0 {
			t.Fatalf("unsafe request accepted: %+v %v", result, err)
		}
	}
	if destinationHits.Load() != 0 {
		t.Fatal("account key could cross pagination origin")
	}
}

func TestPartialPageFailureRejectsEntireSnapshot(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("page") == "2" {
			http.Error(w, "private-vendor-detail", 500)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"count": 2, "next": srv.URL + "?mode=direct&page=2&page_size=1", "results": []map[string]any{proxyFixture("one", true)}})
	}))
	defer srv.Close()
	result, err := Fetch(context.Background(), srv.Client(), srv.URL, "account-api-key", Config{Protocol: domain.TransportHTTP, PageSize: 1, MaxPages: 2}, false)
	if err == nil || result.Complete || len(result.Entries) > 0 || strings.Contains(err.Error(), "private-vendor-detail") {
		t.Fatalf("partial snapshot escaped: %+v %v", result, err)
	}
}

func TestAccountFetchHonorsCallerDeadline(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := Fetch(ctx, srv.Client(), srv.URL, "account-api-key", Config{Protocol: domain.TransportHTTP}, false)
	var ferr *Error
	if !errors.As(err, &ferr) || ferr.Code != "SOURCE_TIMEOUT" || time.Since(start) > time.Second {
		t.Fatalf("deadline was not bounded: %v", err)
	}
}
