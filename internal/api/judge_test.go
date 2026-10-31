package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"proxypool/internal/validation"
)

func TestJudgeHandler(t *testing.T) {
	secret := "judge-test-secret-0123456789abcdef"
	mux := http.NewServeMux()
	mux.Handle("GET /v1/judge", NewJudgeHandler(secret, false))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/v1/judge?nonce=0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatalf("judge request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var body struct {
		Nonce     string `json:"nonce"`
		ExitIP    string `json:"exit_ip"`
		Signature string `json:"signature"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Nonce != "0123456789abcdef0123456789abcdef" {
		t.Fatalf("nonce echo wrong: %+v", body)
	}
	if body.ExitIP == "" {
		t.Fatalf("exit_ip missing")
	}
	want := validation.JudgeSignature(secret, body.Nonce, body.ExitIP)
	if body.Signature != want {
		t.Fatalf("signature mismatch: got %q want %q", body.Signature, want)
	}

	if resp, err := http.Get(srv.URL + "/v1/judge?nonce=not-hex"); err == nil {
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("bad nonce status=%d", resp.StatusCode)
		}
	} else {
		t.Fatalf("bad nonce request: %v", err)
	}
}

func TestJudgeRateLimit(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle("GET /v1/judge", NewJudgeHandler("", false))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	sawLimited := false
	for i := 0; i < 160; i++ {
		resp, err := http.Get(srv.URL + "/v1/judge?nonce=0123456789abcdef0123456789abcdef")
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests {
			sawLimited = true
			break
		}
	}
	if !sawLimited {
		t.Fatal("rate limiter never triggered")
	}
}

func TestJudgeForwardedFor(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle("GET /v1/judge", NewJudgeHandler("", true))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/judge?nonce=0123456789abcdef0123456789abcdef", nil)
	req.Header.Set("X-Forwarded-For", "1.2.3.4, 10.0.0.9")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	var body struct {
		ExitIP string `json:"exit_ip"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.ExitIP != "10.0.0.9" {
		t.Fatalf("exit_ip=%q, want rightmost trusted entry 10.0.0.9", body.ExitIP)
	}
}

func TestJudgeCloudflareConnectingIP(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle("GET /v1/judge", NewJudgeHandler("", true))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/judge?nonce=0123456789abcdef0123456789abcdef", nil)
	// A proxied client may spoof CF-Connecting-IP and XFF's leading entry; the
	// deployment's trusted chain (Cloudflare, then Traefik) rebuilds them.
	req.Header.Set("CF-Connecting-IP", "203.0.113.9")
	req.Header.Set("X-Forwarded-For", "6.6.6.6, 104.21.0.1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	var body struct {
		ExitIP string `json:"exit_ip"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.ExitIP != "203.0.113.9" {
		t.Fatalf("exit_ip=%q, want CF-Connecting-IP 203.0.113.9", body.ExitIP)
	}
}
