package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"proxypool/internal/config"
	"proxypool/internal/storage/sqlite"
)

func testConfig() config.Config {
	cfg := config.Default()
	cfg.APIToken = "test-master-token-0123456789"
	cfg.IdentitySecret = "test-identity-secret-0123456789"
	cfg.JudgeURL = "http://127.0.0.1:8080/v1/judge"
	return cfg
}

// Explicit header presence must always win over the session cookie, even
// when the value is empty or the scheme is unsupported: those requests
// authenticate by header and fail closed instead of riding the cookie.
func TestHeaderTokenExplicitPresence(t *testing.T) {
	cases := []struct {
		name        string
		auth        string
		apiKey      string
		setAPIKey   bool
		wantToken   string
		wantPresent bool
	}{
		{name: "bearer", auth: "Bearer tok", wantToken: "tok", wantPresent: true},
		{name: "bearer lower scheme", auth: "bearer tok", wantToken: "tok", wantPresent: true},
		{name: "bearer upper scheme", auth: "BEARER tok", wantToken: "tok", wantPresent: true},
		{name: "bearer empty token", auth: "Bearer ", wantToken: "", wantPresent: true},
		{name: "bearer no space", auth: "Bearer", wantToken: "", wantPresent: true},
		{name: "unsupported scheme", auth: "Basic abc", wantToken: "Basic abc", wantPresent: true},
		{name: "api key", apiKey: "tok", wantToken: "tok", wantPresent: true},
		{name: "api key empty", setAPIKey: true, wantToken: "", wantPresent: true},
		{name: "absent", wantToken: "", wantPresent: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", "/", nil)
			if tc.auth != "" {
				r.Header.Set("Authorization", tc.auth)
			}
			if tc.setAPIKey || tc.apiKey != "" {
				r.Header.Set("X-API-Key", tc.apiKey)
			}
			token, present := headerToken(r)
			if present != tc.wantPresent || token != tc.wantToken {
				t.Fatalf("headerToken = (%q, %v), want (%q, %v)", token, present, tc.wantToken, tc.wantPresent)
			}
		})
	}
}

// An explicit invalid header must not fall back to a valid admin session,
// while a cookie-only request still authenticates as admin with the default
// project's identity and defaults.
func TestAuthenticateExplicitHeaderNeverFallsBackToCookie(t *testing.T) {
	db, err := sqlite.Open(filepath.Join(t.TempDir(), "auth.db"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := sqlite.New(db)
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	s := &Server{cfg: testConfig(), sessions: newSessions(4),
		logins: newLoginLimiter(0, 10), store: store}
	cookie := &http.Cookie{Name: sessionCookie, Value: s.sessions.issue(time.Hour)}

	req := httptest.NewRequest("POST", "/api/v1/proxies/acquire", nil)
	req.AddCookie(cookie)
	req.Header.Set("Authorization", "Bearer invalid-token")
	if ident, ok, _ := s.authenticate(req); ok {
		t.Fatalf("invalid explicit header fell back to cookie: %+v", ident)
	}

	req = httptest.NewRequest("POST", "/api/v1/proxies/acquire", nil)
	req.AddCookie(cookie)
	ident, ok, viaCookie := s.authenticate(req)
	if !ok || !viaCookie || !ident.Admin || ident.ProjectID != "default" {
		t.Fatalf("cookie identity = %+v ok=%v viaCookie=%v", ident, ok, viaCookie)
	}
}
