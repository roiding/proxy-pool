package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"proxypool/internal/storage/sqlite"
	"proxypool/internal/usage"
)

func settingsTestServer(t *testing.T) *Server {
	t.Helper()
	db, err := sqlite.Open(filepath.Join(t.TempDir(), "settings.db"), 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store := sqlite.New(db)
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	svc, err := usage.New(db, usage.Options{
		MasterKey: "0123456789abcdef0123456789abcdef",
		CheckTime: "09:00",
		Timezone:  "UTC",
	})
	if err != nil {
		t.Fatal(err)
	}
	return NewServer(testConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil, nil, nil, store, nil, nil, nil, WithUsage(svc))
}

func TestBarkTestRejectsLooseJSONBeforeDelivery(t *testing.T) {
	s := settingsTestServer(t)
	cases := []struct {
		name string
		body string
	}{
		{"unknown field", `{"title":"test","unexpected":true}`},
		{"malformed", `{"title":`},
		{"top-level null", `null`},
		{"trailing value", `{}{}`},
		{"oversized", `{"title":"` + strings.Repeat("x", 5000) + `"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/settings/bark/test",
				strings.NewReader(tc.body))
			rec := httptest.NewRecorder()
			s.handleBarkTestV1(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("loose body reached delivery: status=%d body=%s", rec.Code, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), `"code":"INVALID_REQUEST"`) {
				t.Fatalf("unexpected error: %s", rec.Body.String())
			}
		})
	}
}

func TestBarkTestAllowsEmptyRequestAndObject(t *testing.T) {
	s := settingsTestServer(t)
	for _, body := range []string{"", "{}", " \n\t "} {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/settings/bark/test",
			strings.NewReader(body))
		rec := httptest.NewRecorder()
		s.handleBarkTestV1(rec, req)
		if rec.Code != http.StatusBadGateway {
			t.Fatalf("empty request %q was not accepted: status=%d body=%s",
				body, rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), `"code":"BARK_TEST_FAILED"`) {
			t.Fatalf("empty request did not reach delivery path: %s", rec.Body.String())
		}
	}
}

func TestAdminLoginPanelFitsNarrowViewport(t *testing.T) {
	if !strings.Contains(adminPage, "#login .panel { width: 100%; max-width: 360px; }") {
		t.Fatal("login panel must shrink below 360px on narrow viewports")
	}
}

func TestBarkTestRouteAuthStrictnessOrdering(t *testing.T) {
	s := settingsTestServer(t)
	handler := s.Handler()
	looseBody := `{"title":"test","unexpected":true}`

	for _, tc := range []struct {
		name       string
		auth       string
		wantStatus int
		wantCode   string
	}{
		{"no credentials", "", http.StatusUnauthorized, "UNAUTHENTICATED"},
		{"invalid credentials", "Bearer invalid-token", http.StatusUnauthorized, "UNAUTHENTICATED"},
		{"authenticated loose body", "Bearer " + s.cfg.APIToken, http.StatusBadRequest, "INVALID_REQUEST"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost,
				"/api/v1/admin/settings/bark/test", strings.NewReader(looseBody))
			if tc.auth != "" {
				req.Header.Set("Authorization", tc.auth)
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != tc.wantStatus ||
				!strings.Contains(rec.Body.String(), `"code":"`+tc.wantCode+`"`) {
				t.Fatalf("status=%d body=%s, want %d %s",
					rec.Code, rec.Body.String(), tc.wantStatus, tc.wantCode)
			}
		})
	}

	cookie := &http.Cookie{Name: sessionCookie, Value: s.sessions.issue(time.Hour)}
	req := httptest.NewRequest(http.MethodPost,
		"/api/v1/admin/settings/bark/test", strings.NewReader(looseBody))
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden ||
		!strings.Contains(rec.Body.String(), `"code":"CSRF_HEADER_REQUIRED"`) {
		t.Fatalf("cookie mutation without CSRF header: status=%d body=%s",
			rec.Code, rec.Body.String())
	}

	// Empty object is the console's legitimate contract; reaching the
	// configured-delivery check proves middleware and strict decoding passed.
	req = httptest.NewRequest(http.MethodPost,
		"/api/v1/admin/settings/bark/test", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer "+s.cfg.APIToken)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadGateway ||
		!strings.Contains(rec.Body.String(), `"code":"BARK_TEST_FAILED"`) {
		t.Fatalf("empty object did not reach delivery path: status=%d body=%s",
			rec.Code, rec.Body.String())
	}
}

func TestSettingsRouteDoesNotEchoSecretBarkURL(t *testing.T) {
	s := settingsTestServer(t)
	secret := "route-query-secret"
	body := `{"bark_url":"https://bark.example.com/device?token=` + secret + `"}`
	req := httptest.NewRequest(http.MethodPut, "/api/v1/admin/settings",
		strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+s.cfg.APIToken)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code == http.StatusOK {
		t.Fatalf("secret-bearing Bark URL was accepted: %s", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), secret) {
		t.Fatalf("settings route echoed the Bark device/query secret: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "bark_url") {
		t.Fatalf("settings route did not identify the rejected field: %s", rec.Body.String())
	}
}
