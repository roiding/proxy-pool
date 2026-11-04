package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"proxypool/internal/application"
	"proxypool/internal/storage/sqlite"
)

func wiringTestServer(t *testing.T) *Server {
	t.Helper()
	return &Server{cfg: testConfig(), log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		sessions: newSessions(4), logins: newLoginLimiter(0, 10)}
}

func withIdentity(ctx context.Context, ident authIdentity) context.Context {
	return context.WithValue(ctx, authContextKey{}, ident)
}

func TestBuildAcquireV1ForwardsNormalizedValidationScope(t *testing.T) {
	s := wiringTestServer(t)
	ident := authIdentity{ConsumerID: "proj_a", ProjectID: "proj_a"}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/proxies/acquire", nil)
	var body acquireV1Request
	if err := json.Unmarshal([]byte(`{"validation":{"mode":"tls","target":"api.example.com:443"}}`), &body); err != nil {
		t.Fatal(err)
	}
	built, derr := s.buildAcquireV1(req.WithContext(withIdentity(req.Context(), ident)), &body)
	if derr != nil {
		t.Fatalf("tls request rejected: %v", derr)
	}
	if built.Validation == nil || built.Validation.Mode != "tls" ||
		built.Validation.TargetHostPort != "api.example.com:443" {
		t.Fatalf("normalized scope not forwarded: %+v", built.Validation)
	}
	if built.Profile != "" {
		t.Fatalf("handler must not pin a profile; core derives it: %q", built.Profile)
	}

	body = acquireV1Request{}
	if err := json.Unmarshal([]byte(`{"validation":{"mode":"basic"}}`), &body); err != nil {
		t.Fatal(err)
	}
	built, derr = s.buildAcquireV1(req.WithContext(withIdentity(req.Context(), ident)), &body)
	if derr != nil {
		t.Fatalf("basic request rejected: %v", derr)
	}
	if built.Validation == nil || built.Validation.Mode != "basic" {
		t.Fatalf("explicit basic scope not forwarded: %+v", built.Validation)
	}

	body = acquireV1Request{}
	if err := json.Unmarshal([]byte(`{"count":1}`), &body); err != nil {
		t.Fatal(err)
	}
	built, derr = s.buildAcquireV1(req.WithContext(withIdentity(req.Context(), ident)), &body)
	if derr != nil {
		t.Fatalf("default request rejected: %v", derr)
	}
	if built.Validation != nil {
		t.Fatalf("omitted validation must stay nil so the project default applies: %+v", built.Validation)
	}
}

func TestBuildAcquireV1StickyReachesCore(t *testing.T) {
	s := wiringTestServer(t)
	var body acquireV1Request
	if err := json.Unmarshal([]byte(`{"count":1,"mode":"sticky","session_id":"sess-1"}`), &body); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/proxies/acquire", nil)
	ident := authIdentity{ConsumerID: "proj_a", ProjectID: "proj_a", KeyID: "key_1"}
	built, derr := s.buildAcquireV1(req.WithContext(withIdentity(req.Context(), ident)), &body)
	if derr != nil {
		t.Fatalf("sticky request rejected: %v", derr)
	}
	if built.Lease.Mode != "sticky" || built.SessionID != "sess-1" {
		t.Fatalf("sticky wiring lost: mode=%q session=%q", built.Lease.Mode, built.SessionID)
	}
	if built.KeyID != "key_1" || built.ProjectID != "proj_a" || built.ConsumerID != "proj_a" {
		t.Fatalf("auth ids not forwarded: %+v", built)
	}
}

func TestResolveSourceScopeExplicitEmptyIsNotAll(t *testing.T) {
	s := wiringTestServer(t)
	empty := optionalStringSlice{Present: true, Values: []string{}}
	restricted := authIdentity{SourceScopeRestricted: true, AllowedSourceIDs: []string{"a"}}
	got, derr := s.resolveSourceScope(restricted, empty)
	if derr != nil || got == nil || len(got) != 0 {
		t.Fatalf("explicit [] on restricted project: got=%v err=%v", got, derr)
	}
	got, derr = s.resolveSourceScope(restricted, optionalStringSlice{})
	if derr != nil || len(got) != 1 || got[0] != "a" {
		t.Fatalf("omitted filter must resolve to the allowlist: got=%v err=%v", got, derr)
	}
	denied := authIdentity{SourceScopeRestricted: true}
	if _, derr := s.resolveSourceScope(denied, optionalStringSlice{}); derr == nil || derr.Status != 403 {
		t.Fatalf("empty allowlist must fail closed: %v", derr)
	}
	unrestricted := authIdentity{}
	got, derr = s.resolveSourceScope(unrestricted, empty)
	if derr != nil || got == nil || len(got) != 0 {
		t.Fatalf("explicit [] on unrestricted project must mean none: got=%v err=%v", got, derr)
	}
	got, derr = s.resolveSourceScope(unrestricted, optionalStringSlice{})
	if derr != nil || got != nil {
		t.Fatalf("omitted filter on unrestricted project must mean all: got=%v err=%v", got, derr)
	}
}

func TestProjectSourcesTriStateMapping(t *testing.T) {
	if got, derr := sourceScopeFromOptional(optionalStringSlice{}); derr != nil || got != nil {
		t.Fatalf("missing field: got=%v err=%v", got, derr)
	}
	null := optionalStringSlice{Present: true, Null: true}
	if got, derr := sourceScopeFromOptional(null); derr != nil || got != nil {
		t.Fatalf("explicit null must restore unrestricted: got=%v err=%v", got, derr)
	}
	empty := optionalStringSlice{Present: true, Values: []string{}}
	got, derr := sourceScopeFromOptional(empty)
	if derr != nil || got == nil || len(got) != 0 {
		t.Fatalf("explicit [] must be an empty allowlist: got=%v err=%v", got, derr)
	}
	list := optionalStringSlice{Present: true, Values: []string{" a ", "b", "b"}}
	if _, derr := sourceScopeFromOptional(list); derr == nil {
		t.Fatal("duplicated ids must be rejected")
	}
}

func TestSettingsEndpointsRequireUsageService(t *testing.T) {
	s := wiringTestServer(t)
	handler := s.Handler()
	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, "/api/v1/admin/settings"},
		{http.MethodGet, "/api/v1/admin/usage"},
	} {
		req := httptest.NewRequest(tc.method, tc.path, nil)
		resp := httptest.NewRecorder()
		handler.ServeHTTP(resp, req)
		if resp.Code != http.StatusUnauthorized {
			t.Fatalf("%s %s must require auth first: %d", tc.method, tc.path, resp.Code)
		}
	}
}

func TestAcquireV1CountTriStateAndCountryStrictness(t *testing.T) {
	s := wiringTestServer(t)
	ident := authIdentity{ConsumerID: "proj_a", ProjectID: "proj_a"}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/proxies/acquire", nil)
	ctx := withIdentity(req.Context(), ident)
	for _, tc := range []struct {
		name string
		body string
	}{
		{"count null", `{"count":null}`},
		{"country empty", `{"count":1,"country":[""]}`},
		{"country null entry", `{"count":1,"country":[null]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var body acquireV1Request
			if err := json.Unmarshal([]byte(tc.body), &body); err != nil {
				t.Fatal(err)
			}
			if _, derr := s.buildAcquireV1(req.WithContext(ctx), &body); derr == nil ||
				derr.Status != http.StatusBadRequest {
				t.Fatalf("invalid contract accepted: %+v", derr)
			}
		})
	}

	var body acquireV1Request
	if err := json.Unmarshal([]byte(`{"country":[" cn "],"source_ids":null}`), &body); err != nil {
		t.Fatal(err)
	}
	built, derr := s.buildAcquireV1(req.WithContext(ctx), &body)
	if derr != nil {
		t.Fatalf("{}-equivalent request rejected: %v", derr)
	}
	if built.Count != 1 || len(built.Filters.Countries) != 1 || built.Filters.Countries[0] != "CN" ||
		built.Filters.SourceIDs != nil {
		t.Fatalf("default or null scope resolution is wrong: %+v", built)
	}
}

func TestAcquireV1RejectsTopLevelNull(t *testing.T) {
	db, err := sqlite.Open(filepath.Join(t.TempDir(), "api.db"), 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store := sqlite.New(db)
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	cfg := testConfig()
	ingest := application.NewIngestService(store, cfg.IdentitySecret, true)
	acquire := application.NewAcquireService(store, cfg, "", nil, make(chan struct{}, 1), nil)
	s := NewServer(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)),
		acquire, ingest, application.NewLeaseService(store, application.CooldownConfig{}),
		store, nil, nil, nil)
	req, err := http.NewRequest(http.MethodPost, "/api/v1/proxies/acquire", strings.NewReader("null"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+cfg.APIToken)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("top-level null accepted: status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestPrewarmV1RejectsCountAboveConfiguredMaximum(t *testing.T) {
	s := wiringTestServer(t)
	var body prewarmJobBody
	body.Count = new(int)
	*body.Count = s.cfg.MaxCount + 1

	encoder, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/prewarm", strings.NewReader(string(encoder)))
	req = req.WithContext(withIdentity(req.Context(), authIdentity{
		Admin: true, ProjectID: sqlite.DefaultProjectID, ConsumerID: sqlite.DefaultProjectID,
	}))
	rec := httptest.NewRecorder()
	s.handlePrewarmV1(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("oversized prewarm target accepted: status=%d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "exceeds the maximum") {
		t.Fatalf("oversized target missing explicit bound error: %s", rec.Body.String())
	}
}
