package api

import (
	"context"
	"encoding/json"
	"fmt"
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

func TestSourceSecretAdminCASAndRedactedViews(t *testing.T) {
	db, err := sqlite.Open(filepath.Join(t.TempDir(), "source-secrets.db"), 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := sqlite.New(db)
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	cfg := testConfig()
	cfg.SecretEncryptionKey = "test-encryption-master-key-over-32-bytes"
	ingest := application.NewIngestService(store, cfg.IdentitySecret, true)
	keys := application.NewKeyService(store, cfg.IdentitySecret)
	_, downstream, err := keys.Create(context.Background(), "downstream", 0)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil, ingest, nil, store, nil, nil, keys)
	handler := server.Handler()
	request := func(method, path, token, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, r)
		return rec
	}
	create := request(http.MethodPost, "/api/v1/admin/sources", cfg.APIToken, `{"source_id":"private-account","adapter_type":"account_list","parser_type":"webshare_list_v1","enabled":false,"config":{"account":{"vendor_id":"webshare","protocol":"http"},"access":{"project_ids":["default"]}}}`)
	if create.Code != 201 {
		t.Fatalf("create account: %d %s", create.Code, create.Body.String())
	}
	var source sourceView
	if err := json.Unmarshal(create.Body.Bytes(), &source); err != nil {
		t.Fatal(err)
	}
	path := "/api/v1/admin/sources/private-account/secret"
	unauthorized := request(http.MethodPut, path, downstream, fmt.Sprintf(`{"config_version":%d,"value":"must-not-store"}`, source.ConfigVersion))
	if unauthorized.Code != 403 {
		t.Fatalf("downstream accessed source secret: %d %s", unauthorized.Code, unauthorized.Body.String())
	}
	rotated := request(http.MethodPut, path, cfg.APIToken, fmt.Sprintf(`{"config_version":%d,"value":"upstream-api-key-do-not-echo"}`, source.ConfigVersion))
	if rotated.Code != 200 || strings.Contains(rotated.Body.String(), "upstream-api-key") || rotated.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("secret rotation response: %d %s", rotated.Code, rotated.Body.String())
	}
	var status application.SourceSecretStatus
	if err := json.Unmarshal(rotated.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if !status.Configured || status.ConfigVersion <= source.ConfigVersion {
		t.Fatalf("rotation did not update version: %+v", status)
	}
	stale := request(http.MethodPut, path, cfg.APIToken, fmt.Sprintf(`{"config_version":%d,"value":"another"}`, source.ConfigVersion))
	if stale.Code != 409 {
		t.Fatalf("stale CAS accepted: %d %s", stale.Code, stale.Body.String())
	}
	for _, p := range []string{path, "/api/v1/admin/sources/private-account", "/api/v1/admin/sources", "/api/v1/admin/sources/private-account/cache"} {
		rec := request(http.MethodGet, p, cfg.APIToken, "")
		if rec.Code != 200 || strings.Contains(rec.Body.String(), "upstream-api-key-do-not-echo") || strings.Contains(rec.Body.String(), "ciphertext") {
			t.Fatalf("sensitive GET view: %s %d %s", p, rec.Code, rec.Body.String())
		}
	}
	clear := request(http.MethodDelete, path, cfg.APIToken, fmt.Sprintf(`{"config_version":%d}`, status.ConfigVersion))
	if clear.Code != 200 {
		t.Fatalf("clear: %d %s", clear.Code, clear.Body.String())
	}
	if err := json.Unmarshal(clear.Body.Bytes(), &status); err != nil || status.Configured || status.SecretVersion != 2 {
		t.Fatalf("clear status: %+v %v", status, err)
	}
}
