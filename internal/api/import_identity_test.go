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
	"sync"
	"testing"
	"time"

	"proxypool/internal/application"
	"proxypool/internal/domain"
	"proxypool/internal/storage/sqlite"
)

type importIdentityServices struct {
	server *Server
	store  *sqlite.Store
	ingest *application.IngestService
	keys   *application.UploadKeyService
}

func newImportIdentityServices(t *testing.T) importIdentityServices {
	t.Helper()
	db, err := sqlite.Open(filepath.Join(t.TempDir(), "import-identity.db"), 0)
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
	keys := application.NewUploadKeyService(store, cfg.IdentitySecret)
	s := NewServer(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil, ingest, application.NewLeaseService(store, application.CooldownConfig{}),
		store, nil, nil, nil, WithUploadKeys(keys))
	return importIdentityServices{server: s, store: store, ingest: ingest, keys: keys}
}

func (s importIdentityServices) createSource(t *testing.T, id string) {
	t.Helper()
	if err := s.ingest.EnsureSource(context.Background(), id, application.SourceConfig{
		DefaultTransport: domain.TransportHTTP,
		MaxPoolLeases:    1,
	}, true); err != nil {
		t.Fatal(err)
	}
}

func (s importIdentityServices) request(t *testing.T, method, path, token, idempotencyKey string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader("http://198.51.100.10:8080\n"))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "text/plain")
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	rec := httptest.NewRecorder()
	s.server.Handler().ServeHTTP(rec, req)
	return rec
}

func decodeImportIdentityResult(t *testing.T, rec *httptest.ResponseRecorder) application.ImportBatchResult {
	t.Helper()
	var result application.ImportBatchResult
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil || result.ImportID == "" {
		t.Fatalf("missing durable import result: status=%d body=%s err=%v",
			rec.Code, rec.Body.String(), err)
	}
	return result
}

func importIdentityCount(t *testing.T, store *sqlite.Store, table string) int {
	t.Helper()
	var exists, count int
	if err := store.DB().QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists == 0 {
		return 0
	}
	if err := store.DB().QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestImportHandlersForwardOnlyTrustedUploadKeyIdentity(t *testing.T) {
	s := newImportIdentityServices(t)
	s.createSource(t, "upload-source")
	key, token, err := s.keys.Create(context.Background(), "regression-uploader", []string{"upload-source"})
	if err != nil {
		t.Fatal(err)
	}
	const importPath = "/api/v1/sources/upload-source/imports"

	first := s.request(t, http.MethodPost, importPath, token, "before-revoke")
	if first.Code != http.StatusCreated {
		t.Fatalf("active key import: status=%d body=%s", first.Code, first.Body.String())
	}
	firstResult := decodeImportIdentityResult(t, first)
	replay := s.request(t, http.MethodPost, importPath, token, "before-revoke")
	if replay.Code != http.StatusOK {
		t.Fatalf("active key replay: status=%d body=%s", replay.Code, replay.Body.String())
	}
	if replayResult := decodeImportIdentityResult(t, replay); replayResult.ImportID != firstResult.ImportID {
		t.Fatalf("replay changed import_id: first=%s replay=%s",
			firstResult.ImportID, replayResult.ImportID)
	}

	arrived := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	s.ingest.Now = func() time.Time {
		once.Do(func() {
			close(arrived)
			<-release
		})
		return time.Now().UTC()
	}
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- s.request(t, http.MethodPost, importPath, token, "before-revoke")
	}()
	select {
	case <-arrived:
	case <-time.After(4 * time.Second):
		t.Fatal("import did not reach the post-authorization source snapshot")
	}
	disabled := false
	if err := s.keys.Update(context.Background(), key.ID, nil, &disabled); err != nil {
		t.Fatal(err)
	}
	beforeBatches := importIdentityCount(t, s.store, "import_batches")
	beforeIdempotency := importIdentityCount(t, s.store, "import_idempotency")
	beforeInventory := importIdentityCount(t, s.store, "proxy_resources")
	close(release)
	var revoked *httptest.ResponseRecorder
	select {
	case revoked = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("revoked in-flight import did not finish")
	}
	if revoked.Code != http.StatusUnauthorized && revoked.Code != http.StatusForbidden {
		t.Fatalf("revoked key was authorized by handler identity: status=%d body=%s",
			revoked.Code, revoked.Body.String())
	}
	if importIdentityCount(t, s.store, "import_batches") != beforeBatches ||
		importIdentityCount(t, s.store, "import_idempotency") != beforeIdempotency ||
		importIdentityCount(t, s.store, "proxy_resources") != beforeInventory {
		t.Fatal("revoked-key rejection mutated durable import or inventory state")
	}
}

func TestImportHandlersPassNoUploadKeyIDForAdmin(t *testing.T) {
	s := newImportIdentityServices(t)
	s.createSource(t, "admin-source")
	beforeBatches := importIdentityCount(t, s.store, "import_batches")
	beforeInventory := importIdentityCount(t, s.store, "proxy_resources")

	admin := s.request(t, http.MethodPost,
		"/api/v1/admin/sources/admin-source/imports", s.server.cfg.APIToken, "admin-idempotency")
	if admin.Code != http.StatusCreated {
		t.Fatalf("admin import: status=%d body=%s", admin.Code, admin.Body.String())
	}
	var uploaderID string
	if err := s.store.DB().QueryRow(`SELECT uploader_id FROM import_batches LIMIT 1`).Scan(&uploaderID); err != nil {
		t.Fatal(err)
	}
	if uploaderID != sqlite.DefaultProjectID {
		t.Fatalf("admin import stored unexpected trusted uploader: %q", uploaderID)
	}
	if importIdentityCount(t, s.store, "import_batches") != beforeBatches+1 ||
		importIdentityCount(t, s.store, "proxy_resources") != beforeInventory+1 {
		t.Fatal("admin import did not commit through the shared importer")
	}
}
