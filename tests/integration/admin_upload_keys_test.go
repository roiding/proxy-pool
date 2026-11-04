package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"proxypool/internal/application"
	"proxypool/internal/domain"
)

func adminUploadTestServer(t *testing.T) (*env, *httptest.Server, string) {
	t.Helper()
	e := newEnv(t, startJudge(t), nil)
	for _, id := range []string{"upload-main", "upload-other"} {
		if err := e.ingest.EnsureSource(context.Background(), id, application.SourceConfig{
			DefaultTransport: domain.TransportHTTP, MaxPoolLeases: 1,
		}, true); err != nil {
			t.Fatal(err)
		}
	}
	srv := auditServer(t, e)
	return e, srv, "upload-main"
}

func adminUploadRequest(t *testing.T, srv *httptest.Server, method, token, path,
	contentType, idemKey, body string) (int, []byte) {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, srv.URL+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, data
}

func adminUploadCreateKey(t *testing.T, srv *httptest.Server, master, name string, sources ...string) (string, map[string]any) {
	t.Helper()
	ids := make([]string, len(sources))
	for i, s := range sources {
		ids[i] = fmt.Sprintf("%q", s)
	}
	body := fmt.Sprintf(`{"name":%q,"source_ids":[%s]}`, name, strings.Join(ids, ","))
	status, data := adminUploadRequest(t, srv, http.MethodPost, master, "/api/v1/admin/upload-keys",
		"application/json", "", body)
	if status != http.StatusCreated {
		t.Fatalf("create upload key: status=%d body=%s", status, data)
	}
	var out struct {
		Key   map[string]any `json:"key"`
		Token string         `json:"token"`
	}
	if err := json.Unmarshal(data, &out); err != nil || !strings.HasPrefix(out.Token, "uk_") {
		t.Fatalf("create upload key returned no uk_ token: body=%s err=%v", data, err)
	}
	return out.Token, out.Key
}

func TestAdminUploadKeyLifecycleAndImportScope(t *testing.T) {
	e, srv, sourceID := adminUploadTestServer(t)
	master := e.cfg.APIToken

	// Creation returns plaintext once and the masked list never leaks it.
	token, key := adminUploadCreateKey(t, srv, master, "uploader-one", "upload-main")
	if key["token_prefix"] == nil || key["source_ids"] == nil {
		t.Fatalf("masked key output lost prefix or scopes: %v", key)
	}
	status, list := adminUploadRequest(t, srv, http.MethodGet, master,
		"/api/v1/admin/upload-keys", "", "", "")
	if status != http.StatusOK || strings.Contains(string(list), token) ||
		strings.Contains(string(list), "token_hash") {
		t.Fatalf("upload key list leaked credential material: status=%d body=%s", status, list)
	}

	// An upload key cannot reach admin or acquisition surfaces.
	for _, path := range []string{"/api/v1/admin/sources", "/api/v1/admin/keys"} {
		if status, _ := adminUploadRequest(t, srv, http.MethodGet, token, path, "", "", ""); status != http.StatusForbidden {
			t.Fatalf("upload key reached %s: status=%d", path, status)
		}
	}
	if status, _ := adminUploadRequest(t, srv, http.MethodPost, token, "/api/v1/proxies/acquire",
		"application/json", "", `{"count":1}`); status != http.StatusForbidden {
		t.Fatalf("upload key acquired proxies: status=%d", status)
	}

	// A scoped import with an Idempotency-Key applies once (201), then the
	// same content and key replay the stored result (200, same import_id).
	status, data := adminUploadRequest(t, srv, http.MethodPost, token,
		"/api/v1/sources/"+sourceID+"/imports", "text/plain", "retry-1", "http://10.0.0.1:8080\n")
	if status != http.StatusCreated {
		t.Fatalf("first import: status=%d body=%s", status, data)
	}
	var first map[string]any
	if err := json.Unmarshal(data, &first); err != nil || first["import_id"] == "" {
		t.Fatalf("import result missing import_id: %s", data)
	}
	if first["added"] != float64(1) || first["rejected"] != float64(0) {
		t.Fatalf("unexpected import counts: %v", first)
	}
	status, data = adminUploadRequest(t, srv, http.MethodPost, token,
		"/api/v1/sources/"+sourceID+"/imports", "text/plain", "retry-1", "http://10.0.0.1:8080\n")
	if status != http.StatusOK {
		t.Fatalf("idempotent replay: status=%d body=%s", status, data)
	}
	var replay map[string]any
	if err := json.Unmarshal(data, &replay); err != nil || replay["import_id"] != first["import_id"] {
		t.Fatalf("replay changed the batch identity: %s", data)
	}

	// Results are source-scoped and owner-only: the owner reads its own
	// batch, an unscoped source is denied, another key sees nothing.
	status, _ = adminUploadRequest(t, srv, http.MethodGet, token,
		"/api/v1/sources/"+sourceID+"/imports/"+first["import_id"].(string), "", "", "")
	if status != http.StatusOK {
		t.Fatalf("owner result read: status=%d", status)
	}
	status, _ = adminUploadRequest(t, srv, http.MethodGet, token,
		"/api/v1/sources/upload-other/imports/"+first["import_id"].(string), "", "", "")
	if status != http.StatusForbidden {
		t.Fatalf("unscoped source read: status=%d", status)
	}
	otherToken, _ := adminUploadCreateKey(t, srv, master, "uploader-two", "upload-main", "upload-other")
	if status, _ := adminUploadRequest(t, srv, http.MethodGet, otherToken,
		"/api/v1/sources/"+sourceID+"/imports/"+first["import_id"].(string), "", "", ""); status != http.StatusNotFound {
		t.Fatalf("foreign key read another owner's batch: status=%d", status)
	}

	// A project acquisition key must not import.
	_, projToken, err := e.keys.CreateForProject(context.Background(), "default", "downloader", 0)
	if err != nil {
		t.Fatal(err)
	}
	if status, _ := adminUploadRequest(t, srv, http.MethodPost, projToken,
		"/api/v1/sources/"+sourceID+"/imports", "text/plain", "", "http://10.0.0.2:8080\n"); status != http.StatusForbidden {
		t.Fatalf("project key imported: status=%d", status)
	}

	// Disabling one key leaves sibling keys and admin access intact.
	status, _ = adminUploadRequest(t, srv, http.MethodPatch, master,
		"/api/v1/admin/upload-keys/"+key["key_id"].(string), "application/json", "", `{"enabled":false}`)
	if status != http.StatusOK {
		t.Fatalf("disable upload key: status=%d", status)
	}
	if status, _ := adminUploadRequest(t, srv, http.MethodPost, token,
		"/api/v1/sources/"+sourceID+"/imports", "text/plain", "", "http://10.0.0.3:8080\n"); status != http.StatusUnauthorized {
		t.Fatalf("disabled upload key still imported: status=%d", status)
	}
	if status, data := adminUploadRequest(t, srv, http.MethodPost, otherToken,
		"/api/v1/sources/upload-other/imports", "text/plain", "", "http://10.0.0.4:8080\n"); status != http.StatusCreated {
		t.Fatalf("sibling key broken by disable: status=%d body=%s", status, data)
	}

	// Deletion revokes the credential permanently.
	status, _ = adminUploadRequest(t, srv, http.MethodDelete, master,
		"/api/v1/admin/upload-keys/"+key["key_id"].(string), "", "", "")
	if status != http.StatusOK {
		t.Fatalf("delete upload key: status=%d", status)
	}
	if status, _ := adminUploadRequest(t, srv, http.MethodGet, token,
		"/api/v1/sources/"+sourceID+"/imports/"+first["import_id"].(string), "", "", ""); status != http.StatusUnauthorized {
		t.Fatalf("deleted upload key still read: status=%d", status)
	}
}

func TestAdminUploadKeyFailsClosedBeforeApplying(t *testing.T) {
	e, srv, _ := adminUploadTestServer(t)
	master := e.cfg.APIToken
	token, _ := adminUploadCreateKey(t, srv, master, "scoped", "upload-main")

	// An unscoped source must be denied before any inventory is touched.
	if status, _ := adminUploadRequest(t, srv, http.MethodPost, token,
		"/api/v1/sources/upload-other/imports", "text/plain", "", "http://10.0.0.9:8080\n"); status != http.StatusForbidden {
		t.Fatalf("unscoped import: status=%d", status)
	}
	stats, err := e.store.Stats(context.Background())
	if err != nil || stats.Resources != 0 {
		t.Fatalf("denied import touched inventory: stats=%+v err=%v", stats, err)
	}

	// Structural errors reject the whole batch; item errors are reported
	// with a count and bounded sanitized details while legal items append.
	status, data := adminUploadRequest(t, srv, http.MethodPost, token,
		"/api/v1/sources/upload-main/imports", "application/json", "", `{"items":"not-an-array"}`)
	if status < 400 || status >= 500 {
		t.Fatalf("structural error accepted: status=%d body=%s", status, data)
	}
	bad := `{"items":[{"connection":{"transport":"http","host":"10.0.0.10","port":8080}},` +
		`{"connection":{"transport":"http","host":"10.0.0.11"}}]}`
	status, data = adminUploadRequest(t, srv, http.MethodPost, token,
		"/api/v1/sources/upload-main/imports", "application/json", "", bad)
	if status != http.StatusCreated {
		t.Fatalf("mixed batch: status=%d body=%s", status, data)
	}
	var result map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	if result["added"] != float64(1) || result["rejected"] != float64(1) {
		t.Fatalf("mixed batch counts: %v", result)
	}
	if errs, ok := result["errors"].([]any); !ok || len(errs) != 1 {
		t.Fatalf("expected exactly one bounded error entry: %v", result["errors"])
	}
}

// A key revoked while its import request is still in flight must not apply:
// the request-start authentication snapshot alone is not authorization.
func TestAdminUploadKeyRevokedMidRequestCannotApply(t *testing.T) {
	e, srv, _ := adminUploadTestServer(t)
	master := e.cfg.APIToken
	token, _ := adminUploadCreateKey(t, srv, master, "inflight", "upload-main")

	pipeReader, pipeWriter := io.Pipe()
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/api/v1/sources/upload-main/imports", pipeReader)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "text/plain")
	statusCh := make(chan int, 1)
	go func() {
		resp, err := (&http.Client{}).Do(req)
		if err != nil {
			t.Error(err)
			statusCh <- 0
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		statusCh <- resp.StatusCode
	}()

	// io.Pipe writes block until the server consumes them, so this returning
	// proves the server authenticated the request and started reading the
	// body while the key was still enabled.
	if _, err := pipeWriter.Write([]byte("http://10.0.1.1:8080\n")); err != nil {
		t.Fatal(err)
	}
	status, _ := adminUploadRequest(t, srv, http.MethodPatch, master,
		"/api/v1/admin/upload-keys/"+firstUploadKeyID(t, srv, master), "application/json", "", `{"enabled":false}`)
	if status != http.StatusOK {
		t.Fatalf("disable key mid-flight: status=%d", status)
	}
	if err := pipeWriter.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-statusCh:
		if got != http.StatusUnauthorized {
			t.Fatalf("in-flight import after revocation: status=%d, want 401", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("in-flight import never completed")
	}
	stats, err := e.store.Stats(context.Background())
	if err != nil || stats.Resources != 0 {
		t.Fatalf("revoked key applied its batch: stats=%+v err=%v", stats, err)
	}
}

func firstUploadKeyID(t *testing.T, srv *httptest.Server, master string) string {
	t.Helper()
	status, data := adminUploadRequest(t, srv, http.MethodGet, master, "/api/v1/admin/upload-keys", "", "", "")
	if status != http.StatusOK {
		t.Fatalf("list upload keys: status=%d body=%s", status, data)
	}
	var keys []map[string]any
	if err := json.Unmarshal(data, &keys); err != nil || len(keys) == 0 {
		t.Fatalf("no upload keys to disable: %s", data)
	}
	id, _ := keys[0]["key_id"].(string)
	if id == "" {
		t.Fatalf("upload key list missing key_id: %s", data)
	}
	return id
}
