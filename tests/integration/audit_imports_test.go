package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"proxypool/internal/application"
	"proxypool/internal/domain"
)

const auditImportSource = "audit-import-main"

type auditImportReply struct {
	Status int
	Body   []byte
	Err    error
}

func auditImportCall(srv *httptest.Server, method, token, path, contentType, idem, body string) auditImportReply {
	req, err := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
	if err != nil {
		return auditImportReply{Err: err}
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if idem != "" {
		req.Header.Set("Idempotency-Key", idem)
	}
	resp, err := (&http.Client{Timeout: 8 * time.Second}).Do(req)
	if err != nil {
		return auditImportReply{Err: err}
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 256<<10))
	return auditImportReply{Status: resp.StatusCode, Body: data, Err: err}
}

func auditImportCreateKey(t *testing.T, e *env, srv *httptest.Server, name string, sources ...string) (domain.UploadKey, string) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"name": name, "source_ids": sources})
	if err != nil {
		t.Fatal(err)
	}
	r := auditImportCall(srv, http.MethodPost, e.cfg.APIToken, "/api/v1/admin/upload-keys", "application/json", "", string(body))
	var out struct {
		Key   domain.UploadKey `json:"key"`
		Token string           `json:"token"`
	}
	if r.Err != nil || r.Status != http.StatusCreated || json.Unmarshal(r.Body, &out) != nil || out.Key.ID == "" || out.Token == "" {
		t.Fatalf("could not create the isolated upload credential: status=%d err=%v", r.Status, r.Err)
	}
	return out.Key, out.Token
}

func auditImportEnv(t *testing.T) (*env, *httptest.Server, domain.UploadKey, string) {
	t.Helper()
	e := newEnv(t, startJudge(t), nil)
	for _, id := range []string{auditImportSource, "audit-import-other"} {
		if err := e.ingest.EnsureSource(context.Background(), id, application.SourceConfig{
			DefaultTransport: domain.TransportHTTP, MaxPoolLeases: 1,
		}, true); err != nil {
			t.Fatal(err)
		}
	}
	srv := auditServer(t, e)
	key, token := auditImportCreateKey(t, e, srv, "audit-import-key", auditImportSource)
	return e, srv, key, token
}

func auditImportResult(t *testing.T, r auditImportReply) application.ImportBatchResult {
	t.Helper()
	var out application.ImportBatchResult
	if r.Err != nil || (r.Status != http.StatusCreated && r.Status != http.StatusOK) ||
		json.Unmarshal(r.Body, &out) != nil || out.ImportID == "" {
		t.Fatalf("import did not produce a durable batch result: status=%d body=%s err=%v", r.Status, r.Body, r.Err)
	}
	return out
}

func auditImportTableCount(t *testing.T, e *env, table string) int {
	t.Helper()
	var exists, count int
	if err := e.store.DB().QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists == 0 {
		return 0
	}
	if err := e.store.DB().QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestAuditImportFileAndJSONShareInventoryWithoutResettingHealthOrCD(t *testing.T) {
	e, srv, _, token := auditImportEnv(t)
	proxy := startFakeProxy(t)
	u, err := url.Parse(proxy.URL)
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword("audit:user @/%", "audit:password @/%")
	text := u.String() + "\n"
	first := auditImportResult(t, auditImportCall(srv, http.MethodPost, e.cfg.APIToken,
		"/api/v1/admin/sources/"+auditImportSource+"/imports", "text/plain", "", text))
	if first.Added != 1 || first.Rejected != 0 || first.Duplicate != 0 {
		t.Fatalf("admin file import returned wrong counts: %+v", first)
	}
	if auditImportTableCount(t, e, "validation_evidence") != 0 || auditImportTableCount(t, e, "resource_observations") != 0 {
		t.Fatal("import invented validation or an exit observation")
	}
	cd := int64(600)
	delivery, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{
		Count: 1, ProjectID: application.DefaultAcquireProject, ConsumerID: application.DefaultAcquireProject,
		CDSeconds: &cd, Filters: application.Filters{SourceIDs: []string{auditImportSource}},
	})
	if derr != nil || delivery.Delivered != 1 || len(delivery.Items) != 1 {
		t.Fatalf("isolated imported connection could not actually validate: err=%v", derr)
	}
	resourceID := delivery.Items[0].ResourceID
	var incarnation, updatedAt string
	if err := e.store.DB().QueryRow(`SELECT incarnation, updated_at FROM proxy_resources WHERE resource_id=?`, resourceID).
		Scan(&incarnation, &updatedAt); err != nil {
		t.Fatal(err)
	}
	fingerprint := e.acquire.FingerprintOf(delivery.Items[0].Conn)
	beforeCD, err := e.store.ProjectCooldownBlocked(context.Background(), application.DefaultAcquireProject, time.Now(), []string{fingerprint})
	if err != nil || beforeCD[fingerprint].IsZero() {
		t.Fatalf("initial delivery did not commit CD: err=%v", err)
	}
	body, err := json.Marshal(map[string]any{"items": []any{map[string]any{
		"connection": delivery.Items[0].Conn, "provider_expires_at": nil,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	second := auditImportResult(t, auditImportCall(srv, http.MethodPost, token,
		"/api/v1/sources/"+auditImportSource+"/imports", "application/json", "", string(body)))
	if second.ImportID == first.ImportID || second.Added != 0 || second.Updated != 0 || second.Duplicate != 1 || second.Rejected != 0 {
		t.Fatalf("JSON/raw credentials did not merge with file URI identity: %+v", second)
	}
	var currentIncarnation, currentUpdatedAt string
	if err := e.store.DB().QueryRow(`SELECT incarnation, updated_at FROM proxy_resources WHERE resource_id=?`, resourceID).
		Scan(&currentIncarnation, &currentUpdatedAt); err != nil {
		t.Fatal(err)
	}
	afterCD, err := e.store.ProjectCooldownBlocked(context.Background(), application.DefaultAcquireProject, time.Now(), []string{fingerprint})
	if err != nil || !afterCD[fingerprint].Equal(beforeCD[fingerprint]) || incarnation != currentIncarnation || updatedAt != currentUpdatedAt {
		t.Fatal("cross-entry duplicate reset inventory incarnation or committed project CD")
	}
	stats, err := e.store.Stats(context.Background())
	if err != nil || stats.Resources != 1 || stats.EnabledOffers != 1 || stats.HealthPass != 1 {
		t.Fatalf("cross-entry duplicate created a second inventory or erased PASS health: stats=%+v err=%v", stats, err)
	}
}

func TestAuditImportIdempotencyAndPermissionsAreIdentityAndSourceScoped(t *testing.T) {
	e, srv, _, token := auditImportEnv(t)
	path := "/api/v1/sources/" + auditImportSource + "/imports"
	body := "http://8.8.8.8:8080\n"
	first := auditImportResult(t, auditImportCall(srv, http.MethodPost, token, path, "text/plain", "audit-retry", body))
	replayReply := auditImportCall(srv, http.MethodPost, token, path, "text/plain", "audit-retry", body)
	replay := auditImportResult(t, replayReply)
	if replayReply.Status != http.StatusOK || replay.ImportID != first.ImportID || replay.Added != first.Added || replay.CreatedAt != first.CreatedAt {
		t.Fatal("same uploader/source/key/content changed its durable batch result")
	}
	conflict := auditImportCall(srv, http.MethodPost, token, path, "text/plain", "audit-retry", "http://8.8.4.4:8080\n")
	if conflict.Err != nil || conflict.Status != http.StatusConflict || auditImportTableCount(t, e, "proxy_resources") != 1 {
		t.Fatalf("different content re-applied the idempotency key: status=%d err=%v", conflict.Status, conflict.Err)
	}
	_, sibling := auditImportCreateKey(t, e, srv, "audit-import-sibling", auditImportSource, "audit-import-other")
	other := auditImportResult(t, auditImportCall(srv, http.MethodPost, sibling, path, "text/plain", "audit-retry", body))
	if other.ImportID == first.ImportID || other.Duplicate != 1 {
		t.Fatal("different uploader shared the first uploader's batch identity")
	}
	for _, tc := range []struct {
		token, path string
		status      int
	}{
		{token, path + "/" + first.ImportID, http.StatusOK},
		{sibling, path + "/" + first.ImportID, http.StatusNotFound},
		{token, "/api/v1/sources/audit-import-other/imports/" + first.ImportID, http.StatusForbidden},
		{e.cfg.APIToken, path + "/" + first.ImportID, http.StatusOK},
	} {
		r := auditImportCall(srv, http.MethodGet, tc.token, tc.path, "", "", "")
		if r.Err != nil || r.Status != tc.status {
			t.Fatalf("batch query escaped owner/source scope: status=%d expected=%d err=%v", r.Status, tc.status, r.Err)
		}
	}
	_, projectToken, err := e.keys.CreateForProject(context.Background(), application.DefaultAcquireProject, "audit-downloader-only", 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ token, path, body string }{
		{projectToken, path, body},
		{token, "/api/v1/proxies/acquire", `{"count":1}`},
		{token, "/api/v1/admin/upload-keys", `{"name":"audit-escalation","source_ids":["audit-import-other"]}`},
	} {
		r := auditImportCall(srv, http.MethodPost, tc.token, tc.path, "application/json", "", tc.body)
		if r.Err != nil || r.Status != http.StatusForbidden {
			t.Fatalf("credential type escalated into another authority: path=%s status=%d err=%v", tc.path, r.Status, r.Err)
		}
	}
	list := auditImportCall(srv, http.MethodGet, e.cfg.APIToken, "/api/v1/admin/upload-keys", "", "", "")
	if list.Err != nil || list.Status != http.StatusOK || strings.Contains(string(list.Body), token) ||
		strings.Contains(string(list.Body), sibling) || strings.Contains(string(list.Body), "token_hash") {
		t.Fatal("masked upload-key list exposed recoverable credential material")
	}
}

func TestAuditImportStructuralErrorsAndLimitsRejectWholeBatch(t *testing.T) {
	validJSON := `{"items":[{"connection":{"transport":"http","host":"8.8.8.8","port":8080}}]}`
	cases := []struct {
		name, contentType, body string
		status                  int
	}{
		{"null", "application/json", `null`, http.StatusBadRequest},
		{"trailing_value", "application/json", validJSON + ` {}`, http.StatusBadRequest},
		{"trailing_garbage", "application/json", validJSON + ` garbage`, http.StatusBadRequest},
		{"wrong_items_type", "application/json", `{"items":"not-an-array"}`, http.StatusBadRequest},
		{"secret_unknown_field", "application/json", `{"audit-private-password-marker":true,"items":[]}`, http.StatusBadRequest},
		{"malformed_media_type", "application/json; charset", validJSON, http.StatusBadRequest},
		{"text_item_limit", "text/plain", strings.Repeat("http://8.8.8.8:8080\n", application.MaxImportItems+1), http.StatusRequestEntityTooLarge},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e, srv, _, token := auditImportEnv(t)
			r := auditImportCall(srv, http.MethodPost, token, "/api/v1/sources/"+auditImportSource+"/imports", tc.contentType, "", tc.body)
			if r.Err != nil || r.Status != tc.status {
				t.Errorf("structurally invalid import was accepted: status=%d expected=%d err=%v", r.Status, tc.status, r.Err)
			}
			if strings.Contains(string(r.Body), "audit-private-password-marker") {
				t.Error("structural parser error echoed attacker-supplied secret material")
			}
			if auditImportTableCount(t, e, "proxy_resources") != 0 || auditImportTableCount(t, e, "import_batches") != 0 {
				t.Fatal("structural rejection left inventory or a batch record behind")
			}
		})
	}
}

func TestAuditImportErrorsCannotEchoPasswordsAndFieldBoundsMatchFormats(t *testing.T) {
	cases := []struct {
		name, contentType, body, secret string
	}{
		{"invalid_text_escape", "text/plain", "http://audit-user:audit-password-%ZZ@8.8.8.8:8080\n", "audit-password"},
		{"invalid_unterminated_userinfo", "text/plain", "http://audit-user:audit-password-%ZZ8.8.8.8:8080\n", "audit-password"},
		{"invalid_bare_unterminated_userinfo", "text/plain", "audit-user:audit-password-%ZZ8.8.8.8:8080\n", "audit-password"},
		{"oversize_text_password", "text/plain", "http://audit-user:" + strings.Repeat("p", 257) + "@8.8.8.8:8080\n", strings.Repeat("p", 100)},
		{"oversize_json_password", "application/json", `{"items":[{"connection":{"transport":"http","host":"8.8.8.8","port":8080,"username":"audit-user","password":"` + strings.Repeat("p", 257) + `"}}]}`, strings.Repeat("p", 100)},
		{"oversize_text_host", "text/plain", "http://audit-user:audit-password@" + strings.Repeat("a", 254) + ":8080\n", "audit-password"},
		{"json_host_control", "application/json", `{"items":[{"connection":{"transport":"http","host":"8.8.8.8\u000aevil","port":8080,"username":"audit-user","password":"audit-password"}}]}`, "audit-password"},
		{"json_host_contains_uri", "application/json", `{"items":[{"connection":{"transport":"http","host":"http://8.8.8.8","port":8080,"username":"audit-user","password":"audit-password"}}]}`, "audit-password"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e, srv, _, token := auditImportEnv(t)
			r := auditImportCall(srv, http.MethodPost, token, "/api/v1/sources/"+auditImportSource+"/imports", tc.contentType, "", tc.body)
			out := auditImportResult(t, r)
			if out.Added != 0 || out.Rejected != 1 || len(out.Errors) != 1 || auditImportTableCount(t, e, "proxy_resources") != 0 {
				t.Errorf("invalid or overlong credential bypassed the common per-item bounds: added=%d rejected=%d errors=%d", out.Added, out.Rejected, len(out.Errors))
			}
			if strings.Contains(string(r.Body), tc.secret) || strings.Contains(string(r.Body), "audit-user") {
				t.Error("returned import error exposed URI authentication material")
			}
			var stored string
			if err := e.store.DB().QueryRow(`SELECT errors_json FROM import_batches WHERE import_id=?`, out.ImportID).Scan(&stored); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(stored, tc.secret) || strings.Contains(stored, "audit-user") {
				t.Error("durable import error stored URI authentication material")
			}
		})
	}
}

func auditImportPauseAfterSourceRead(t *testing.T, e *env) (<-chan struct{}, func()) {
	t.Helper()
	arrived, release := make(chan struct{}), make(chan struct{})
	var armed atomic.Bool
	armed.Store(true)
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	e.ingest.Now = func() time.Time {
		if armed.CompareAndSwap(true, false) {
			close(arrived)
			<-release
		}
		return time.Now().UTC()
	}
	return arrived, unblock
}

func TestAuditImportCommitRechecksChangedSourceConfiguration(t *testing.T) {
	for _, replay := range []bool{false, true} {
		for _, change := range []string{"disabled", "updated", "deleted_recreated"} {
			t.Run(fmt.Sprintf("%s/replay_%t", change, replay), func(t *testing.T) {
				e, srv, _, token := auditImportEnv(t)
				path := "/api/v1/sources/" + auditImportSource + "/imports"
				const body = "http://8.8.8.8:8080\n"
				if replay {
					_ = auditImportResult(t, auditImportCall(srv, http.MethodPost, token, path, "text/plain", "audit-source-final", body))
				}
				src, err := e.store.GetSource(context.Background(), auditImportSource)
				if err != nil {
					t.Fatal(err)
				}
				arrived, unblock := auditImportPauseAfterSourceRead(t, e)
				done := make(chan auditImportReply, 1)
				go func() {
					done <- auditImportCall(srv, http.MethodPost, token, path, "text/plain", "audit-source-final", body)
				}()
				select {
				case <-arrived:
				case <-time.After(4 * time.Second):
					t.Fatal("import did not reach the post-authorization/source-snapshot boundary")
				}
				switch change {
				case "disabled":
					src.Enabled = false
					err = e.store.UpdateSource(context.Background(), src, src.ConfigVersion, time.Now())
				case "updated":
					cfg := application.SourceConfig{DefaultTransport: domain.TransportHTTP, MaxPoolLeases: 17}
					data, marshalErr := json.Marshal(cfg)
					if marshalErr != nil {
						t.Fatal(marshalErr)
					}
					src.ConfigJSON = string(data)
					err = e.store.UpdateSource(context.Background(), src, src.ConfigVersion, time.Now())
				case "deleted_recreated":
					_, err = e.store.DeleteSource(context.Background(), src.ID, time.Now())
					if err == nil {
						err = e.store.CreateSource(context.Background(), src, time.Now())
					}
				}
				if err != nil {
					t.Fatal(err)
				}
				before := make(map[string]int)
				for _, table := range []string{"proxy_resources", "source_offers", "import_batches", "import_idempotency"} {
					before[table] = auditImportTableCount(t, e, table)
				}
				unblock()
				select {
				case r := <-done:
					if r.Err != nil || r.Status < 400 || r.Status >= 500 {
						t.Errorf("stale source snapshot was applied or misreported as storage failure: change=%s status=%d err=%v", change, r.Status, r.Err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("guarded import did not finish")
				}
				for table, expected := range before {
					if auditImportTableCount(t, e, table) != expected {
						t.Fatalf("stale source apply/replay changed persistent state: table=%s", table)
					}
				}
			})
		}
	}
}

func TestAuditImportCommitRechecksRevokedUploadKeyIncludingReplay(t *testing.T) {
	for _, revoke := range []string{"disabled", "deleted", "source_unbound"} {
		for _, replay := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/replay_%t", revoke, replay), func(t *testing.T) {
				e, srv, key, token := auditImportEnv(t)
				path := "/api/v1/sources/" + auditImportSource + "/imports"
				const body = "http://8.8.8.8:8080\n"
				if replay {
					_ = auditImportResult(t, auditImportCall(srv, http.MethodPost, token, path, "text/plain", "audit-final-auth", body))
				}
				before := auditImportTableCount(t, e, "import_batches")
				arrived, unblock := auditImportPauseAfterSourceRead(t, e)
				done := make(chan auditImportReply, 1)
				go func() {
					done <- auditImportCall(srv, http.MethodPost, token, path, "text/plain", "audit-final-auth", body)
				}()
				select {
				case <-arrived:
				case <-time.After(4 * time.Second):
					t.Fatal("import did not reach the boundary after the API's late credential recheck")
				}
				var err error
				switch revoke {
				case "disabled":
					disabled := false
					err = e.store.UpdateUploadKey(context.Background(), key.ID, nil, &disabled, time.Now())
				case "deleted":
					err = e.store.DeleteUploadKey(context.Background(), key.ID)
				case "source_unbound":
					_, err = e.store.DB().Exec(`DELETE FROM upload_key_sources WHERE key_id=? AND source_id=?`, key.ID, auditImportSource)
				}
				if err != nil {
					t.Fatal(err)
				}
				unblock()
				select {
				case r := <-done:
					if r.Err != nil || (r.Status != http.StatusUnauthorized && r.Status != http.StatusForbidden) {
						t.Errorf("revoked upload identity still applied or replayed a batch: status=%d err=%v", r.Status, r.Err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("revoked import did not finish")
				}
				if auditImportTableCount(t, e, "import_batches") != before || (!replay && auditImportTableCount(t, e, "proxy_resources") != 0) {
					t.Fatal("a key revoked after parsing still mutated persistent inventory or batches")
				}
			})
		}
	}
}

func TestAuditImportInventoryAndIdempotencyCommitOrRollbackTogether(t *testing.T) {
	e, srv, _, token := auditImportEnv(t)
	path := "/api/v1/sources/" + auditImportSource + "/imports"
	_ = auditImportResult(t, auditImportCall(srv, http.MethodPost, token, path, "application/json", "", `{"items":[]}`))
	if _, err := e.store.DB().Exec(`CREATE TRIGGER audit_abort_import_batch BEFORE INSERT ON import_batches
		BEGIN SELECT RAISE(ABORT, 'audit simulated batch storage failure'); END`); err != nil {
		t.Fatal(err)
	}
	r := auditImportCall(srv, http.MethodPost, token, path, "text/plain", "audit-atomic", "http://8.8.8.8:8080\n")
	if r.Err != nil || r.Status < 500 {
		t.Errorf("batch storage failure did not become a storage error: status=%d err=%v", r.Status, r.Err)
	}
	if auditImportTableCount(t, e, "proxy_resources") != 0 || auditImportTableCount(t, e, "source_offers") != 0 ||
		auditImportTableCount(t, e, "import_idempotency") != 0 || auditImportTableCount(t, e, "import_batches") != 1 {
		t.Fatal("batch failure committed inventory, an idempotency marker, or a partial report")
	}
	if _, err := e.store.DB().Exec(`DROP TRIGGER audit_abort_import_batch`); err != nil {
		t.Fatal(err)
	}
	first := auditImportResult(t, auditImportCall(srv, http.MethodPost, token, path, "text/plain", "audit-atomic", "http://8.8.8.8:8080\n"))
	replay := auditImportResult(t, auditImportCall(srv, http.MethodPost, token, path, "text/plain", "audit-atomic", "http://8.8.8.8:8080\n"))
	if first.ImportID != replay.ImportID || first.Added != 1 || auditImportTableCount(t, e, "proxy_resources") != 1 ||
		auditImportTableCount(t, e, "import_idempotency") != 1 {
		t.Fatal("retry after a rolled-back failure could not apply exactly once")
	}
}

func TestAuditImportCorruptStoredResultCannotReapplyCommittedBatch(t *testing.T) {
	e, srv, key, token := auditImportEnv(t)
	path := "/api/v1/sources/" + auditImportSource + "/imports"
	const body = "http://8.8.8.8:8080\n"
	first := auditImportResult(t, auditImportCall(srv, http.MethodPost, token, path, "text/plain", "audit-corrupt-result", body))
	if _, err := e.store.DB().Exec(`UPDATE import_batches SET errors_json=? WHERE import_id=?`,
		"audit simulated corrupt JSON result", first.ImportID); err != nil {
		t.Fatal(err)
	}
	r := auditImportCall(srv, http.MethodPost, token, path, "text/plain", "audit-corrupt-result", body)
	if r.Err != nil || r.Status < 500 {
		t.Errorf("stored result read failure was treated as an expired idempotency window: status=%d err=%v", r.Status, r.Err)
	}
	var importID string
	if err := e.store.DB().QueryRow(`SELECT import_id FROM import_idempotency WHERE uploader_id=? AND source_id=? AND idempotency_key=?`,
		key.ID, auditImportSource, "audit-corrupt-result").Scan(&importID); err != nil {
		t.Fatal(err)
	}
	if importID != first.ImportID || auditImportTableCount(t, e, "import_batches") != 1 ||
		auditImportTableCount(t, e, "proxy_resources") != 1 || auditImportTableCount(t, e, "source_offers") != 1 {
		t.Fatal("stored result corruption changed the committed batch, marker, or inventory")
	}
}

func TestAuditImportConcurrentRetriesApplyOnlyOnce(t *testing.T) {
	e, srv, _, token := auditImportEnv(t)
	const workers = 8
	start := make(chan struct{})
	done := make(chan auditImportReply, workers)
	for i := 0; i < workers; i++ {
		go func() {
			<-start
			var r auditImportReply
			for attempt := 0; attempt < 3; attempt++ {
				r = auditImportCall(srv, http.MethodPost, token, "/api/v1/sources/"+auditImportSource+"/imports", "text/plain", "audit-concurrent", "http://8.8.8.8:8080\n")
				if r.Err != nil || r.Status != http.StatusServiceUnavailable {
					break
				}
				time.Sleep(30 * time.Millisecond)
			}
			done <- r
		}()
	}
	close(start)
	var importID string
	firstApplies := 0
	for i := 0; i < workers; i++ {
		select {
		case r := <-done:
			result := auditImportResult(t, r)
			if importID == "" {
				importID = result.ImportID
			}
			if r.Status == http.StatusCreated {
				firstApplies++
			}
			if result.ImportID != importID || result.Added != 1 || result.Rejected != 0 {
				t.Fatal("concurrent idempotent retries returned different applied reports")
			}
		case <-time.After(10 * time.Second):
			t.Fatal("bounded concurrent import retries did not finish")
		}
	}
	if firstApplies != 1 || auditImportTableCount(t, e, "proxy_resources") != 1 ||
		auditImportTableCount(t, e, "source_offers") != 1 || auditImportTableCount(t, e, "import_batches") != 1 ||
		auditImportTableCount(t, e, "import_idempotency") != 1 {
		t.Fatal("concurrent retry committed more than one inventory/batch/idempotency application")
	}
}
