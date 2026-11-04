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

func auditRequest(t *testing.T, srv *httptest.Server, token, method, path, body string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, data
}

func auditProject(t *testing.T, srv *httptest.Server, master, name, extra string) domain.Project {
	t.Helper()
	body := fmt.Sprintf(`{"name":%q`, name) + extra + "}"
	status, data := auditPost(t, srv, master, "/api/v1/admin/projects", body)
	var project domain.Project
	if status != http.StatusCreated || json.Unmarshal(data, &project) != nil || project.ID == "" {
		t.Fatalf("create project: status=%d body=%s", status, data)
	}
	return project
}

func auditProjectKey(t *testing.T, srv *httptest.Server, master, projectID, name string) (domain.APIKey, string) {
	t.Helper()
	status, data := auditPost(t, srv, master, "/api/v1/admin/projects/"+projectID+"/keys",
		fmt.Sprintf(`{"name":%q}`, name))
	var created struct {
		Key   domain.APIKey `json:"key"`
		Token string        `json:"token"`
	}
	if status != http.StatusCreated || json.Unmarshal(data, &created) != nil || created.Token == "" || created.Key.ProjectID != projectID {
		t.Fatalf("create project key: status=%d body=%s", status, data)
	}
	return created.Key, created.Token
}

func TestAuditMultipleKeysAndKeyDeletionShareFixedProjectCooldown(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	e.importProxy(t, "audit-shared-project", startFakeProxy(t).URL)
	srv := auditServer(t, e)
	project := auditProject(t, srv, e.cfg.APIToken, "shared-project", `,"cooldown_ms":0`)
	keyA, tokenA := auditProjectKey(t, srv, e.cfg.APIToken, project.ID, "audit-key-a")
	_, tokenB := auditProjectKey(t, srv, e.cfg.APIToken, project.ID, "audit-key-b")
	status, data := auditPost(t, srv, tokenA, "/api/v1/proxies/acquire", `{"count":1,"cd":600}`)
	var first application.AcquireResult
	if status != http.StatusOK || json.Unmarshal(data, &first) != nil || first.Delivered != 1 {
		t.Fatalf("initial project delivery: status=%d body=%s", status, data)
	}
	if _, err := e.leases.Release(context.Background(), first.Items[0].LeaseID, keyA.ID, false); err != nil {
		t.Fatal(err)
	}
	assertBlocked := func(token string) {
		t.Helper()
		status, data := auditPost(t, srv, token, "/api/v1/proxies/acquire", `{"count":1,"cd":0}`)
		var result application.AcquireResult
		if status != http.StatusOK || json.Unmarshal(data, &result) != nil || result.Delivered != 0 {
			t.Fatalf("another key or cd=0 bypassed project cooldown: status=%d body=%s", status, data)
		}
	}
	assertBlocked(tokenB)
	if err := e.keys.Delete(context.Background(), keyA.ID); err != nil {
		t.Fatal(err)
	}
	_, tokenC := auditProjectKey(t, srv, e.cfg.APIToken, project.ID, "audit-key-c")
	assertBlocked(tokenB)
	assertBlocked(tokenC)
	other := auditProject(t, srv, e.cfg.APIToken, "independent-project", `,"cooldown_ms":0`)
	_, otherToken := auditProjectKey(t, srv, e.cfg.APIToken, other.ID, "audit-other-key")
	status, data = auditPost(t, srv, otherToken, "/api/v1/proxies/acquire", `{"count":1,"cd":0}`)
	var independent application.AcquireResult
	if status != http.StatusOK || json.Unmarshal(data, &independent) != nil || independent.Delivered != 1 {
		t.Fatalf("independent project was incorrectly blocked: status=%d body=%s", status, data)
	}
}

func TestAuditProjectEmptyAllowlistAndExplicitNullAreDistinct(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	e.importProxy(t, "audit-allowed", startFakeProxy(t).URL)
	srv := auditServer(t, e)
	project := auditProject(t, srv, e.cfg.APIToken, "scope-project", `,"allowed_source_ids":[]`)
	_, token := auditProjectKey(t, srv, e.cfg.APIToken, project.ID, "audit-scope-key")
	status, data := auditPost(t, srv, token, "/api/v1/proxies/acquire", `{"count":1}`)
	if status != http.StatusForbidden {
		var result application.AcquireResult
		if status != http.StatusOK || json.Unmarshal(data, &result) != nil || result.Delivered != 0 {
			t.Fatalf("empty allowlist became unrestricted: status=%d body=%s", status, data)
		}
	}
	status, data = auditRequest(t, srv, e.cfg.APIToken, http.MethodPut,
		"/api/v1/admin/projects/"+project.ID+"/sources",
		fmt.Sprintf(`{"config_version":%d,"source_ids":null}`, project.ConfigVersion))
	if status != http.StatusOK {
		t.Fatalf("explicit null must restore unrestricted scope: status=%d body=%s", status, data)
	}
	status, data = auditPost(t, srv, token, "/api/v1/proxies/acquire", `{"count":1}`)
	var result application.AcquireResult
	if status != http.StatusOK || json.Unmarshal(data, &result) != nil || result.Delivered != 1 {
		t.Fatalf("restored source scope did not apply: status=%d body=%s", status, data)
	}
}

func TestAuditProjectZeroCooldownDoesNotReviveKeyLegacyDefault(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	e.importProxy(t, "audit-zero-project-default", startFakeProxy(t).URL)
	srv := auditServer(t, e)
	project := auditProject(t, srv, e.cfg.APIToken, "zero-project-default", `,"cooldown_ms":0`)
	// A retained key field models data that remains after a project policy
	// change; retaining it must not restore the superseded business default.
	_, token, err := e.keys.CreateForProject(context.Background(), project.ID, "retained-key-default", 60000)
	if err != nil {
		t.Fatal(err)
	}
	status, data := auditPost(t, srv, token, "/api/v1/proxies/acquire", `{"count":1}`)
	var result application.AcquireResult
	if status != http.StatusOK || json.Unmarshal(data, &result) != nil || result.Delivered != 1 {
		t.Fatalf("new project delivery: status=%d body=%s", status, data)
	}
	if result.CooldownSeconds != 0 {
		t.Errorf("project zero default resurrected a retained key default: %s", data)
	}
	fp := e.acquire.FingerprintOf(result.Items[0].Conn)
	blocked, err := e.store.ProjectCooldownBlocked(context.Background(), project.ID, time.Now(), []string{fp})
	if err != nil || len(blocked) != 0 {
		t.Fatalf("new delivery opened a window against project policy: windows=%+v err=%v", blocked, err)
	}
}

func TestAuditProjectTLSDefaultIsNotSilentlyDowngraded(t *testing.T) {
	j := startJudge(t)
	e := newEnv(t, j, nil)
	e.importProxy(t, "audit-project-tls", startFakeProxy(t).URL)
	srv := auditServer(t, e)
	// This local target is deliberately closed. Basic mode could pass via the
	// judge, but inheriting the project's TLS default must return a shortfall.
	project := auditProject(t, srv, e.cfg.APIToken, "tls-default-project",
		`,"validation":{"mode":"tls","target":"127.0.0.1:1"}`)
	_, token := auditProjectKey(t, srv, e.cfg.APIToken, project.ID, "audit-default-tls-key")
	status, data := auditPost(t, srv, token, "/api/v1/proxies/acquire", `{"count":1,"timeout":1}`)
	var result application.AcquireResult
	if status != http.StatusOK || json.Unmarshal(data, &result) != nil || result.Delivered != 0 {
		t.Fatalf("project TLS default was replaced with basic: status=%d body=%s", status, data)
	}
}

func TestAuditJobIsReadableOnlyByItsProject(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	srv := auditServer(t, e)
	a := auditProject(t, srv, e.cfg.APIToken, "job-owner-a", "")
	b := auditProject(t, srv, e.cfg.APIToken, "job-owner-b", "")
	_, tokenA := auditProjectKey(t, srv, e.cfg.APIToken, a.ID, "audit-job-key-a")
	_, tokenB := auditProjectKey(t, srv, e.cfg.APIToken, b.ID, "audit-job-key-b")
	jobID := "job_audit_owned"
	if err := e.store.CreateJob(context.Background(), domain.Job{
		ID: jobID, ProjectID: a.ID, Kind: "prewarm", ParamsJSON: "{}", Target: 1,
	}, time.Now()); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		token string
		want  int
	}{{tokenA, http.StatusOK}, {tokenB, http.StatusNotFound}} {
		status, data := auditRequest(t, srv, tc.token, http.MethodGet, "/api/v1/jobs/"+jobID, "")
		if status != tc.want {
			t.Fatalf("job project boundary: status=%d want=%d body=%s", status, tc.want, data)
		}
	}
}
