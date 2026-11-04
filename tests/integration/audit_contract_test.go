package integration

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"proxypool/internal/api"
	"proxypool/internal/application"
	"proxypool/internal/config"
	"proxypool/internal/domain"
)

func auditServer(t *testing.T, e *env) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(api.NewServer(e.cfg, testLogger(),
		e.acquire, e.ingest, e.leases, e.store, e.nightly, e.prewarm, e.keys).Handler())
	t.Cleanup(srv.Close)
	return srv
}

func auditPost(t *testing.T, srv *httptest.Server, token, path, body string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
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

func TestAuditAcquireRejectsMalformedContract(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	srv := auditServer(t, e)
	cases := []string{
		`null`,
		`{"count":null}`,
		`{"count":0}`,
		`{"count":-1}`,
		fmt.Sprintf(`{"count":%d}`, e.cfg.MaxCount+1),
		`{"count":1,"timeout":0}`,
		`{"count":1,"timeout":-1}`,
		`{"count":1,"timeout":3600000}`,
		`{"count":1,"cd":-1}`,
		`{"count":1,"country":["CHINA"]}`,
		`{"count":1,"country":[""]}`,
		`{"count":1,"country":[null]}`,
		`{"count":1,"protocols":["socks4"]}`,
		`{"count":1,"validation":{"mode":"tls"}}`,
		`{"count":1,"validation":{"mode":"tls","target":"https://example.com/path"}}`,
		`{"count":1,"mode":"sticky"}`,
		`{"count":2,"mode":"sticky","session_id":"audit-session"}`,
		`{"count":1,"project_id":"another-project"}`,
		`{"count":1} {"count":2}`,
	}
	for _, body := range cases {
		t.Run(body, func(t *testing.T) {
			status, data := auditPost(t, srv, e.cfg.APIToken, "/api/v1/proxies/acquire", body)
			if status != http.StatusBadRequest {
				t.Fatalf("invalid contract accepted: status=%d body=%s", status, data)
			}
		})
	}
}

func TestAuditEmptySupplyIsExplainedBusinessResult(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	srv := auditServer(t, e)
	status, data := auditPost(t, srv, e.cfg.APIToken, "/api/v1/proxies/acquire", `{"count":2}`)
	var result application.AcquireResult
	if status != http.StatusOK || json.Unmarshal(data, &result) != nil {
		t.Fatalf("empty supply must be HTTP 200: status=%d body=%s", status, data)
	}
	if result.Requested != 2 || result.Delivered != 0 || result.Missing != 2 || result.Items == nil {
		t.Fatalf("incoherent empty result: %s", data)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	var reason string
	_ = json.Unmarshal(fields["reason"], &reason)
	if reason == "" {
		_ = json.Unmarshal(fields["shortfall_reason"], &reason)
	}
	if reason == "" {
		t.Fatalf("empty supply has no explanation: %s", data)
	}
}

func TestAuditDeadlineKeepsAlreadyValidatedPartial(t *testing.T) {
	j := startJudge(t)
	e := newEnv(t, j, func(c *config.Config) {
		c.ParallelPerRequest = 2
		c.ConnectTimeoutMS = 2500
		c.ProbeTotalTimeoutMS = 3000
	})
	good := startFakeProxy(t)
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
		http.Error(w, "slow upstream", http.StatusBadGateway)
	}))
	t.Cleanup(slow.Close)
	if err := e.ingest.EnsureSource(context.Background(), "audit-local", application.SourceConfig{
		DefaultTransport: domain.TransportHTTP, MaxPoolLeases: 1,
	}, true); err != nil {
		t.Fatal(err)
	}
	if _, derr := e.ingest.ImportLines(context.Background(), "audit-local", good.URL+"\n"+slow.URL+"\n"); derr != nil {
		t.Fatal(derr)
	}
	srv := auditServer(t, e)
	started := time.Now()
	status, data := auditPost(t, srv, e.cfg.APIToken, "/api/v1/proxies/acquire", `{"count":2,"timeout":1}`)
	elapsed := time.Since(started)
	var result application.AcquireResult
	if status != http.StatusOK || json.Unmarshal(data, &result) != nil ||
		result.Delivered != 1 || result.Missing != 1 || !result.Partial || len(result.Items) != 1 {
		t.Fatalf("slow probe discarded a validated partial: status=%d body=%s", status, data)
	}
	if elapsed > 1800*time.Millisecond {
		t.Fatalf("whole-request timeout was not bounded: %s", elapsed)
	}
}

func TestAuditExplicitSourceScopeNeverFallsBackOutsideIt(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	e.importProxy(t, "unrequested", startFakeProxy(t).URL)
	srv := auditServer(t, e)
	status, data := auditPost(t, srv, e.cfg.APIToken, "/api/v1/proxies/acquire",
		`{"count":1,"source_ids":["does-not-exist"]}`)
	if status == http.StatusBadRequest || status == http.StatusForbidden {
		return
	}
	var result application.AcquireResult
	if status != http.StatusOK || json.Unmarshal(data, &result) != nil || result.Delivered != 0 {
		t.Fatalf("explicit scope leaked an outside candidate: status=%d body=%s", status, data)
	}
}

func TestAuditProjectCredentialsAreNotAcceptedInURL(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	srv := auditServer(t, e)
	status, data := auditPost(t, srv, "", "/api/v1/proxies/acquire?key="+e.cfg.APIToken, `{"count":1}`)
	if status != http.StatusUnauthorized {
		t.Fatalf("URL credential accepted: status=%d body=%s", status, data)
	}
}

func TestAuditExplicitProjectCredentialTakesPrecedenceOverAdminCookie(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	k := newKeyAPI(t, e)
	t.Cleanup(k.srv.Close)
	token := k.createKey(t, "audit-explicit-auth", 0)
	resp, err := http.Post(k.srv.URL+"/admin/login", "application/json",
		strings.NewReader(fmt.Sprintf(`{"token":%q}`, e.cfg.APIToken)))
	if err != nil {
		t.Fatal(err)
	}
	cookies := resp.Cookies()
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || len(cookies) == 0 {
		t.Fatal("fixture could not establish an admin session")
	}
	cases := []struct {
		name, method, path, bearer string
		status                     int
	}{
		{"invalid_header", http.MethodPost, "/api/v1/proxies/acquire", "pk_invalid_explicit_credential", http.StatusUnauthorized},
		{"project_header", http.MethodGet, "/api/v1/admin/keys", token, http.StatusForbidden},
		{"cookie_only", http.MethodGet, "/api/v1/admin/keys", "", http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(tc.method, k.srv.URL+tc.path, strings.NewReader(`{"count":1}`))
			if err != nil {
				t.Fatal(err)
			}
			if tc.bearer != "" {
				req.Header.Set("Authorization", "Bearer "+tc.bearer)
			}
			for _, cookie := range cookies {
				req.AddCookie(cookie)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			data, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != tc.status {
				t.Fatalf("cookie changed explicit request identity: status=%d want=%d body=%s", resp.StatusCode, tc.status, data)
			}
		})
	}
}

func TestAuditEmptyAuthorizationNeverFallsBackToAdminCookie(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	srv := auditServer(t, e)
	login, err := http.Post(srv.URL+"/admin/login", "application/json",
		strings.NewReader(fmt.Sprintf(`{"token":%q}`, e.cfg.APIToken)))
	if err != nil {
		t.Fatal(err)
	}
	cookies := login.Cookies()
	_ = login.Body.Close()
	if login.StatusCode != http.StatusOK || len(cookies) == 0 {
		t.Fatal("fixture could not establish an admin session")
	}
	for _, header := range []string{"", "   "} {
		req, err := http.NewRequest(http.MethodGet, srv.URL+"/api/v1/admin/projects", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", header)
		for _, cookie := range cookies {
			req.AddCookie(cookie)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		data, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("explicit empty Authorization rode the admin cookie: status=%d body=%s", resp.StatusCode, data)
		}
	}
}

func TestAuditDeliveryPersistenceFailuresAreNotSupplyShortfalls(t *testing.T) {
	for _, table := range []string{"leases", "project_proxy_cd"} {
		t.Run(table, func(t *testing.T) {
			e := newEnv(t, startJudge(t), nil)
			e.importProxy(t, "audit-store-failure", startFakeProxy(t).URL)
			srv := auditServer(t, e)
			if _, err := e.store.DB().Exec(`CREATE TRIGGER audit_reject_delivery BEFORE INSERT ON ` + table +
				` BEGIN SELECT RAISE(ABORT, 'audit simulated delivery failure'); END`); err != nil {
				t.Fatal(err)
			}
			status, data := auditPost(t, srv, e.cfg.APIToken, "/api/v1/proxies/acquire", `{"count":1,"cd":600}`)
			if status < 500 {
				t.Errorf("critical persistence failure became a supply shortfall: status=%d body=%s", status, data)
			}
			var active, cooldowns int
			if err := e.store.DB().QueryRow(`SELECT COUNT(*) FROM leases WHERE state='ACTIVE'`).Scan(&active); err != nil {
				t.Fatal(err)
			}
			if err := e.store.DB().QueryRow(`SELECT COUNT(*) FROM project_proxy_cd`).Scan(&cooldowns); err != nil {
				t.Fatal(err)
			}
			if active != 0 || cooldowns != 0 {
				t.Errorf("failed delivery left lease/CD state: active=%d cooldowns=%d", active, cooldowns)
			}
		})
	}
}

func TestAuditCandidateStorageFailuresAreNotSupplyShortfalls(t *testing.T) {
	for _, phase := range []string{"selection", "reservation"} {
		t.Run(phase, func(t *testing.T) {
			e := newEnv(t, startJudge(t), nil)
			e.importProxy(t, "audit-candidate-store-failure", startFakeProxy(t).URL)
			srv := auditServer(t, e)
			statement := `ALTER TABLE source_offers RENAME TO audit_hidden_source_offers`
			if phase == "reservation" {
				statement = `CREATE TRIGGER audit_reject_reservation BEFORE INSERT ON reservations
					BEGIN SELECT RAISE(ABORT, 'audit simulated reservation failure'); END`
			}
			if _, err := e.store.DB().Exec(statement); err != nil {
				t.Fatal(err)
			}
			status, data := auditPost(t, srv, e.cfg.APIToken, "/api/v1/proxies/acquire", `{"count":1,"cd":600}`)
			if status < 500 {
				t.Errorf("critical %s storage failure became a supply shortfall: status=%d body=%s", phase, status, data)
			}
			var leases, cooldowns int
			if err := e.store.DB().QueryRow(`SELECT COUNT(*) FROM leases`).Scan(&leases); err != nil {
				t.Fatal(err)
			}
			if err := e.store.DB().QueryRow(`SELECT COUNT(*) FROM project_proxy_cd`).Scan(&cooldowns); err != nil {
				t.Fatal(err)
			}
			if leases != 0 || cooldowns != 0 {
				t.Errorf("failed %s left delivery state: leases=%d cooldowns=%d", phase, leases, cooldowns)
			}
		})
	}
}

func TestAuditTLSValidationSendsNoBusinessBody(t *testing.T) {
	j := startJudge(t)
	e := newEnv(t, j, nil)
	e.importProxy(t, "audit-local", startFakeProxy(t).URL)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tlsListener := tls.NewListener(listener, &tls.Config{
		Certificates: j.srv.TLS.Certificates, MinVersion: tls.VersionTLS12,
	})
	t.Cleanup(func() { _ = tlsListener.Close() })
	var applicationBytes atomic.Int64
	completed := make(chan error, 1)
	go func() {
		conn, err := tlsListener.Accept()
		if err != nil {
			completed <- err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		if err := conn.(*tls.Conn).Handshake(); err != nil {
			completed <- err
			return
		}
		buf := make([]byte, 2048)
		n, _ := conn.Read(buf)
		applicationBytes.Store(int64(n))
		completed <- nil
	}()
	srv := auditServer(t, e)
	status, data := auditPost(t, srv, e.cfg.APIToken, "/api/v1/proxies/acquire",
		fmt.Sprintf(`{"count":1,"validation":{"mode":"tls","target":%q}}`, listener.Addr().String()))
	var result application.AcquireResult
	if status != http.StatusOK || json.Unmarshal(data, &result) != nil || result.Delivered != 1 {
		t.Fatalf("target TLS validation failed: status=%d body=%s", status, data)
	}
	if result.Items[0].Validation.Mode != "tls" || result.Items[0].Validation.Target != listener.Addr().String() {
		t.Fatalf("target TLS silently degraded to another validation scope: %s", data)
	}
	select {
	case err := <-completed:
		if err != nil {
			t.Fatalf("target handshake: %v", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("validation did not close its target TLS connection")
	}
	if applicationBytes.Load() != 0 {
		t.Fatalf("TLS-only validation sent %d application bytes", applicationBytes.Load())
	}
}
