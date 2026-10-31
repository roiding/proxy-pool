package integration

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"proxypool/internal/api"
	"proxypool/internal/application"
	"proxypool/internal/config"
	"proxypool/internal/domain"
	"proxypool/internal/maintenance"
	"proxypool/internal/storage/sqlite"
)

// judge is a controlled HTTPS judge: /judge echoes the nonce and a fixed exit
// IP. healthy=false simulates a judge outage without changing URLs.
type judge struct {
	srv     *httptest.Server
	healthy atomic.Bool
}

func startJudge(t *testing.T) *judge {
	t.Helper()
	j := &judge{}
	j.healthy.Store(true)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/judge", func(w http.ResponseWriter, r *http.Request) {
		if !j.healthy.Load() {
			http.Error(w, "unhealthy", http.StatusServiceUnavailable)
			return
		}
		nonce := r.URL.Query().Get("nonce")
		if nonce == "" {
			http.Error(w, "missing nonce", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"nonce": nonce, "exit_ip": "203.0.113.7"})
	})
	j.srv = httptest.NewTLSServer(mux)
	t.Cleanup(j.srv.Close)
	return j
}

func (j *judge) url() string { return j.srv.URL + "/v1/judge" }

func (j *judge) certPool() *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(j.srv.Certificate())
	return pool
}

func (j *judge) certPEM() string {
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: j.srv.Certificate().Raw}))
}

// fakeProxy implements the CONNECT tunnel the probe expects.
func startFakeProxy(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			// Absolute-form forward proxy requests (plain-HTTP targets).
			if r.URL.Host == "" {
				http.Error(w, "absolute URI required", http.StatusBadRequest)
				return
			}
			out, err := http.NewRequest(r.Method, r.URL.String(), r.Body)
			if err != nil {
				http.Error(w, "bad upstream request", http.StatusBadRequest)
				return
			}
			out.Header = r.Header.Clone()
			out.Header.Del("Proxy-Authorization")
			upstream, err := http.DefaultTransport.RoundTrip(out)
			if err != nil {
				http.Error(w, "upstream unreachable", http.StatusBadGateway)
				return
			}
			defer upstream.Body.Close()
			for k, vv := range upstream.Header {
				for _, v := range vv {
					w.Header().Add(k, v)
				}
			}
			w.WriteHeader(upstream.StatusCode)
			_, _ = io.Copy(w, upstream.Body)
			return
		}
		dst, err := net.DialTimeout("tcp", r.Host, 2*time.Second)
		if err != nil {
			http.Error(w, "upstream unreachable", http.StatusBadGateway)
			return
		}
		hj, ok := w.(http.Hijacker)
		if !ok {
			dst.Close()
			http.Error(w, "hijack unsupported", http.StatusInternalServerError)
			return
		}
		conn, buf, err := hj.Hijack()
		if err != nil {
			dst.Close()
			return
		}
		defer conn.Close()
		defer dst.Close()
		if _, err := conn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n")); err != nil {
			return
		}
		done := make(chan struct{}, 2)
		go func() { _, _ = io.Copy(dst, buf); done <- struct{}{} }()
		go func() { _, _ = io.Copy(conn, dst); done <- struct{}{} }()
		<-done
	}))
	t.Cleanup(srv.Close)
	return srv
}

type env struct {
	cfg     config.Config
	store   *sqlite.Store
	ingest  *application.IngestService
	leases  *application.LeaseService
	acquire *application.AcquireService
	nightly *application.NightlyValidator
}

func newEnv(t *testing.T, j *judge, mutate func(*config.Config)) *env {
	t.Helper()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DatabasePath = filepath.Join(dir, "test.db")
	cfg.LockPath = filepath.Join(dir, "test.lock")
	cfg.APIToken = "test-token-0123456789abcdef"
	cfg.IdentitySecret = "test-secret-0123456789abcdef"
	cfg.JudgeURL = j.url()
	cfg.JudgeCAPem = j.certPEM()
	cfg.AllowPrivateAddresses = true
	cfg.CooldownBaseMS = 60000
	if mutate != nil {
		mutate(&cfg)
	}
	return newEnvWithConfig(t, cfg, j.certPool())
}

func newEnvWithConfig(t *testing.T, cfg config.Config, rootCAs *x509.CertPool) *env {
	t.Helper()
	db, err := sqlite.Open(cfg.DatabasePath, cfg.SQLiteCacheKiB)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	store := sqlite.New(db)
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return &env{
		cfg:    cfg,
		store:  store,
		ingest: application.NewIngestService(store, cfg.IdentitySecret, cfg.AllowPrivateAddresses),
		leases: application.NewLeaseService(store, application.CooldownConfig{Base: cfg.CooldownBase(), Max: cfg.CooldownMax(), Jitter: cfg.CooldownJitter}),
		acquire: application.NewAcquireService(store, cfg, cfg.JudgeURL, rootCAs,
			make(chan struct{}, cfg.ProbeSlots)),
		nightly: application.NewNightlyValidator(store, cfg, cfg.JudgeURL, rootCAs,
			application.CooldownConfig{Base: cfg.CooldownBase(), Max: cfg.CooldownMax(), Jitter: cfg.CooldownJitter},
			make(chan struct{}, cfg.ProbeSlots)),
	}
}

func (e *env) importProxy(t *testing.T, sourceID, line string) {
	t.Helper()
	if err := e.ingest.EnsureSource(context.Background(), sourceID, application.SourceConfig{
		DefaultTransport: domain.TransportHTTP,
		MaxPoolLeases:    1,
	}, true); err != nil {
		t.Fatalf("ensure source: %v", err)
	}
	result, derr := e.ingest.ImportLines(context.Background(), sourceID, line+"\n")
	if derr != nil {
		t.Fatalf("import: %v", derr)
	}
	if result.Added != 1 {
		t.Fatalf("import added %d, want 1: %+v", result.Added, result)
	}
}

func TestAcquireValidatesBeforeDelivery(t *testing.T) {
	j := startJudge(t)
	proxy := startFakeProxy(t)
	e := newEnv(t, j, nil)
	e.importProxy(t, "manual", fmt.Sprintf("http://127.0.0.1:%d", portOf(proxy.URL)))

	result, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{Count: 1})
	if derr != nil {
		t.Fatalf("acquire: %v", derr)
	}
	if result.Delivered != 1 || len(result.Items) != 1 {
		t.Fatalf("delivered %d, want 1: %+v", result.Delivered, result)
	}
	item := result.Items[0]
	if item.Conn.Host != "127.0.0.1" || item.Conn.Port != portOf(proxy.URL) {
		t.Fatalf("connection mismatch: %+v", item.Conn)
	}
	if item.Validation.Profile != "https_basic_v1" || item.Validation.ObservedExitIP != "203.0.113.7" {
		t.Fatalf("evidence wrong: %+v", item.Validation)
	}
	if !item.ExpiresAt.After(time.Now()) {
		t.Fatalf("lease already expired: %s", item.ExpiresAt)
	}
	stats, err := e.store.Stats(context.Background())
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.ActiveLeases != 1 || stats.ActiveReservations != 0 {
		t.Fatalf("allocation state wrong: %+v", stats)
	}
}

func TestExclusiveLeaseBlocksAndReleaseUnblocks(t *testing.T) {
	j := startJudge(t)
	proxy := startFakeProxy(t)
	e := newEnv(t, j, nil)
	e.importProxy(t, "manual", fmt.Sprintf("http://127.0.0.1:%d", portOf(proxy.URL)))

	first, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{Count: 1})
	if derr != nil {
		t.Fatalf("first acquire: %v", derr)
	}
	_, derr = e.acquire.Acquire(context.Background(), application.AcquireRequest{Count: 1})
	if derr == nil || derr.Code != "NO_ELIGIBLE_CANDIDATES" {
		t.Fatalf("second acquire error = %v, want NO_ELIGIBLE_CANDIDATES", derr)
	}
	if ok, derr := e.leases.Release(context.Background(), first.Items[0].LeaseID); derr != nil || !ok {
		t.Fatalf("release: ok=%v err=%v", ok, derr)
	}
	if ok, derr := e.leases.Release(context.Background(), first.Items[0].LeaseID); derr != nil || ok {
		t.Fatalf("idempotent release: ok=%v err=%v", ok, derr)
	}
	second, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{Count: 1})
	if derr != nil || second.Delivered != 1 {
		t.Fatalf("acquire after release: %v %+v", derr, second)
	}
}

func TestFailingProxyCooldownsInsteadOfRetrying(t *testing.T) {
	j := startJudge(t)
	e := newEnv(t, j, nil)
	e.importProxy(t, "manual", "http://127.0.0.1:1")

	_, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{Count: 1})
	if derr == nil || derr.Code != "INSUFFICIENT_VALIDATED_RESOURCES" {
		t.Fatalf("first acquire error = %v, want INSUFFICIENT_VALIDATED_RESOURCES", derr)
	}
	_, derr = e.acquire.Acquire(context.Background(), application.AcquireRequest{Count: 1})
	if derr == nil || derr.Code != "NO_ELIGIBLE_CANDIDATES" {
		t.Fatalf("second acquire error = %v, want NO_ELIGIBLE_CANDIDATES (cooldown)", derr)
	}
}

func TestJudgeOutageIsInconclusiveAndNotAttributed(t *testing.T) {
	j := startJudge(t)
	proxy := startFakeProxy(t)
	e := newEnv(t, j, nil)
	e.importProxy(t, "manual", fmt.Sprintf("http://127.0.0.1:%d", portOf(proxy.URL)))

	j.healthy.Store(false)
	_, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{Count: 1})
	if derr == nil || derr.Code != "VALIDATION_INFRA_UNAVAILABLE" {
		t.Fatalf("judge outage error = %v, want VALIDATION_INFRA_UNAVAILABLE", derr)
	}
	j.healthy.Store(true)
	result, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{Count: 1})
	if derr != nil || result.Delivered != 1 {
		t.Fatalf("acquire after judge recovery: %v %+v", derr, result)
	}
}

func TestConcurrentAcquireSingleWinner(t *testing.T) {
	j := startJudge(t)
	proxy := startFakeProxy(t)
	e := newEnv(t, j, nil)
	e.importProxy(t, "manual", fmt.Sprintf("http://127.0.0.1:%d", portOf(proxy.URL)))

	var wg sync.WaitGroup
	var mu sync.Mutex
	wins, losses := 0, 0
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{Count: 1})
			mu.Lock()
			defer mu.Unlock()
			if derr == nil {
				wins++
			} else {
				losses++
			}
		}()
	}
	wg.Wait()
	if wins != 1 || losses != 1 {
		t.Fatalf("concurrent acquire wins=%d losses=%d, want 1/1", wins, losses)
	}
	stats, err := e.store.Stats(context.Background())
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.ActiveLeases != 1 || stats.ActiveReservations != 0 {
		t.Fatalf("allocation state wrong: %+v", stats)
	}
}

func TestLeaseExpirySweepReclaimsSlot(t *testing.T) {
	j := startJudge(t)
	proxy := startFakeProxy(t)
	e := newEnv(t, j, func(c *config.Config) { c.LeaseTTLMS = 300; c.MinUsableMS = 100 })
	e.importProxy(t, "manual", fmt.Sprintf("http://127.0.0.1:%d", portOf(proxy.URL)))

	if _, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{Count: 1}); derr != nil {
		t.Fatalf("first acquire: %v", derr)
	}
	_, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{Count: 1})
	if derr == nil {
		t.Fatal("second acquire should fail while lease is active")
	}
	time.Sleep(400 * time.Millisecond)
	maintenance.RunOnce(context.Background(), e.store, e.cfg.AttemptRetention(), testLogger())
	result, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{Count: 1})
	if derr != nil || result.Delivered != 1 {
		t.Fatalf("acquire after sweep: %v %+v", derr, result)
	}
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func portOf(serverURL string) int {
	idx := strings.LastIndex(serverURL, ":")
	port := 0
	fmt.Sscanf(serverURL[idx+1:], "%d", &port)
	return port
}

func TestAPIEndToEnd(t *testing.T) {
	j := startJudge(t)
	proxy := startFakeProxy(t)
	e := newEnv(t, j, nil)
	e.importProxy(t, "manual", fmt.Sprintf("http://127.0.0.1:%d", portOf(proxy.URL)))
	httpSrv := httptest.NewServer(api.NewServer(e.cfg, testLogger(),
		e.acquire, e.ingest, e.leases, e.store, e.nightly).Handler())
	defer httpSrv.Close()

	resp, err := http.Post(httpSrv.URL+"/v1/proxies/acquire", "application/json", strings.NewReader(`{"count":1}`))
	if err != nil {
		t.Fatalf("unauthenticated request: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d body %s", resp.StatusCode, body)
	}

	req, _ := http.NewRequest(http.MethodPost, httpSrv.URL+"/v1/proxies/acquire",
		strings.NewReader(`{"count":1,"profile":"https_basic_v1"}`))
	req.Header.Set("Authorization", "Bearer "+e.cfg.APIToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("acquire request: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("acquire status = %d body %s", resp.StatusCode, body)
	}
	var result struct {
		Delivered int `json:"delivered"`
		Items     []struct {
			LeaseID    string `json:"lease_id"`
			Connection struct {
				Host string `json:"host"`
				Port int    `json:"port"`
			} `json:"connection"`
			Validation struct {
				ObservedExitIP string `json:"observed_exit_ip"`
			} `json:"validation"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("decode acquire response: %v", err)
	}
	if result.Delivered != 1 || len(result.Items) != 1 {
		t.Fatalf("acquire result: %s", body)
	}
	if result.Items[0].Connection.Port != portOf(proxy.URL) ||
		result.Items[0].Validation.ObservedExitIP != "203.0.113.7" {
		t.Fatalf("item payload: %s", body)
	}

	release, _ := http.NewRequest(http.MethodPost,
		fmt.Sprintf("%s/v1/leases/%s/release", httpSrv.URL, result.Items[0].LeaseID), nil)
	release.Header.Set("Authorization", "Bearer "+e.cfg.APIToken)
	resp, err = http.DefaultClient.Do(release)
	if err != nil {
		t.Fatalf("release request: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"released":true`) {
		t.Fatalf("release status = %d body %s", resp.StatusCode, body)
	}
}

func TestNightlyValidationRetiresDeadProxies(t *testing.T) {
	j := startJudge(t)
	e := newEnv(t, j, func(c *config.Config) { c.NightlyRetireFailures = 1 })
	goodProxy := startFakeProxy(t)
	if err := e.ingest.EnsureSource(context.Background(), "manual",
		application.SourceConfig{DefaultTransport: domain.TransportHTTP, MaxPoolLeases: 1}, true); err != nil {
		t.Fatalf("ensure source: %v", err)
	}
	content := fmt.Sprintf("http://127.0.0.1:%d", portOf(goodProxy.URL)) + "\nhttp://127.0.0.1:1\n"
	if _, derr := e.ingest.ImportLines(context.Background(), "manual", content); derr != nil {
		t.Fatalf("import: %v", derr)
	}

	probed, retired, err := e.nightly.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("nightly run: %v", err)
	}
	if probed != 2 || retired != 1 {
		t.Fatalf("probed=%d retired=%d, want 2/1", probed, retired)
	}
	stats, err := e.store.Stats(context.Background())
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.HealthPass != 1 || stats.HealthFail != 1 || stats.EnabledOffers != 1 {
		t.Fatalf("stats after retirement: %+v", stats)
	}

	// Re-uploading the same list must not revive the retired proxy (tombstone).
	reimport, derr := e.ingest.ImportLines(context.Background(), "manual", content)
	if derr != nil || reimport.Skipped != 1 || reimport.Updated != 1 || reimport.Added != 0 {
		t.Fatalf("re-upload after retirement: %v %+v", derr, reimport)
	}

	// The surviving good proxy must still be acquirable.
	acquired, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{Count: 1})
	if derr != nil || acquired.Delivered != 1 {
		t.Fatalf("acquire after nightly pass: %v %+v", derr, acquired)
	}
}

func TestImportSnapshotReplacesMissingOffers(t *testing.T) {
	j := startJudge(t)
	e := newEnv(t, j, nil)
	if err := e.ingest.EnsureSource(context.Background(), "manual",
		application.SourceConfig{DefaultTransport: domain.TransportHTTP, MaxPoolLeases: 1}, true); err != nil {
		t.Fatalf("ensure source: %v", err)
	}
	first, derr := e.ingest.ImportLines(context.Background(), "manual",
		"1.2.3.4:8080\n5.6.7.8:3128\n")
	if derr != nil || first.Added != 2 {
		t.Fatalf("first import: %v %+v", derr, first)
	}
	second, derr := e.ingest.ImportLines(context.Background(), "manual", "1.2.3.4:8080\n")
	if derr != nil {
		t.Fatalf("second import: %v", derr)
	}
	if second.Added != 1 || second.Revoked != 2 {
		t.Fatalf("snapshot counts wrong: %+v", second)
	}
	again, derr := e.ingest.ImportLines(context.Background(), "manual", "1.2.3.4:8080\n")
	if derr != nil || again.Updated != 1 || again.Revoked != 0 || again.Added != 0 {
		t.Fatalf("idempotent re-upload wrong: %+v", again)
	}
	stats, err := e.store.Stats(context.Background())
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.EnabledOffers != 1 || stats.Resources != 2 {
		t.Fatalf("inventory after replacement: %+v", stats)
	}
}

func TestAdminPageAndSourcesAPI(t *testing.T) {
	j := startJudge(t)
	e := newEnv(t, j, nil)
	e.importProxy(t, "manual", "http://127.0.0.1:1")
	httpSrv := httptest.NewServer(api.NewServer(e.cfg, testLogger(),
		e.acquire, e.ingest, e.leases, e.store, e.nightly).Handler())
	defer httpSrv.Close()

	resp, err := http.Get(httpSrv.URL + "/admin")
	if err != nil {
		t.Fatalf("admin page: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "控制台") {
		t.Fatalf("admin page status=%d", resp.StatusCode)
	}

	req, _ := http.NewRequest(http.MethodGet, httpSrv.URL+"/v1/admin/sources", nil)
	req.Header.Set("Authorization", "Bearer "+e.cfg.APIToken)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("sources overview: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"source_id":"manual"`) {
		t.Fatalf("sources overview status=%d body=%s", resp.StatusCode, body)
	}
}

func TestAdminAuthSessionAndDailyStats(t *testing.T) {
	j := startJudge(t)
	proxy := startFakeProxy(t)
	e := newEnv(t, j, nil)
	e.importProxy(t, "manual", fmt.Sprintf("http://127.0.0.1:%d", portOf(proxy.URL)))
	httpSrv := httptest.NewServer(api.NewServer(e.cfg, testLogger(),
		e.acquire, e.ingest, e.leases, e.store, e.nightly).Handler())
	defer httpSrv.Close()
	client := &http.Client{}

	resp, err := client.Get(httpSrv.URL + "/v1/admin/stats")
	if err != nil {
		t.Fatalf("stats without auth: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated stats status=%d", resp.StatusCode)
	}

	login := func(token string) *http.Response {
		r, _ := http.NewRequest(http.MethodPost, httpSrv.URL+"/admin/login",
			strings.NewReader(`{"token":"`+token+`"}`))
		r.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(r)
		if err != nil {
			t.Fatalf("login: %v", err)
		}
		resp.Body.Close()
		return resp
	}
	if resp := login("wrong-token-wrong-token"); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("bad login status=%d", resp.StatusCode)
	}
	resp = login(e.cfg.APIToken)
	if resp.StatusCode != http.StatusOK || len(resp.Cookies()) == 0 {
		t.Fatalf("login status=%d cookies=%d", resp.StatusCode, len(resp.Cookies()))
	}
	var session *http.Cookie
	for _, c := range resp.Cookies() {
		if c.Name == "pp_admin" {
			session = c
		}
	}
	if session == nil {
		t.Fatal("session cookie missing")
	}

	req, _ := http.NewRequest(http.MethodGet, httpSrv.URL+"/v1/admin/stats", nil)
	req.Header.Set("Cookie", session.Name+"="+session.Value)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("stats with cookie: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cookie stats status=%d", resp.StatusCode)
	}

	// Cookie mutations need the CSRF header; bearer requests do not.
	req, _ = http.NewRequest(http.MethodPost, httpSrv.URL+"/v1/sources",
		strings.NewReader(`{"source_id":"manual2","parser_type":"lines_v1"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Cookie", session.Name+"="+session.Value)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("mutation without csrf header: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("missing csrf header status=%d", resp.StatusCode)
	}

	// Run an acquisition so today's validation attempts exist, then check rates.
	acqReq, _ := http.NewRequest(http.MethodPost, httpSrv.URL+"/v1/proxies/acquire",
		strings.NewReader(`{"count":1}`))
	acqReq.Header.Set("Authorization", "Bearer "+e.cfg.APIToken)
	acqReq.Header.Set("Content-Type", "application/json")
	resp, err = client.Do(acqReq)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("acquire status=%d", resp.StatusCode)
	}

	req, _ = http.NewRequest(http.MethodGet, httpSrv.URL+"/v1/admin/daily", nil)
	req.Header.Set("Cookie", session.Name+"="+session.Value)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("daily: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"source_id":"manual"`) ||
		!strings.Contains(string(body), `"proxies":1`) || !strings.Contains(string(body), `"pass":1`) {
		t.Fatalf("daily stats status=%d body=%s", resp.StatusCode, body)
	}

	logout, _ := http.NewRequest(http.MethodPost, httpSrv.URL+"/admin/logout", nil)
	logout.Header.Set("Cookie", session.Name+"="+session.Value)
	resp, err = client.Do(logout)
	if err != nil {
		t.Fatalf("logout: %v", err)
	}
	resp.Body.Close()
	req, _ = http.NewRequest(http.MethodGet, httpSrv.URL+"/v1/admin/stats", nil)
	req.Header.Set("Cookie", session.Name+"="+session.Value)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("stats after logout: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("stats after logout status=%d", resp.StatusCode)
	}
}

func TestManualSourceValidationViaAPI(t *testing.T) {
	j := startJudge(t)
	proxy := startFakeProxy(t)
	e := newEnv(t, j, nil)
	if err := e.ingest.EnsureSource(context.Background(), "manual",
		application.SourceConfig{DefaultTransport: domain.TransportHTTP, MaxPoolLeases: 1}, true); err != nil {
		t.Fatalf("ensure source: %v", err)
	}
	if _, derr := e.ingest.ImportLines(context.Background(), "manual",
		fmt.Sprintf("http://127.0.0.1:%d", portOf(proxy.URL))+"\nhttp://127.0.0.1:1\n"); derr != nil {
		t.Fatalf("import: %v", derr)
	}
	httpSrv := httptest.NewServer(api.NewServer(e.cfg, testLogger(),
		e.acquire, e.ingest, e.leases, e.store, e.nightly).Handler())
	defer httpSrv.Close()

	validate, _ := http.NewRequest(http.MethodPost, httpSrv.URL+"/v1/sources/nope/validate", nil)
	validate.Header.Set("Authorization", "Bearer "+e.cfg.APIToken)
	resp, err := http.DefaultClient.Do(validate)
	if err != nil {
		t.Fatalf("validate unknown source: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown source status=%d", resp.StatusCode)
	}

	validate, _ = http.NewRequest(http.MethodPost, httpSrv.URL+"/v1/sources/manual/validate", nil)
	validate.Header.Set("Authorization", "Bearer "+e.cfg.APIToken)
	resp, err = http.DefaultClient.Do(validate)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("validate status=%d", resp.StatusCode)
	}

	deadline := time.Now().Add(15 * time.Second)
	var status struct {
		Running bool   `json:"running"`
		Probed  int64  `json:"probed"`
		Retired int64  `json:"retired"`
		Mode    string `json:"mode"`
	}
	for {
		if time.Now().After(deadline) {
			t.Fatal("validation did not finish in time")
		}
		req, _ := http.NewRequest(http.MethodGet, httpSrv.URL+"/v1/admin/validation", nil)
		req.Header.Set("Authorization", "Bearer "+e.cfg.APIToken)
		st, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("validation status: %v", err)
		}
		if st.StatusCode != http.StatusOK {
			st.Body.Close()
			t.Fatalf("validation status code=%d", st.StatusCode)
		}
		if err := json.NewDecoder(st.Body).Decode(&status); err != nil {
			st.Body.Close()
			t.Fatalf("decode status: %v", err)
		}
		st.Body.Close()
		if !status.Running {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if status.Probed != 2 || status.Retired != 0 || status.Mode != "manual" {
		t.Fatalf("validation status: %+v", status)
	}
	stats, err := e.store.Stats(context.Background())
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.HealthPass != 1 || stats.HealthFail != 1 {
		t.Fatalf("stats after manual validation: %+v", stats)
	}
}

func TestBuiltInJudgeSignedHTTP(t *testing.T) {
	secret := "test-judge-secret-0123456789abcdef"
	judgeSrv := httptest.NewServer(api.NewJudgeHandler(secret, false))
	defer judgeSrv.Close()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DatabasePath = filepath.Join(dir, "test.db")
	cfg.LockPath = filepath.Join(dir, "test.lock")
	cfg.APIToken = "test-token-0123456789abcdef"
	cfg.IdentitySecret = "test-secret-0123456789abcdef"
	cfg.JudgeURL = judgeSrv.URL + "/v1/judge"
	cfg.JudgeSecret = secret
	cfg.AllowPrivateAddresses = true
	e := newEnvWithConfig(t, cfg, nil)
	proxy := startFakeProxy(t)
	e.importProxy(t, "manual", fmt.Sprintf("http://127.0.0.1:%d", portOf(proxy.URL)))

	result, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{Count: 1})
	if derr != nil || result.Delivered != 1 {
		t.Fatalf("acquire via built-in judge: %v %+v", derr, result)
	}
	// The judge observed the fake proxy's own egress address.
	if got := result.Items[0].Validation.ObservedExitIP; got != "127.0.0.1" {
		t.Fatalf("observed exit ip %q, want 127.0.0.1", got)
	}

	// A probe configured with the wrong judge secret must fail as suspected
	// interception instead of trusting an unverifiable response.
	if _, derr := e.leases.Release(context.Background(), result.Items[0].LeaseID); derr != nil {
		t.Fatalf("release for wrong-secret case: %v", derr)
	}
	cfgWrong := cfg
	cfgWrong.JudgeSecret = "wrong-judge-secret-0123456789abcdef"
	bad := application.NewAcquireService(e.store, cfgWrong, cfgWrong.JudgeURL, nil,
		make(chan struct{}, cfg.ProbeSlots))
	if err := e.ingest.EnsureSource(context.Background(), "manual2",
		application.SourceConfig{DefaultTransport: domain.TransportHTTP, MaxPoolLeases: 1}, true); err != nil {
		t.Fatalf("ensure source2: %v", err)
	}
	if _, derr := e.ingest.ImportLines(context.Background(), "manual2",
		fmt.Sprintf("http://127.0.0.1:%d", portOf(proxy.URL))+"\n"); derr != nil {
		t.Fatalf("import manual2: %v", derr)
	}
	if _, derr := bad.Acquire(context.Background(), application.AcquireRequest{Count: 1}); derr == nil ||
		derr.Code != "INSUFFICIENT_VALIDATED_RESOURCES" {
		t.Fatalf("wrong-secret acquire error = %v, want INSUFFICIENT_VALIDATED_RESOURCES", derr)
	}
}
