package integration

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
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
	prewarm *application.PrewarmService
	keys    *application.KeyService
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
	if _, err := store.MigrateWithOptions(context.Background(), sqlite.MigrateOptions{
		IdentitySecret: cfg.IdentitySecret, LegacyMaxCooldown: cfg.D(cfg.MaxDownstreamCooldownMS),
	}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	probeSlots := make(chan struct{}, cfg.ProbeSlots)
	e := &env{
		cfg:    cfg,
		store:  store,
		ingest: application.NewIngestService(store, cfg.IdentitySecret, cfg.AllowPrivateAddresses),
		leases: application.NewLeaseService(store, application.CooldownConfig{Base: cfg.CooldownBase(), Max: cfg.CooldownMax(), Jitter: cfg.CooldownJitter}),
		acquire: application.NewAcquireService(store, cfg, cfg.JudgeURL, rootCAs,
			probeSlots, nil),
		nightly: application.NewNightlyValidator(store, cfg, cfg.JudgeURL, rootCAs,
			application.CooldownConfig{Base: cfg.CooldownBase(), Max: cfg.CooldownMax(), Jitter: cfg.CooldownJitter},
			probeSlots, nil),
		prewarm: application.NewPrewarmService(store, cfg, cfg.JudgeURL, rootCAs,
			probeSlots, nil),
		keys: application.NewKeyService(store, cfg.IdentitySecret),
	}
	if err := e.ingest.ConfigureStaticCache(cfg.StaticCacheTTL(),
		cfg.StaticCacheMaxEntries, cfg.StaticCacheMaxBytes); err != nil {
		t.Fatalf("configure static cache: %v", err)
	}
	e.acquire.Refiller = e.ingest
	if rootCAs != nil {
		digest := sha256.Sum256([]byte(cfg.JudgeCAPem))
		e.acquire.SetTargetRootCAs(rootCAs, hex.EncodeToString(digest[:]))
	}
	return e
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

func TestReleaseDoesNotClearProjectCooldown(t *testing.T) {
	j := startJudge(t)
	proxy := startFakeProxy(t)
	e := newEnv(t, j, nil)
	e.importProxy(t, "manual", fmt.Sprintf("http://127.0.0.1:%d", portOf(proxy.URL)))

	req := application.AcquireRequest{Count: 1, ConsumerID: "audit-release", CooldownMS: 60000}
	first, derr := e.acquire.Acquire(context.Background(), req)
	if derr != nil {
		t.Fatalf("first acquire: %v", derr)
	}
	blocked, derr := e.acquire.Acquire(context.Background(), req)
	if derr != nil || blocked.Delivered != 0 || blocked.Missing != 1 {
		t.Fatalf("second acquire must be an empty CD result: %v %+v", derr, blocked)
	}
	if ok, derr := e.leases.Release(context.Background(), first.Items[0].LeaseID, "master", true); derr != nil || !ok {
		t.Fatalf("release: ok=%v err=%v", ok, derr)
	}
	if ok, derr := e.leases.Release(context.Background(), first.Items[0].LeaseID, "master", true); derr != nil || ok {
		t.Fatalf("idempotent release: ok=%v err=%v", ok, derr)
	}
	second, derr := e.acquire.Acquire(context.Background(), req)
	if derr != nil || second.Delivered != 0 {
		t.Fatalf("release must not cancel the committed CD: %v %+v", derr, second)
	}
}

func TestFailingProxyCooldownsInsteadOfRetrying(t *testing.T) {
	j := startJudge(t)
	e := newEnv(t, j, nil)
	e.importProxy(t, "manual", "http://127.0.0.1:1")

	first, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{Count: 1})
	if derr != nil || first.Delivered != 0 || first.ShortfallReason == "" {
		t.Fatalf("first acquire must explain its shortfall: %v %+v", derr, first)
	}
	second, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{Count: 1})
	if derr != nil || second.Delivered != 0 {
		t.Fatalf("cooling candidate must not be delivered: %v %+v", derr, second)
	}
}

func TestCompletedFailedValidationDeletesUploadAndReuploadRestoresSupply(t *testing.T) {
	j := startJudge(t)
	proxy := startFakeProxy(t)
	e := newEnv(t, j, nil)
	e.importProxy(t, "manual", fmt.Sprintf("http://127.0.0.1:%d", portOf(proxy.URL)))

	j.healthy.Store(false)
	missing, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{Count: 1})
	if derr != nil || missing.Delivered != 0 || missing.ShortfallReason != "VALIDATION_INFRA_UNAVAILABLE" {
		t.Fatalf("judge outage must be an inconclusive shortfall: %v %+v", derr, missing)
	}
	j.healthy.Store(true)
	e.importProxy(t, "manual", fmt.Sprintf("http://127.0.0.1:%d", portOf(proxy.URL)))
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
			result, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{
				Count: 1, ConsumerID: "audit-concurrent", CooldownMS: 60000,
			})
			mu.Lock()
			defer mu.Unlock()
			if derr == nil && result.Delivered == 1 {
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

func TestLeaseExpirySweepPreservesProjectCooldown(t *testing.T) {
	j := startJudge(t)
	proxy := startFakeProxy(t)
	e := newEnv(t, j, func(c *config.Config) { c.LeaseTTLMS = 300; c.MinUsableMS = 100 })
	e.importProxy(t, "manual", fmt.Sprintf("http://127.0.0.1:%d", portOf(proxy.URL)))

	req := application.AcquireRequest{Count: 1, ConsumerID: "audit-sweep", CooldownMS: 60000}
	if result, derr := e.acquire.Acquire(context.Background(), req); derr != nil || result.Delivered != 1 {
		t.Fatalf("first acquire: %v", derr)
	}
	blocked, derr := e.acquire.Acquire(context.Background(), req)
	if derr != nil || blocked.Delivered != 0 {
		t.Fatalf("second acquire must respect CD: %v %+v", derr, blocked)
	}
	time.Sleep(400 * time.Millisecond)
	maintenance.RunOnce(context.Background(), e.store, e.cfg.AttemptRetention(), 0, testLogger())
	result, derr := e.acquire.Acquire(context.Background(), req)
	if derr != nil || result.Delivered != 0 {
		t.Fatalf("lease expiry must not clear CD: %v %+v", derr, result)
	}
	other, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{
		Count: 1, ConsumerID: "audit-other-project",
	})
	if derr != nil || other.Delivered != 1 {
		t.Fatalf("another project's independent CD must remain usable: %v %+v", derr, other)
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

// fakeSocks5 implements the minimal SOCKS5 server the probe expects: method
// negotiation, optional username/password auth (RFC 1929), CONNECT, and raw
// relaying to the requested target.
type fakeSocks5 struct {
	ln          net.Listener
	authUser    string
	authPass    string
	requireAuth bool
}

func startFakeSocks5(t *testing.T, authUser, authPass string) *fakeSocks5 {
	t.Helper()
	f := &fakeSocks5{
		authUser:    authUser,
		authPass:    authPass,
		requireAuth: authUser != "",
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	f.ln = ln
	go f.serve()
	t.Cleanup(func() { ln.Close() })
	return f
}

func (f *fakeSocks5) addr() string { return f.ln.Addr().String() }

func (f *fakeSocks5) serve() {
	for {
		conn, err := f.ln.Accept()
		if err != nil {
			return
		}
		go f.handle(conn)
	}
}

func (f *fakeSocks5) handle(conn net.Conn) {
	defer conn.Close()
	greeting, err := readN(conn, 2)
	if err != nil || greeting[0] != 5 {
		return
	}
	methods, err := readN(conn, int(greeting[1]))
	if err != nil {
		return
	}
	if f.requireAuth {
		has := false
		for _, m := range methods {
			if m == 0x02 {
				has = true
			}
		}
		if !has {
			return
		}
		if _, err := conn.Write([]byte{5, 0x02}); err != nil {
			return
		}
		creds, err := readN(conn, 2)
		if err != nil || creds[0] != 1 {
			return
		}
		user, err := readN(conn, int(creds[1]))
		if err != nil {
			return
		}
		plen, err := readN(conn, 1)
		if err != nil {
			return
		}
		pass, err := readN(conn, int(plen[0]))
		if err != nil {
			return
		}
		if string(user) != f.authUser || string(pass) != f.authPass {
			_, _ = conn.Write([]byte{1, 0x01})
			return
		}
		if _, err := conn.Write([]byte{1, 0x00}); err != nil {
			return
		}
	} else {
		if _, err := conn.Write([]byte{5, 0x00}); err != nil {
			return
		}
	}
	req, err := readN(conn, 4)
	if err != nil || req[0] != 5 || req[1] != 1 {
		return
	}
	var host string
	switch req[3] {
	case 1:
		ip, err := readN(conn, 4)
		if err != nil {
			return
		}
		host = net.IP(ip).String()
	case 3:
		l, err := readN(conn, 1)
		if err != nil {
			return
		}
		name, err := readN(conn, int(l[0]))
		if err != nil {
			return
		}
		host = string(name)
	default:
		return
	}
	portBytes, err := readN(conn, 2)
	if err != nil {
		return
	}
	port := int(portBytes[0])<<8 | int(portBytes[1])
	dst, err := net.DialTimeout("tcp", net.JoinHostPort(host, fmt.Sprintf("%d", port)), 2*time.Second)
	if err != nil {
		_, _ = conn.Write([]byte{5, 1, 0, 1, 0, 0, 0, 0, 0, 0})
		return
	}
	defer dst.Close()
	if _, err := conn.Write([]byte{5, 0, 0, 1, 0, 0, 0, 0, 0, 0}); err != nil {
		return
	}
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(dst, conn); done <- struct{}{} }()
	go func() { _, _ = io.Copy(conn, dst); done <- struct{}{} }()
	<-done
}

func readN(r io.Reader, n int) ([]byte, error) {
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}
	return b, nil
}

func TestAPIEndToEnd(t *testing.T) {
	j := startJudge(t)
	proxy := startFakeProxy(t)
	e := newEnv(t, j, nil)
	e.importProxy(t, "manual", fmt.Sprintf("http://127.0.0.1:%d", portOf(proxy.URL)))
	httpSrv := httptest.NewServer(api.NewServer(e.cfg, testLogger(),
		e.acquire, e.ingest, e.leases, e.store, e.nightly, e.prewarm, e.keys).Handler())
	defer httpSrv.Close()

	resp, err := http.Post(httpSrv.URL+"/api/v1/proxies/acquire", "application/json", strings.NewReader(`{"count":1}`))
	if err != nil {
		t.Fatalf("unauthenticated request: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d body %s", resp.StatusCode, body)
	}

	req, _ := http.NewRequest(http.MethodPost, httpSrv.URL+"/api/v1/proxies/acquire",
		strings.NewReader(`{"count":1}`))
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
			Observed struct {
				ExitIP string `json:"exit_ip"`
			} `json:"observed"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("decode acquire response: %v", err)
	}
	if result.Delivered != 1 || len(result.Items) != 1 {
		t.Fatalf("acquire result: %s", body)
	}
	if result.Items[0].Connection.Port != portOf(proxy.URL) ||
		result.Items[0].Observed.ExitIP != "203.0.113.7" {
		t.Fatalf("item payload: %s", body)
	}
	row, err := e.store.GetLease(context.Background(), result.Items[0].LeaseID)
	if err != nil {
		t.Fatal(err)
	}
	var proof domain.Evidence
	if err := json.Unmarshal([]byte(row.ValidationJSON), &proof); err != nil ||
		proof.Outcome != "PASS" || proof.ObservedExitIP != result.Items[0].Observed.ExitIP {
		t.Fatalf("canonical observation is not backed by committed PASS evidence: err=%v", err)
	}

	release, _ := http.NewRequest(http.MethodPost,
		fmt.Sprintf("%s/api/v1/leases/%s/release", httpSrv.URL, result.Items[0].LeaseID), nil)
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

func TestNightlyValidationDeletesFailedUploadsAndReuploadIsUnvalidated(t *testing.T) {
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
		t.Fatalf("completed connection refusal must remove the uploaded candidate: probed=%d retired=%d", probed, retired)
	}
	stats, err := e.store.Stats(context.Background())
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.HealthPass != 1 || stats.HealthFail != 0 || stats.EnabledOffers != 1 ||
		stats.Resources != 1 {
		t.Fatalf("failed candidate was not removed while good candidate was retained: %+v", stats)
	}

	// Re-uploading must merge without resetting either candidate's health.
	reimport, derr := e.ingest.ImportLines(context.Background(), "manual", content)
	if derr != nil || reimport.Skipped != 0 || reimport.Updated != 1 || reimport.Added != 1 {
		t.Fatalf("re-upload after uncertainty: %v %+v", derr, reimport)
	}

	// The surviving good proxy must still be acquirable.
	acquired, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{Count: 1})
	if derr != nil || acquired.Delivered != 1 {
		t.Fatalf("acquire after nightly pass: %v %+v", derr, acquired)
	}
}

func TestImportLinesAppendsToPool(t *testing.T) {
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
	if second.Added != 0 || second.Updated != 1 {
		t.Fatalf("append counts wrong: %+v", second)
	}
	again, derr := e.ingest.ImportLines(context.Background(), "manual", "1.2.3.4:8080\n")
	if derr != nil || again.Updated != 1 || again.Added != 0 {
		t.Fatalf("idempotent re-upload wrong: %+v", again)
	}
	stats, err := e.store.Stats(context.Background())
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.EnabledOffers != 2 || stats.Resources != 2 {
		t.Fatalf("inventory after append: %+v", stats)
	}
}

func TestDeleteSourceViaAPI(t *testing.T) {
	j := startJudge(t)
	proxy := startFakeProxy(t)
	e := newEnv(t, j, nil)
	e.importProxy(t, "manual", fmt.Sprintf("http://127.0.0.1:%d", portOf(proxy.URL)))
	httpSrv := httptest.NewServer(api.NewServer(e.cfg, testLogger(),
		e.acquire, e.ingest, e.leases, e.store, e.nightly, e.prewarm, e.keys).Handler())
	defer httpSrv.Close()

	del := func(id string) (int, []byte) {
		req, _ := http.NewRequest(http.MethodDelete, httpSrv.URL+"/api/v1/admin/sources/"+id, nil)
		req.Header.Set("Authorization", "Bearer "+e.cfg.APIToken)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("delete %s: %v", id, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return resp.StatusCode, body
	}
	if code, _ := del("nope"); code != http.StatusNotFound {
		t.Fatalf("delete unknown source status=%d", code)
	}
	code, bodyBytes := del("manual")
	if code != http.StatusOK {
		t.Fatalf("delete status=%d", code)
	}
	var body struct {
		RemovedOffers int `json:"removed_offers"`
	}
	if err := json.Unmarshal(bodyBytes, &body); err != nil {
		t.Fatalf("decode delete response: %v", err)
	}
	if body.RemovedOffers != 1 {
		t.Fatalf("removed_offers=%d, want 1", body.RemovedOffers)
	}
	stats, err := e.store.Stats(context.Background())
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.EnabledOffers != 0 {
		t.Fatalf("offers survived deletion: %+v", stats)
	}
	if stats.Resources != 0 {
		t.Fatalf("resources survived deletion: %+v", stats)
	}
	// Re-uploading after deletion rebuilds the channel from scratch.
	e.importProxy(t, "manual", fmt.Sprintf("http://127.0.0.1:%d", portOf(proxy.URL)))
	stats, err = e.store.Stats(context.Background())
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.EnabledOffers != 1 {
		t.Fatalf("re-upload after deletion: %+v", stats)
	}
}

func TestAcquireSupportsSocks5(t *testing.T) {
	j := startJudge(t)
	p := startFakeSocks5(t, "", "")
	e := newEnv(t, j, nil)
	e.importProxy(t, "manual", "socks5://"+p.addr())

	result, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{Count: 1})
	if derr != nil || result.Delivered != 1 {
		t.Fatalf("socks5 acquire: %v %+v", derr, result)
	}
	if got := result.Items[0].Conn.Transport; got != domain.TransportSOCKS5 {
		t.Fatalf("transport = %q, want socks5", got)
	}
	if result.Items[0].Validation.ObservedExitIP != "203.0.113.7" {
		t.Fatalf("evidence wrong: %+v", result.Items[0].Validation)
	}
}

func TestSocks5AuthFailureRetiresImmediately(t *testing.T) {
	j := startJudge(t)
	p := startFakeSocks5(t, "realuser", "realpass")
	e := newEnv(t, j, nil)
	if err := e.ingest.EnsureSource(context.Background(), "manual",
		application.SourceConfig{DefaultTransport: domain.TransportSOCKS5, MaxPoolLeases: 1}, true); err != nil {
		t.Fatalf("ensure source: %v", err)
	}
	if _, derr := e.ingest.ImportLines(context.Background(), "manual",
		"socks5://wronguser:wrongpass@"+p.addr()+"\n"); derr != nil {
		t.Fatalf("import: %v", derr)
	}
	if result, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{Count: 1}); derr != nil || result.Delivered != 0 {
		t.Fatalf("wrong socks5 credentials must not be delivered: %v %+v", derr, result)
	}
	stats, err := e.store.Stats(context.Background())
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.EnabledOffers != 0 || stats.Resources != 0 {
		t.Fatalf("auth-failed socks5 offer not retired: %+v", stats)
	}
	// There is no persistent blacklist; a later upload creates an unverified candidate.
	res, derr := e.ingest.ImportLines(context.Background(), "manual",
		"socks5://wronguser:wrongpass@"+p.addr()+"\n")
	if derr != nil {
		t.Fatalf("re-import: %v", derr)
	}
	if res.Added != 1 || res.Skipped != 0 {
		t.Fatalf("a deleted connection must not leave a tombstone: %+v", res)
	}
}

func TestAdminPageAndSourcesAPI(t *testing.T) {
	j := startJudge(t)
	e := newEnv(t, j, nil)
	e.importProxy(t, "manual", "http://127.0.0.1:1")
	httpSrv := httptest.NewServer(api.NewServer(e.cfg, testLogger(),
		e.acquire, e.ingest, e.leases, e.store, e.nightly, e.prewarm, e.keys).Handler())
	defer httpSrv.Close()

	resp, err := http.Get(httpSrv.URL + "/admin")
	if err != nil {
		t.Fatalf("admin page: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "proxy-pool 管理台") {
		t.Fatalf("admin page status=%d", resp.StatusCode)
	}

	req, _ := http.NewRequest(http.MethodGet, httpSrv.URL+"/api/v1/admin/sources", nil)
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
		e.acquire, e.ingest, e.leases, e.store, e.nightly, e.prewarm, e.keys).Handler())
	defer httpSrv.Close()
	client := &http.Client{}

	resp, err := client.Get(httpSrv.URL + "/api/v1/admin/stats")
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

	req, _ := http.NewRequest(http.MethodGet, httpSrv.URL+"/api/v1/admin/stats", nil)
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
	req, _ = http.NewRequest(http.MethodPost, httpSrv.URL+"/api/v1/admin/sources",
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
	acqReq, _ := http.NewRequest(http.MethodPost, httpSrv.URL+"/api/v1/proxies/acquire",
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

	req, _ = http.NewRequest(http.MethodGet, httpSrv.URL+"/api/v1/admin/daily", nil)
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
	req, _ = http.NewRequest(http.MethodGet, httpSrv.URL+"/api/v1/admin/stats", nil)
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
		e.acquire, e.ingest, e.leases, e.store, e.nightly, e.prewarm, e.keys).Handler())
	defer httpSrv.Close()

	validate, _ := http.NewRequest(http.MethodPost, httpSrv.URL+"/api/v1/admin/sources/nope/validate", nil)
	validate.Header.Set("Authorization", "Bearer "+e.cfg.APIToken)
	resp, err := http.DefaultClient.Do(validate)
	if err != nil {
		t.Fatalf("validate unknown source: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown source status=%d", resp.StatusCode)
	}

	validate, _ = http.NewRequest(http.MethodPost, httpSrv.URL+"/api/v1/admin/sources/manual/validate", nil)
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
		req, _ := http.NewRequest(http.MethodGet, httpSrv.URL+"/api/v1/admin/validation", nil)
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
	if status.Probed != 2 || status.Retired != 1 || status.Mode != "manual" {
		t.Fatalf("validation status: %+v", status)
	}
	stats, err := e.store.Stats(context.Background())
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.HealthPass != 1 || stats.HealthFail != 0 || stats.Resources != 1 {
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

	// A different judge signing configuration must invalidate reusable evidence.
	if _, derr := e.leases.Release(context.Background(), result.Items[0].LeaseID, "master", true); derr != nil {
		t.Fatalf("release for wrong-secret case: %v", derr)
	}
	cfgWrong := cfg
	cfgWrong.JudgeSecret = "wrong-judge-secret-0123456789abcdef"
	bad := application.NewAcquireService(e.store, cfgWrong, cfgWrong.JudgeURL, nil,
		make(chan struct{}, cfg.ProbeSlots), nil)
	if err := e.ingest.EnsureSource(context.Background(), "manual2",
		application.SourceConfig{DefaultTransport: domain.TransportHTTP, MaxPoolLeases: 1}, true); err != nil {
		t.Fatalf("ensure source2: %v", err)
	}
	if _, derr := e.ingest.ImportLines(context.Background(), "manual2",
		fmt.Sprintf("http://127.0.0.1:%d", portOf(proxy.URL))+"\n"); derr != nil {
		t.Fatalf("import manual2: %v", derr)
	}
	if result, derr := bad.Acquire(context.Background(), application.AcquireRequest{Count: 1}); derr != nil || result.Delivered != 0 {
		t.Fatalf("wrong-secret evidence must not be accepted or reused: %v %+v", derr, result)
	}
}

func TestAuthRequiredProxyClassifiedAsAuth(t *testing.T) {
	j := startJudge(t)
	// A proxy that always answers CONNECT with 407 (expired credentials).
	authProxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Proxy Authentication Required", http.StatusProxyAuthRequired)
	}))
	defer authProxy.Close()
	e := newEnv(t, j, nil)
	e.importProxy(t, "manual", fmt.Sprintf("http://user:pass@127.0.0.1:%d", portOf(authProxy.URL)))

	result, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{Count: 1})
	if derr != nil || result.Delivered != 0 {
		t.Fatalf("auth-failed proxy must not be delivered: %v %+v", derr, result)
	}
	stats, err := e.store.Stats(context.Background())
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Resources != 0 || stats.EnabledOffers != 0 {
		// Only an AUTH classification retires immediately; a PROXY
		// classification would leave the resource in inventory on cooldown.
		t.Fatalf("407 must retire the resource immediately: %+v", stats)
	}
}
