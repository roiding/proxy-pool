package integration

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"proxypool/internal/api"
	"proxypool/internal/application"
	"proxypool/internal/config"
	"proxypool/internal/domain"
	"proxypool/internal/geoip"
	"proxypool/internal/storage/sqlite"
)

func TestSourceCircuitOpensAndSwitchesSources(t *testing.T) {
	j := startJudge(t)
	good := startFakeProxy(t)
	e := newEnv(t, j, func(c *config.Config) {
		c.SourceFailureThreshold = 2
		c.SourceCooldownBaseMS = 300000
		c.CooldownBaseMS = 1
		c.CooldownJitter = 0
	})
	if err := e.ingest.EnsureSource(context.Background(), "bad",
		application.SourceConfig{DefaultTransport: domain.TransportHTTP, MaxPoolLeases: 1}, true); err != nil {
		t.Fatalf("ensure bad source: %v", err)
	}
	if _, derr := e.ingest.ImportLines(context.Background(), "bad",
		"http://127.0.0.1:1\nhttp://127.0.0.1:2\n"); derr != nil {
		t.Fatalf("import bad: %v", derr)
	}

	// Two consecutive attributed failures open the source circuit.
	for i := 0; i < 2; i++ {
		if _, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{Count: 1}); derr == nil {
			t.Fatalf("dead source acquire %d should fail", i)
		}
	}
	overviews, err := e.store.SourcesOverview(context.Background())
	if err != nil || len(overviews) != 1 {
		t.Fatalf("overview: %v %+v", err, overviews)
	}
	if overviews[0].CircuitState != sqlite.CircuitOpen || overviews[0].CooldownUntil == nil {
		t.Fatalf("bad source circuit not open: %+v", overviews[0])
	}

	// Let the per-resource cooldowns lapse. The source circuit still blocks
	// selection, and the shortfall must say the source is cooling.
	time.Sleep(20 * time.Millisecond)
	_, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{Count: 1})
	if derr == nil || !strings.Contains(derr.Message, "cooling") {
		t.Fatalf("expected cooling shortfall, got %v", derr)
	}

	// A healthy source remains selectable: dynamic source switching.
	if err := e.ingest.EnsureSource(context.Background(), "good",
		application.SourceConfig{DefaultTransport: domain.TransportHTTP, MaxPoolLeases: 1}, true); err != nil {
		t.Fatalf("ensure good source: %v", err)
	}
	if _, derr := e.ingest.ImportLines(context.Background(), "good",
		fmt.Sprintf("http://127.0.0.1:%d\n", portOf(good.URL))); derr != nil {
		t.Fatalf("import good: %v", derr)
	}
	acquired, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{Count: 1})
	if derr != nil || acquired.Delivered != 1 || acquired.Items[0].SourceID != "good" {
		t.Fatalf("switch to healthy source failed: %v %+v", derr, acquired)
	}
}

func TestMigrateV1ToV5(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	db, err := sqlite.Open(filepath.Join(dir, "migrate.db"), cfg.SQLiteCacheKiB)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	store := sqlite.New(db)
	ctx := context.Background()
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("initial migrate: %v", err)
	}
	// Simulate an existing v1 deployment and migrate forward.
	if _, err := db.ExecContext(ctx, `
		DROP TABLE resource_observations;
		DROP TABLE source_circuits;
		DROP TABLE consumer_proxy_cooldowns;
		DROP TABLE api_keys;
		DROP TABLE reservations;
		DROP TABLE leases;
		CREATE TABLE reservations (
		  reservation_id TEXT PRIMARY KEY,
		  resource_id TEXT NOT NULL REFERENCES proxy_resources(resource_id),
		  offer_id TEXT NOT NULL,
		  context_key TEXT NOT NULL,
		  owner TEXT NOT NULL,
		  fence INTEGER NOT NULL,
		  deadline TEXT NOT NULL,
		  state TEXT NOT NULL DEFAULT 'RESERVED',
		  created_at TEXT NOT NULL
		);
		CREATE INDEX idx_res_resource ON reservations(resource_id, state, deadline);
		CREATE TABLE leases (
		  lease_id TEXT PRIMARY KEY,
		  tenant TEXT NOT NULL,
		  request_id TEXT NOT NULL,
		  resource_id TEXT NOT NULL REFERENCES proxy_resources(resource_id),
		  offer_id TEXT NOT NULL,
		  context_key TEXT NOT NULL,
		  mode TEXT NOT NULL DEFAULT 'exclusive',
		  fence INTEGER NOT NULL,
		  deadline TEXT NOT NULL,
		  state TEXT NOT NULL DEFAULT 'ACTIVE',
		  connection_json TEXT NOT NULL,
		  validation_json TEXT NOT NULL,
		  created_at TEXT NOT NULL,
		  released_at TEXT
		);
		CREATE INDEX idx_leases_resource ON leases(resource_id, state, deadline);
		PRAGMA user_version = 1;`); err != nil {
		t.Fatalf("simulate v1: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("v1 -> v5 migrate: %v", err)
	}
	var n int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'
		AND name IN ('source_circuits', 'resource_observations',
		             'api_keys', 'consumer_proxy_cooldowns')`).Scan(&n); err != nil {
		t.Fatalf("count tables: %v", err)
	}
	if n != 4 {
		t.Fatalf("v5 tables missing after migration: %d", n)
	}
	cols, err := db.QueryContext(ctx, `PRAGMA table_info(reservations)`)
	if err != nil {
		t.Fatalf("reservation columns: %v", err)
	}
	defer cols.Close()
	found := false
	for cols.Next() {
		var cid int
		var name, typ string
		var notnull, pk int
		var dflt any
		if err := cols.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			t.Fatalf("scan column: %v", err)
		}
		if name == "consumer_id" {
			found = true
		}
	}
	if !found {
		t.Fatal("reservations.consumer_id missing after migration")
	}
	leaseCols, err := db.QueryContext(ctx, `PRAGMA table_info(leases)`)
	if err != nil {
		t.Fatalf("lease columns: %v", err)
	}
	defer leaseCols.Close()
	found = false
	for leaseCols.Next() {
		var cid int
		var name, typ string
		var notnull, pk int
		var dflt any
		if err := leaseCols.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			t.Fatalf("scan lease column: %v", err)
		}
		if name == "consumer_id" {
			found = true
		}
	}
	if !found {
		t.Fatal("leases.consumer_id missing after migration")
	}
}

func TestSourceCircuitBackoffAndReset(t *testing.T) {
	j := startJudge(t)
	e := newEnv(t, j, nil)
	if err := e.ingest.EnsureSource(context.Background(), "s",
		application.SourceConfig{DefaultTransport: domain.TransportHTTP, MaxPoolLeases: 1}, true); err != nil {
		t.Fatalf("ensure source: %v", err)
	}
	policy := sqlite.SourceCircuitPolicy{
		FailureThreshold: 2,
		Base:             10 * time.Second,
		Max:              25 * time.Second,
	}
	now := time.Now()
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		circuit, err := e.store.RecordSourceOutcome(ctx, "s", "FAIL", now, policy)
		if err != nil {
			t.Fatalf("fail %d: %v", i, err)
		}
		if i == 0 && circuit.State != sqlite.CircuitClosed {
			t.Fatalf("circuit opened before threshold: %+v", circuit)
		}
		if i == 1 {
			if circuit.State != sqlite.CircuitOpen || circuit.CooldownUntil == nil {
				t.Fatalf("circuit did not open: %+v", circuit)
			}
			if d := circuit.CooldownUntil.Sub(now); d != 10*time.Second {
				t.Fatalf("first cooldown = %s, want 10s", d)
			}
		}
	}
	// A failed trial after the cooldown doubles the backoff.
	trial := now.Add(11 * time.Second)
	circuit, err := e.store.RecordSourceOutcome(ctx, "s", "FAIL", trial, policy)
	if err != nil {
		t.Fatalf("trial fail: %v", err)
	}
	if circuit.OpenCount != 2 {
		t.Fatalf("open_count = %d, want 2", circuit.OpenCount)
	}
	if d := circuit.CooldownUntil.Sub(trial); d != 20*time.Second {
		t.Fatalf("second cooldown = %s, want 20s", d)
	}
	// Cap applies.
	later := trial.Add(21 * time.Second)
	circuit, err = e.store.RecordSourceOutcome(ctx, "s", "FAIL", later, policy)
	if err != nil {
		t.Fatalf("capped fail: %v", err)
	}
	if d := circuit.CooldownUntil.Sub(later); d != 25*time.Second {
		t.Fatalf("capped cooldown = %s, want 25s", d)
	}
	// A pass closes and resets.
	circuit, err = e.store.RecordSourceOutcome(ctx, "s", "PASS", later.Add(time.Second), policy)
	if err != nil {
		t.Fatalf("pass: %v", err)
	}
	if circuit.State != sqlite.CircuitClosed || circuit.ConsecutiveFailures != 0 ||
		circuit.OpenCount != 0 || circuit.CooldownUntil != nil {
		t.Fatalf("pass did not reset circuit: %+v", circuit)
	}
}

func TestPrewarmReachesTarget(t *testing.T) {
	j := startJudge(t)
	good1 := startFakeProxy(t)
	good2 := startFakeProxy(t)
	e := newEnv(t, j, nil)
	if err := e.ingest.EnsureSource(context.Background(), "manual",
		application.SourceConfig{DefaultTransport: domain.TransportHTTP, MaxPoolLeases: 1}, true); err != nil {
		t.Fatalf("ensure source: %v", err)
	}
	content := fmt.Sprintf("http://127.0.0.1:%d\nhttp://127.0.0.1:%d\nhttp://127.0.0.1:1\n",
		portOf(good1.URL), portOf(good2.URL))
	if _, derr := e.ingest.ImportLines(context.Background(), "manual", content); derr != nil {
		t.Fatalf("import: %v", derr)
	}
	status, err := e.prewarm.RunOnce(context.Background(), application.PrewarmRequest{
		Count: 2, DeadlineMS: 10000,
	})
	if err != nil {
		t.Fatalf("prewarm: %v", err)
	}
	if status.Running || status.Ready != 2 || status.Probed < 2 || status.Probed > 3 {
		t.Fatalf("prewarm status: %+v", status)
	}
	if status.FinishedAt == nil {
		t.Fatalf("prewarm did not finish: %+v", status)
	}
	acquired, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{Count: 1})
	if derr != nil || acquired.Delivered != 1 {
		t.Fatalf("acquire after prewarm: %v %+v", derr, acquired)
	}
}

func TestPrewarmAPI(t *testing.T) {
	j := startJudge(t)
	good1 := startFakeProxy(t)
	good2 := startFakeProxy(t)
	e := newEnv(t, j, nil)
	if err := e.ingest.EnsureSource(context.Background(), "manual",
		application.SourceConfig{DefaultTransport: domain.TransportHTTP, MaxPoolLeases: 1}, true); err != nil {
		t.Fatalf("ensure source: %v", err)
	}
	content := fmt.Sprintf("http://127.0.0.1:%d\nhttp://127.0.0.1:%d\n",
		portOf(good1.URL), portOf(good2.URL))
	if _, derr := e.ingest.ImportLines(context.Background(), "manual", content); derr != nil {
		t.Fatalf("import: %v", derr)
	}
	httpSrv := httptest.NewServer(api.NewServer(e.cfg, testLogger(),
		e.acquire, e.ingest, e.leases, e.store, e.nightly, e.prewarm, e.keys).Handler())
	defer httpSrv.Close()
	req, _ := http.NewRequest(http.MethodPost, httpSrv.URL+"/v1/prewarm",
		strings.NewReader(`{"count":2,"deadline_ms":10000}`))
	req.Header.Set("Authorization", "Bearer "+e.cfg.APIToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("prewarm request: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("prewarm status=%d body=%s", resp.StatusCode, body)
	}

	var status application.PrewarmStatus
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		status = e.prewarm.Status()
		if !status.Running && status.FinishedAt != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if status.Running || status.Ready != 2 {
		t.Fatalf("prewarm API status: %+v", status)
	}
}

type fakeGeo struct{ loc geoip.Location }

func (f fakeGeo) Lookup(netip.Addr) (geoip.Location, bool) { return f.loc, true }

func TestGeoObservationAndCountryFilter(t *testing.T) {
	j := startJudge(t)
	good := startFakeProxy(t)
	e := newEnv(t, j, nil)
	acq := application.NewAcquireService(e.store, e.cfg, e.cfg.JudgeURL, j.certPool(),
		make(chan struct{}, e.cfg.ProbeSlots), fakeGeo{loc: geoip.Location{
			CountryCode: "CN", Country: "中国", Region: "广东省", City: "深圳市",
		}})
	e.importProxy(t, "manual", fmt.Sprintf("http://127.0.0.1:%d", portOf(good.URL)))

	acquired, derr := acq.Acquire(context.Background(), application.AcquireRequest{Count: 1})
	if derr != nil || acquired.Delivered != 1 {
		t.Fatalf("acquire: %v %+v", derr, acquired)
	}
	obs := acquired.Items[0].Observed
	if obs == nil || obs.ExitIP != "203.0.113.7" || obs.CountryCode != "CN" ||
		obs.City != "深圳市" {
		t.Fatalf("observation: %+v", obs)
	}
	if _, derr := e.leases.Release(context.Background(), acquired.Items[0].LeaseID); derr != nil {
		t.Fatalf("release: %v", derr)
	}

	if _, derr := acq.Acquire(context.Background(), application.AcquireRequest{
		Count: 1, Filters: application.Filters{Countries: []string{"US"}},
	}); derr == nil {
		t.Fatal("country filter US should not match CN observation")
	}
	acquired, derr = acq.Acquire(context.Background(), application.AcquireRequest{
		Count: 1, Filters: application.Filters{Countries: []string{"cn"}},
	})
	if derr != nil || acquired.Delivered != 1 {
		t.Fatalf("country filter CN: %v %+v", derr, acquired)
	}
}
