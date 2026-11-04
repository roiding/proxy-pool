package integration

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"path/filepath"
	"testing"
	"time"

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
		res, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{Count: 1})
		if derr != nil || res.Delivered != 0 {
			t.Fatalf("dead source must answer an empty result, not an error: %v %+v", derr, res)
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
	// selection; the empty result explains itself instead of erroring.
	time.Sleep(20 * time.Millisecond)
	cooling, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{Count: 1})
	if derr != nil || cooling.Delivered != 0 || cooling.ShortfallReason == "" {
		t.Fatalf("open circuit must deliver an explained empty result: %v %+v", derr, cooling)
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

// waitForJob polls a prewarm job until it reaches a terminal state.
func waitForJob(t *testing.T, p *application.PrewarmService, jobID string, timeout time.Duration) domain.Job {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		j, derr := p.GetJob(context.Background(), "", jobID)
		if derr != nil {
			t.Fatalf("get job: %v", derr)
		}
		if j.State != sqlite.JobPending && j.State != sqlite.JobRunning {
			return j
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("prewarm job did not finish in time")
	return domain.Job{}
}

func TestPrewarmJobReachesTarget(t *testing.T) {
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
	job, derr := e.prewarm.StartJob(context.Background(), "", application.PrewarmRequest{
		Count: 2, DeadlineMS: 10000,
	})
	if derr != nil {
		t.Fatalf("start job: %v", derr)
	}
	if job.ID == "" || job.ProjectID != sqlite.DefaultProjectID || job.Target != 2 {
		t.Fatalf("persisted job: %+v", job)
	}
	final := waitForJob(t, e.prewarm, job.ID, 10*time.Second)
	if final.State != sqlite.JobCompleted || final.Ready != 2 || final.StopReason != "target_met" {
		t.Fatalf("prewarm job: %+v", final)
	}
	// Prewarm never delivers: the fixed project CD must be untouched.
	conn := domain.Connection{Transport: domain.TransportHTTP,
		Host: "127.0.0.1", Port: portOf(good1.URL)}
	fp := e.acquire.FingerprintOf(conn)
	blocked, err := e.store.ProjectCooldownBlocked(context.Background(),
		sqlite.DefaultProjectID, time.Now(), []string{fp})
	if err != nil || len(blocked) != 0 {
		t.Fatalf("prewarm must not write project CD: %v %v", err, blocked)
	}
	acquired, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{Count: 1})
	if derr != nil || acquired.Delivered != 1 {
		t.Fatalf("acquire after prewarm: %v %+v", derr, acquired)
	}
}

func TestPrewarmReadyIsCurrentNotCumulative(t *testing.T) {
	j := startJudge(t)
	good := startFakeProxy(t)
	e := newEnv(t, j, nil)
	if err := e.ingest.EnsureSource(context.Background(), "manual",
		application.SourceConfig{DefaultTransport: domain.TransportHTTP, MaxPoolLeases: 1}, true); err != nil {
		t.Fatalf("ensure source: %v", err)
	}
	content := fmt.Sprintf("http://127.0.0.1:%d\nhttp://127.0.0.1:1\n", portOf(good.URL))
	if _, derr := e.ingest.ImportLines(context.Background(), "manual", content); derr != nil {
		t.Fatalf("import: %v", derr)
	}
	job, derr := e.prewarm.StartJob(context.Background(), "", application.PrewarmRequest{
		Count: 2, DeadlineMS: 8000,
	})
	if derr != nil {
		t.Fatalf("start job: %v", derr)
	}
	final := waitForJob(t, e.prewarm, job.ID, 10*time.Second)
	// A connection refusal is not proof of death, and repeated success
	// against the one good proxy must never accumulate into Ready=2.
	if final.State != sqlite.JobPartial || final.Ready != 1 {
		t.Fatalf("ready must be the current valid count: %+v", final)
	}
}

func TestPrewarmJobCancelStopsRun(t *testing.T) {
	j := startJudge(t)
	hang := startHangingProxy(t)
	e := newEnv(t, j, nil)
	if err := e.ingest.EnsureSource(context.Background(), "manual",
		application.SourceConfig{DefaultTransport: domain.TransportHTTP, MaxPoolLeases: 1}, true); err != nil {
		t.Fatalf("ensure source: %v", err)
	}
	if _, derr := e.ingest.ImportLines(context.Background(), "manual",
		fmt.Sprintf("http://127.0.0.1:%d\n", portOf(hang))); derr != nil {
		t.Fatalf("import: %v", derr)
	}
	job, derr := e.prewarm.StartJob(context.Background(), "", application.PrewarmRequest{
		Count: 1, DeadlineMS: 9000,
	})
	if derr != nil {
		t.Fatalf("start job: %v", derr)
	}
	if derr := e.prewarm.CancelJob(context.Background(), job.ID); derr != nil {
		t.Fatalf("cancel job: %v", derr)
	}
	final := waitForJob(t, e.prewarm, job.ID, 5*time.Second)
	if final.State != sqlite.JobCancelled || final.StopReason != sqlite.JobStopCanceled {
		t.Fatalf("cancelled job: %+v", final)
	}
}

func TestPrewarmRestartMarksJobsInterrupted(t *testing.T) {
	j := startJudge(t)
	e := newEnv(t, j, nil)
	job := domain.Job{ID: domain.NewID("warm_"), ProjectID: sqlite.DefaultProjectID,
		Kind: application.PrewarmJobKind, Target: 1}
	if err := e.store.CreateJob(context.Background(), job, time.Now()); err != nil {
		t.Fatalf("create orphan job: %v", err)
	}
	n, err := e.prewarm.RecoverInterrupted(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("recover: %d %v", n, err)
	}
	got, derr := e.prewarm.GetJob(context.Background(), sqlite.DefaultProjectID, job.ID)
	if derr != nil || got.State != sqlite.JobInterrupted ||
		got.StopReason != "service_restart" {
		t.Fatalf("interrupted job: %v %+v %v", derr, got, derr)
	}
	// Ownership: another project cannot read the job.
	if _, derr := e.prewarm.GetJob(context.Background(), "proj-other", job.ID); derr == nil {
		t.Fatal("cross-project job read must fail")
	}
}

// startHangingProxy accepts TCP connections but never answers the CONNECT
// request, so a probe only finishes when its context is cancelled.
func startHangingProxy(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	done := make(chan struct{})
	t.Cleanup(func() {
		close(done)
		ln.Close()
	})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				select {
				case <-done:
				case <-time.After(30 * time.Second):
				}
				c.Close()
			}(conn)
		}
	}()
	return ln.Addr().String()
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
	if _, derr := e.leases.Release(context.Background(), acquired.Items[0].LeaseID, "master", true); derr != nil {
		t.Fatalf("release: %v", derr)
	}

	mismatch, derr := acq.Acquire(context.Background(), application.AcquireRequest{
		Count: 1, Filters: application.Filters{Countries: []string{"US"}},
	})
	if derr != nil || mismatch.Delivered != 0 {
		t.Fatalf("country mismatch must be an empty result, not an error: %v %+v", derr, mismatch)
	}
	if mismatch.ShortfallReason == "" {
		t.Fatalf("shortfall reason: %q", mismatch.ShortfallReason)
	}
	acquired, derr = acq.Acquire(context.Background(), application.AcquireRequest{
		Count: 1, Filters: application.Filters{Countries: []string{"cn"}},
	})
	if derr != nil || acquired.Delivered != 1 {
		t.Fatalf("country filter CN: %v %+v", derr, acquired)
	}
}
