package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"proxypool/internal/application"
	"proxypool/internal/config"
	"proxypool/internal/domain"
)

// TestCoreSyntheticBoundedLoadMetrics measures the core acquire paths with
// only loopback fake supply. It intentionally exercises the service API, not
// the HTTP surface, so every report marks HTTP status as not applicable. Set
// CORE_LOAD_REPORT_PATH to explicitly write an acceptance artifact; ordinary
// test and race runs leave committed reports untouched.
func TestCoreSyntheticBoundedLoadMetrics(t *testing.T) {
	started := time.Now().UTC()
	reports := []coreLoadReport{
		runCoreLoadSharedProjectCD(t),
		runCoreLoadCapacity(t),
		runCoreLoadPartial(t),
		runCoreLoadDeadline(t),
		runCoreLoadCallerCancel(t),
	}
	revision, revisionTime := coreLoadVCSProvenance()
	artifact := coreLoadArtifact{
		Provenance: coreLoadProvenance{
			GeneratedAt:   time.Now().UTC().Format(time.RFC3339Nano),
			StartedAt:     started.Format(time.RFC3339Nano),
			Test:          "TestCoreSyntheticBoundedLoadMetrics",
			GoVersion:     runtime.Version(),
			GOOS:          runtime.GOOS,
			GOARCH:        runtime.GOARCH,
			GOMAXPROCS:    runtime.GOMAXPROCS(0),
			VCSRevision:   revision,
			VCSTime:       revisionTime,
			SourceNote:    os.Getenv("CORE_LOAD_SOURCE_NOTE"),
			Execution:     "service-level application.Acquire calls against fresh temporary SQLite and loopback fake judge/proxy fixtures",
			ExternalCalls: "none; no real vendor, static, cloud, or production endpoint calls",
		},
		Limitations: []string{
			"Local synthetic fixture results only; no Huawei or 5000-stock target performance is measured or implied.",
			"HTTP status is N/A because these tests call the application service directly.",
			"Workloads with n=1 have degenerate P50/P95/P99 values equal to that single sample, not a distribution.",
			"Goroutine high-water values are 1ms periodic samples, not absolute all-instant maxima.",
			"Concurrency is bounded at 6 requests; these are not production throughput or capacity claims.",
		},
		Reports: reports,
	}
	data, err := json.MarshalIndent(artifact, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("CORE_LOAD_METRICS %s", data)
	output := os.Getenv("CORE_LOAD_REPORT_PATH")
	if output == "" {
		t.Log("CORE_LOAD_REPORT_PATH is unset; acceptance artifact not written")
		return
	}
	output, err = filepath.Abs(output)
	if err != nil {
		t.Fatalf("resolve load metrics path: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(output), 0o755); err != nil {
		t.Fatalf("create load metrics directory: %v", err)
	}
	if err := os.WriteFile(output, append(data, '\n'), 0o644); err != nil {
		t.Fatalf("write load metrics: %v", err)
	}
	t.Logf("CORE_LOAD_REPORT_WRITTEN %s", output)
}

type coreLoadProvenance struct {
	GeneratedAt   string `json:"generated_at"`
	StartedAt     string `json:"started_at"`
	Test          string `json:"test"`
	GoVersion     string `json:"go_version"`
	GOOS          string `json:"goos"`
	GOARCH        string `json:"goarch"`
	GOMAXPROCS    int    `json:"gomaxprocs"`
	VCSRevision   string `json:"vcs_revision,omitempty"`
	VCSTime       string `json:"vcs_time,omitempty"`
	SourceNote    string `json:"source_note,omitempty"`
	Execution     string `json:"execution"`
	ExternalCalls string `json:"external_calls"`
}

type coreLoadArtifact struct {
	Provenance  coreLoadProvenance `json:"provenance"`
	Limitations []string           `json:"limitations"`
	Reports     []coreLoadReport   `json:"reports"`
}

func coreLoadVCSProvenance() (string, string) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "", ""
	}
	var revision, revisionTime string
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			revision = setting.Value
		case "vcs.time":
			revisionTime = setting.Value
		}
	}
	return revision, revisionTime
}

type coreLoadWorkload struct {
	Requests       int    `json:"requests"`
	Concurrency    int    `json:"concurrency"`
	CountPerCall   int    `json:"count_per_call"`
	DeadlineMS     int64  `json:"deadline_ms"`
	CDSeconds      int64  `json:"cd_seconds"`
	MaxPoolLeases  int    `json:"max_pool_leases"`
	ParallelProbes int    `json:"parallel_probes_per_request"`
	ProbeSlots     int    `json:"probe_slots"`
	MaxActive      int    `json:"max_active_acquires"`
	Fixture        string `json:"fixture"`
}

type coreLoadLatency struct {
	MinMS float64 `json:"min_ms"`
	P50MS float64 `json:"p50_ms"`
	P95MS float64 `json:"p95_ms"`
	P99MS float64 `json:"p99_ms"`
	MaxMS float64 `json:"max_ms"`
}

type coreLoadGoroutines struct {
	Before      int `json:"before"`
	SampledHigh int `json:"sampled_high_1ms"`
	After       int `json:"after"`
}

type coreLoadCounts struct {
	Sources            int64          `json:"sources"`
	Resources          int64          `json:"resources"`
	EnabledOffers      int64          `json:"enabled_offers"`
	ActiveLeases       int64          `json:"active_leases"`
	ActiveReservations int64          `json:"active_reservations"`
	ProjectCDRows      int64          `json:"project_cd_rows"`
	StickyBindings     int64          `json:"sticky_bindings"`
	Reservations       map[string]int `json:"reservations_by_state,omitempty"`
}

type coreLoadReport struct {
	Scenario     string             `json:"scenario"`
	Scope        string             `json:"scope"`
	HTTPStatus   string             `json:"http_status"`
	Workload     coreLoadWorkload   `json:"workload"`
	Business     map[string]int     `json:"business_status"`
	Delivered    int                `json:"delivered"`
	Missing      int                `json:"missing"`
	LatencyMS    coreLoadLatency    `json:"latency_ms"`
	RuntimeMS    float64            `json:"runtime_ms"`
	Bounded      bool               `json:"bounded"`
	Goroutines   coreLoadGoroutines `json:"goroutines"`
	During       *coreLoadCounts    `json:"during,omitempty"`
	AfterLoad    coreLoadCounts     `json:"after_load"`
	Final        coreLoadCounts     `json:"final"`
	Cleanup      []string           `json:"cleanup"`
	Observations map[string]any     `json:"observations,omitempty"`
}

type coreLoadCall struct {
	result  application.AcquireResult
	derr    *domain.Error
	latency time.Duration
}

type coreLoadGoroutineSampler struct {
	before int
	high   atomic.Int64
	stop   chan struct{}
	done   chan struct{}
}

func startCoreLoadGoroutineSampler() *coreLoadGoroutineSampler {
	s := &coreLoadGoroutineSampler{
		before: runtime.NumGoroutine(),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	s.high.Store(int64(s.before))
	go func() {
		defer close(s.done)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				n := int64(runtime.NumGoroutine())
				for {
					old := s.high.Load()
					if n <= old || s.high.CompareAndSwap(old, n) {
						break
					}
				}
			case <-s.stop:
				return
			}
		}
	}()
	return s
}

func (s *coreLoadGoroutineSampler) finish(t *testing.T) coreLoadGoroutines {
	t.Helper()
	close(s.stop)
	<-s.done
	limit := s.before + 8
	deadline := time.Now().Add(2 * time.Second)
	after := runtime.NumGoroutine()
	for after > limit && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		after = runtime.NumGoroutine()
	}
	if after > limit {
		t.Errorf("goroutines did not settle: before=%d after=%d limit=%d", s.before, after, limit)
	}
	return coreLoadGoroutines{Before: s.before, SampledHigh: int(s.high.Load()), After: after}
}

func coreLoadNewEnv(t *testing.T, parallel, probeSlots, maxActive int) *env {
	t.Helper()
	return newEnv(t, startJudge(t), func(c *config.Config) {
		c.MaxActiveAcquires = maxActive
		c.ParallelPerRequest = parallel
		c.ProbeSlots = probeSlots
		c.ConnectTimeoutMS = 500
		c.ProbeTotalTimeoutMS = 1500
	})
}

func coreLoadImport(t *testing.T, e *env, sourceID string, maxLeases int, urls ...string) {
	t.Helper()
	if err := e.ingest.EnsureSource(context.Background(), sourceID, application.SourceConfig{
		DefaultTransport: domain.TransportHTTP,
		MaxPoolLeases:    maxLeases,
	}, true); err != nil {
		t.Fatal(err)
	}
	result, derr := e.ingest.ImportLines(context.Background(), sourceID,
		strings.Join(urls, "\n")+"\n")
	if derr != nil {
		t.Fatal(derr)
	}
	if result.Added != len(urls) {
		t.Fatalf("import added %d, want %d: %+v", result.Added, len(urls), result)
	}
}

func coreLoadRunConcurrent(t *testing.T, e *env, req application.AcquireRequest,
	concurrency int, timeout time.Duration,
) ([]coreLoadCall, time.Duration) {
	t.Helper()
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	begin := make(chan struct{})
	answers := make(chan coreLoadCall, concurrency)
	var workers sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-begin
			began := time.Now()
			result, derr := e.acquire.Acquire(ctx, req)
			answers <- coreLoadCall{result: result, derr: derr, latency: time.Since(began)}
		}()
	}
	close(begin)
	workers.Wait()
	close(answers)
	calls := make([]coreLoadCall, 0, concurrency)
	for call := range answers {
		calls = append(calls, call)
	}
	return calls, time.Since(start)
}

func coreLoadBusinessStatus(call coreLoadCall) string {
	if call.derr != nil {
		return "ERROR:" + call.derr.Code
	}
	result := call.result
	switch {
	case result.Delivered == result.Requested:
		return "DELIVERED"
	case result.Partial:
		return "PARTIAL:" + result.ShortfallReason
	default:
		return "EMPTY:" + result.ShortfallReason
	}
}

func coreLoadSummarize(calls []coreLoadCall) (map[string]int, int, int, []application.AcquiredItem, []time.Duration) {
	statuses := map[string]int{}
	delivered, missing := 0, 0
	items := make([]application.AcquiredItem, 0)
	latencies := make([]time.Duration, 0, len(calls))
	for _, call := range calls {
		statuses[coreLoadBusinessStatus(call)]++
		delivered += call.result.Delivered
		missing += call.result.Missing
		items = append(items, call.result.Items...)
		latencies = append(latencies, call.latency)
	}
	return statuses, delivered, missing, items, latencies
}

func coreLoadPercentiles(latencies []time.Duration) coreLoadLatency {
	if len(latencies) == 0 {
		return coreLoadLatency{}
	}
	sorted := append([]time.Duration(nil), latencies...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	rank := func(p float64) time.Duration {
		index := int(math.Ceil(p*float64(len(sorted)))) - 1
		if index < 0 {
			index = 0
		}
		return sorted[index]
	}
	ms := func(d time.Duration) float64 {
		return math.Round(float64(d)/float64(time.Millisecond)*1000) / 1000
	}
	return coreLoadLatency{
		MinMS: ms(sorted[0]),
		P50MS: ms(rank(0.50)),
		P95MS: ms(rank(0.95)),
		P99MS: ms(rank(0.99)),
		MaxMS: ms(sorted[len(sorted)-1]),
	}
}

func coreLoadCountsNow(t *testing.T, e *env) coreLoadCounts {
	t.Helper()
	ctx := context.Background()
	stats, err := e.store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	counts := coreLoadCounts{
		Sources:            stats.Sources,
		Resources:          stats.Resources,
		EnabledOffers:      stats.EnabledOffers,
		ActiveLeases:       stats.ActiveLeases,
		ActiveReservations: stats.ActiveReservations,
	}
	if err := e.store.DB().QueryRowContext(ctx,
		"SELECT COUNT(*) FROM project_proxy_cd").Scan(&counts.ProjectCDRows); err != nil {
		t.Fatal(err)
	}
	if err := e.store.DB().QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sticky_bindings").Scan(&counts.StickyBindings); err != nil {
		t.Fatal(err)
	}
	rows, err := e.store.DB().QueryContext(ctx,
		"SELECT state, COUNT(*) FROM reservations GROUP BY state ORDER BY state")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	counts.Reservations = map[string]int{}
	for rows.Next() {
		var state string
		var count int
		if err := rows.Scan(&state, &count); err != nil {
			t.Fatal(err)
		}
		counts.Reservations[state] = count
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return counts
}

func coreLoadRelease(t *testing.T, e *env, items []application.AcquiredItem) int {
	t.Helper()
	for _, item := range items {
		ok, derr := e.leases.Release(context.Background(), item.LeaseID,
			application.DefaultAcquireProject, false)
		if derr != nil || !ok {
			t.Errorf("release lease %s: ok=%v err=%v", item.LeaseID, ok, derr)
		}
	}
	return len(items)
}

func coreLoadClearExpiredCD(t *testing.T, e *env, at time.Time) int64 {
	t.Helper()
	removed, err := e.store.ClearExpiredProjectCooldowns(context.Background(), at)
	if err != nil {
		t.Fatal(err)
	}
	return removed
}

func coreLoadCheckCalls(t *testing.T, calls []coreLoadCall) {
	t.Helper()
	for _, call := range calls {
		if call.derr != nil {
			t.Errorf("service-level acquire returned an error instead of a business result: %v", call.derr)
			continue
		}
		if call.result.Delivered < call.result.Requested && call.result.ShortfallReason == "" {
			t.Errorf("shortfall lacks a business reason: %+v", call.result)
		}
	}
}

func coreLoadCheckUniqueFingerprints(t *testing.T, e *env,
	items []application.AcquiredItem, wantAtMost int,
) {
	t.Helper()
	seen := map[string]bool{}
	for _, item := range items {
		fp := e.acquire.FingerprintOf(item.Conn)
		if seen[fp] {
			t.Errorf("duplicate connection delivered inside shared project scope: %s", fp)
		}
		seen[fp] = true
	}
	if len(seen) > wantAtMost {
		t.Errorf("delivered %d unique connections, capacity ceiling is %d", len(seen), wantAtMost)
	}
}

func coreLoadCheckSingleFingerprint(t *testing.T, e *env,
	items []application.AcquiredItem,
) string {
	t.Helper()
	if len(items) == 0 {
		t.Error("expected at least one delivery")
		return ""
	}
	fp := e.acquire.FingerprintOf(items[0].Conn)
	for _, item := range items[1:] {
		if got := e.acquire.FingerprintOf(item.Conn); got != fp {
			t.Errorf("capacity deliveries used different connections: %s != %s", got, fp)
		}
	}
	return fp
}

func coreLoadRequireBounded(t *testing.T, elapsed, limit time.Duration) bool {
	t.Helper()
	if elapsed > limit {
		t.Errorf("workload exceeded its bounded runtime: elapsed=%s limit=%s", elapsed, limit)
		return false
	}
	return true
}

func coreLoadRequest(sourceID string, count int, deadlineMS, cdSeconds int64) application.AcquireRequest {
	cd := cdSeconds
	return application.AcquireRequest{
		Count: count, ProjectID: application.DefaultAcquireProject,
		ConsumerID: application.DefaultAcquireProject, CDSeconds: &cd,
		DeadlineMS: deadlineMS,
		Filters:    application.Filters{SourceIDs: []string{sourceID}},
	}
}

func runCoreLoadSharedProjectCD(t *testing.T) coreLoadReport {
	t.Helper()
	const (
		concurrency = 6
		proxyCount  = 3
	)
	e := coreLoadNewEnv(t, 2, 4, concurrency)
	urls := make([]string, 0, proxyCount)
	for i := 0; i < proxyCount; i++ {
		urls = append(urls, startFakeProxy(t).URL)
	}
	coreLoadImport(t, e, "core-load-shared-project", 1, urls...)
	req := coreLoadRequest("core-load-shared-project", 1, 5000, 600)
	sampler := startCoreLoadGoroutineSampler()
	calls, elapsed := coreLoadRunConcurrent(t, e, req, concurrency, 8*time.Second)
	g := sampler.finish(t)

	statuses, delivered, missing, items, latencies := coreLoadSummarize(calls)
	coreLoadCheckCalls(t, calls)
	coreLoadCheckUniqueFingerprints(t, e, items, proxyCount)
	if delivered != proxyCount {
		t.Errorf("shared project delivered %d, want one per %d available connections", delivered, proxyCount)
	}
	after := coreLoadCountsNow(t, e)
	if after.ActiveReservations != 0 {
		t.Errorf("shared project retained %d active reservations", after.ActiveReservations)
	}
	if after.ActiveLeases != int64(delivered) {
		t.Errorf("active leases=%d, want one per delivery=%d", after.ActiveLeases, delivered)
	}
	if after.ProjectCDRows != int64(delivered) {
		t.Errorf("project CD rows=%d, want one per delivery=%d", after.ProjectCDRows, delivered)
	}
	released := coreLoadRelease(t, e, items)
	removed := coreLoadClearExpiredCD(t, e, time.Now().Add(11*time.Minute))
	final := coreLoadCountsNow(t, e)
	if final.ActiveLeases != 0 || final.ActiveReservations != 0 || final.ProjectCDRows != 0 {
		t.Errorf("shared project cleanup incomplete: %+v", final)
	}
	return coreLoadReport{
		Scenario:   "shared_project_cd_concurrent",
		Scope:      "service-level, loopback fake judge/proxies, fresh tmpDB",
		HTTPStatus: "N/A (service-level call; no HTTP request)",
		Workload: coreLoadWorkload{
			Requests: concurrency, Concurrency: concurrency, CountPerCall: 1,
			DeadlineMS: req.DeadlineMS, CDSeconds: 600, MaxPoolLeases: 1,
			ParallelProbes: 2, ProbeSlots: 4, MaxActive: concurrency,
			Fixture: fmt.Sprintf("%d distinct fake CONNECT proxies", proxyCount),
		},
		Business: statuses, Delivered: delivered, Missing: missing,
		LatencyMS: coreLoadPercentiles(latencies), RuntimeMS: coreLoadMS(elapsed),
		Bounded:    coreLoadRequireBounded(t, elapsed, 8*time.Second),
		Goroutines: g, AfterLoad: after, Final: final,
		Cleanup: []string{
			fmt.Sprintf("released %d delivered leases", released),
			fmt.Sprintf("removed %d expired project-CD rows at a deterministic future maintenance time", removed),
		},
		Observations: map[string]any{
			"unique_connections": len(items),
			"project_cd_scope":   "shared across all concurrent requests",
		},
	}
}

func runCoreLoadCapacity(t *testing.T) coreLoadReport {
	t.Helper()
	const concurrency = 6
	e := coreLoadNewEnv(t, 2, 2, concurrency)
	coreLoadImport(t, e, "core-load-capacity", 2, startFakeProxy(t).URL)
	req := coreLoadRequest("core-load-capacity", 1, 5000, 0)
	sampler := startCoreLoadGoroutineSampler()
	calls, elapsed := coreLoadRunConcurrent(t, e, req, concurrency, 8*time.Second)
	g := sampler.finish(t)
	statuses, delivered, missing, items, latencies := coreLoadSummarize(calls)
	coreLoadCheckCalls(t, calls)
	coreLoadCheckSingleFingerprint(t, e, items)
	if delivered != 2 {
		t.Errorf("capacity=2 delivered %d connections under %d requests", delivered, concurrency)
	}
	after := coreLoadCountsNow(t, e)
	if after.ActiveReservations != 0 {
		t.Errorf("capacity workload retained %d active reservations", after.ActiveReservations)
	}
	if after.ActiveLeases > 2 {
		t.Errorf("capacity workload retained %d active leases above MaxPoolLeases=2", after.ActiveLeases)
	}
	if after.ProjectCDRows != 0 {
		t.Errorf("cd=0 unexpectedly opened %d project-CD rows", after.ProjectCDRows)
	}
	released := coreLoadRelease(t, e, items)
	final := coreLoadCountsNow(t, e)
	if final.ActiveLeases != 0 || final.ActiveReservations != 0 || final.ProjectCDRows != 0 {
		t.Errorf("capacity cleanup incomplete: %+v", final)
	}
	return coreLoadReport{
		Scenario:   "capacity_limit",
		Scope:      "service-level, loopback fake judge/proxy, fresh tmpDB",
		HTTPStatus: "N/A (service-level call; no HTTP request)",
		Workload: coreLoadWorkload{
			Requests: concurrency, Concurrency: concurrency, CountPerCall: 1,
			DeadlineMS: req.DeadlineMS, CDSeconds: 0, MaxPoolLeases: 2,
			ParallelProbes: 2, ProbeSlots: 2, MaxActive: concurrency,
			Fixture: "one fake CONNECT proxy with capacity 2",
		},
		Business: statuses, Delivered: delivered, Missing: missing,
		LatencyMS: coreLoadPercentiles(latencies), RuntimeMS: coreLoadMS(elapsed),
		Bounded:    coreLoadRequireBounded(t, elapsed, 8*time.Second),
		Goroutines: g, AfterLoad: after, Final: final,
		Cleanup: []string{fmt.Sprintf("released %d active leases", released)},
		Observations: map[string]any{
			"max_pool_leases": 2,
			"no_project_cd":   true,
		},
	}
}

func runCoreLoadPartial(t *testing.T) coreLoadReport {
	t.Helper()
	e := coreLoadNewEnv(t, 2, 2, 2)
	coreLoadImport(t, e, "core-load-partial", 1, startFakeProxy(t).URL)
	req := coreLoadRequest("core-load-partial", 2, 1200, 0)
	sampler := startCoreLoadGoroutineSampler()
	calls, elapsed := coreLoadRunConcurrent(t, e, req, 1, 4*time.Second)
	g := sampler.finish(t)
	statuses, delivered, missing, items, latencies := coreLoadSummarize(calls)
	coreLoadCheckCalls(t, calls)
	if len(calls) != 1 || !calls[0].result.Partial || delivered != 1 ||
		calls[0].result.Missing != 1 ||
		calls[0].result.ShortfallReason != "NO_ELIGIBLE_CANDIDATES" {
		t.Errorf("partial result mismatch: %+v", calls)
	}
	after := coreLoadCountsNow(t, e)
	if after.ActiveReservations != 0 || after.ActiveLeases != 1 || after.ProjectCDRows != 0 {
		t.Errorf("partial after-load state mismatch: %+v", after)
	}
	released := coreLoadRelease(t, e, items)
	final := coreLoadCountsNow(t, e)
	if final.ActiveLeases != 0 || final.ActiveReservations != 0 {
		t.Errorf("partial cleanup incomplete: %+v", final)
	}
	return coreLoadReport{
		Scenario:   "partial_result",
		Scope:      "service-level, loopback fake judge/proxy, fresh tmpDB",
		HTTPStatus: "N/A (service-level call; no HTTP request)",
		Workload: coreLoadWorkload{
			Requests: 1, Concurrency: 1, CountPerCall: 2,
			DeadlineMS: req.DeadlineMS, CDSeconds: 0, MaxPoolLeases: 1,
			ParallelProbes: 2, ProbeSlots: 2, MaxActive: 2,
			Fixture: "one fake CONNECT proxy for a two-connection request",
		},
		Business: statuses, Delivered: delivered, Missing: missing,
		LatencyMS: coreLoadPercentiles(latencies), RuntimeMS: coreLoadMS(elapsed),
		Bounded: elapsed <= 4*time.Second, Goroutines: g, AfterLoad: after, Final: final,
		Cleanup: []string{fmt.Sprintf("released %d delivered lease", released)},
	}
}

type coreLoadBlockingProxy struct {
	srv      *httptest.Server
	hits     atomic.Int32
	firstHit chan struct{}
}

func startCoreLoadBlockingProxy(t *testing.T) *coreLoadBlockingProxy {
	t.Helper()
	p := &coreLoadBlockingProxy{firstHit: make(chan struct{})}
	p.srv = httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		if p.hits.Add(1) == 1 {
			close(p.firstHit)
		}
		<-r.Context().Done()
	}))
	t.Cleanup(p.srv.Close)
	return p
}

func runCoreLoadDeadline(t *testing.T) coreLoadReport {
	t.Helper()
	e := coreLoadNewEnv(t, 1, 1, 2)
	proxy := startCoreLoadBlockingProxy(t)
	coreLoadImport(t, e, "core-load-deadline", 1, proxy.srv.URL)
	req := coreLoadRequest("core-load-deadline", 1, 500, 0)
	sampler := startCoreLoadGoroutineSampler()
	started := time.Now()
	result, derr := e.acquire.Acquire(context.Background(), req)
	elapsed := time.Since(started)
	g := sampler.finish(t)
	call := coreLoadCall{result: result, derr: derr, latency: elapsed}
	statuses, delivered, missing, items, latencies := coreLoadSummarize([]coreLoadCall{call})
	coreLoadCheckCalls(t, []coreLoadCall{call})
	if derr != nil || delivered != 0 ||
		result.ShortfallReason != "VALIDATION_BUDGET_EXHAUSTED" {
		t.Errorf("deadline must return an explained empty business result: result=%+v err=%v", result, derr)
	}
	bounded := coreLoadRequireBounded(t, elapsed, 2*time.Second)
	after := coreLoadCountsNow(t, e)
	if after.ActiveReservations != 0 || after.ActiveLeases != 0 || after.ProjectCDRows != 0 {
		t.Errorf("deadline cleanup incomplete: %+v", after)
	}
	released := coreLoadRelease(t, e, items)
	final := coreLoadCountsNow(t, e)
	return coreLoadReport{
		Scenario:   "deadline",
		Scope:      "service-level, loopback fake judge/proxy, fresh tmpDB",
		HTTPStatus: "N/A (service-level call; no HTTP request)",
		Workload: coreLoadWorkload{
			Requests: 1, Concurrency: 1, CountPerCall: 1,
			DeadlineMS: req.DeadlineMS, CDSeconds: 0, MaxPoolLeases: 1,
			ParallelProbes: 1, ProbeSlots: 1, MaxActive: 2,
			Fixture: "one blocking fake CONNECT proxy",
		},
		Business: statuses, Delivered: delivered, Missing: missing,
		LatencyMS: coreLoadPercentiles(latencies), RuntimeMS: coreLoadMS(elapsed),
		Bounded: bounded, Goroutines: g, AfterLoad: after, Final: final,
		Cleanup:      []string{fmt.Sprintf("released %d delivered leases", released)},
		Observations: map[string]any{"blocking_proxy_hits": proxy.hits.Load()},
	}
}

func runCoreLoadCallerCancel(t *testing.T) coreLoadReport {
	t.Helper()
	e := coreLoadNewEnv(t, 1, 1, 2)
	proxy := startCoreLoadBlockingProxy(t)
	coreLoadImport(t, e, "core-load-cancel", 1, proxy.srv.URL)
	req := coreLoadRequest("core-load-cancel", 1, 3000, 0)
	sampler := startCoreLoadGoroutineSampler()
	ctx, cancel := context.WithCancel(context.Background())
	type answer struct {
		result  application.AcquireResult
		derr    *domain.Error
		latency time.Duration
	}
	answerCh := make(chan answer, 1)
	started := time.Now()
	go func() {
		began := time.Now()
		result, derr := e.acquire.Acquire(ctx, req)
		answerCh <- answer{result: result, derr: derr, latency: time.Since(began)}
	}()
	select {
	case <-proxy.firstHit:
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("blocking proxy was never reached")
	}
	during := coreLoadCountsNow(t, e)
	cancel()
	var got answer
	select {
	case got = <-answerCh:
	case <-time.After(3 * time.Second):
		t.Fatal("canceled acquire did not return within its bounded cleanup window")
	}
	elapsed := time.Since(started)
	bounded := coreLoadRequireBounded(t, elapsed, 2*time.Second)
	g := sampler.finish(t)
	call := coreLoadCall{result: got.result, derr: got.derr, latency: got.latency}
	statuses, delivered, missing, items, latencies := coreLoadSummarize([]coreLoadCall{call})
	coreLoadCheckCalls(t, []coreLoadCall{call})
	if got.derr != nil || delivered != 0 ||
		got.result.ShortfallReason != "VALIDATION_BUDGET_EXHAUSTED" {
		t.Errorf("caller cancellation must return an explained empty business result: result=%+v err=%v",
			got.result, got.derr)
	}
	after := coreLoadCountsNow(t, e)
	if after.ActiveReservations != 0 {
		t.Errorf("caller cancellation retained %d active reservations after return", after.ActiveReservations)
	}
	if after.ActiveLeases != 0 || after.ProjectCDRows != 0 {
		t.Errorf("caller cancellation left leases/CD behind: %+v", after)
	}
	released := coreLoadRelease(t, e, items)
	final := coreLoadCountsNow(t, e)
	return coreLoadReport{
		Scenario:   "caller_cancel",
		Scope:      "service-level, loopback fake judge/proxy, fresh tmpDB",
		HTTPStatus: "N/A (service-level call; no HTTP request)",
		Workload: coreLoadWorkload{
			Requests: 1, Concurrency: 1, CountPerCall: 1,
			DeadlineMS: req.DeadlineMS, CDSeconds: 0, MaxPoolLeases: 1,
			ParallelProbes: 1, ProbeSlots: 1, MaxActive: 2,
			Fixture: "one blocking fake CONNECT proxy, caller cancellation after reservation",
		},
		Business: statuses, Delivered: delivered, Missing: missing,
		LatencyMS: coreLoadPercentiles(latencies), RuntimeMS: coreLoadMS(elapsed),
		Bounded: bounded, Goroutines: g, During: &during,
		AfterLoad: after, Final: final,
		Cleanup: []string{fmt.Sprintf("released %d delivered leases", released)},
		Observations: map[string]any{
			"blocking_proxy_hits":               proxy.hits.Load(),
			"active_reservations_before_cancel": during.ActiveReservations,
		},
	}
}

func coreLoadMS(d time.Duration) float64 {
	return math.Round(float64(d)/float64(time.Millisecond)*1000) / 1000
}
