package application

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"proxypool/internal/config"
	"proxypool/internal/domain"
	"proxypool/internal/storage/sqlite"
)

type poolMaintenanceHarness struct {
	ingest *IngestService
	warm   *PrewarmService
	maint  *PoolMaintenanceService
	now    time.Time
}

func newPoolMaintenanceHarness(t *testing.T) *poolMaintenanceHarness {
	t.Helper()
	ingest := newTestIngest(t)
	ctx := context.Background()
	if err := ingest.store.EnsurePoolMaintenanceSchema(ctx); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	ingest.Now = func() time.Time { return now }
	cfg := config.Default()
	cfg.IdentitySecret = ingest.secret
	warm := NewPrewarmService(ingest.store, cfg, "", nil,
		make(chan struct{}, 4), nil)
	warm.StaticSupply = ingest
	warm.Now = func() time.Time { return now }
	maint := NewPoolMaintenanceService(ingest.store, warm)
	maint.Now = func() time.Time { return now }
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = warm.Shutdown(shutdownCtx)
	})
	return &poolMaintenanceHarness{ingest: ingest, warm: warm, maint: maint, now: now}
}

func ensurePoolMaintenanceProject(t *testing.T, h *poolMaintenanceHarness,
	id string, enabled bool, target int, allowed []string) domain.Project {
	t.Helper()
	ctx := context.Background()
	if err := h.ingest.store.EnsureProject(ctx, domain.Project{
		ID: id, Enabled: enabled, AllowedSourceIDs: allowed,
	}, h.now); err != nil {
		t.Fatal(err)
	}
	if _, err := h.ingest.store.DB().ExecContext(ctx, `
		UPDATE projects
		SET auto_prewarm_target = ?, config_version = config_version + 1, updated_at = ?
		WHERE project_id = ?`, target, h.now.UTC().Format("2006-01-02T15:04:05.000000000Z07:00"),
		id); err != nil {
		t.Fatal(err)
	}
	project, err := h.ingest.store.GetProject(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return project
}

func poolMaintenanceJobs(t *testing.T, h *poolMaintenanceHarness,
	projectID string) []domain.Job {
	t.Helper()
	jobs, err := h.ingest.store.ListJobs(context.Background(), projectID, 100)
	if err != nil {
		t.Fatal(err)
	}
	return jobs
}

func TestPoolMaintenanceTargetMetDefersWithoutCreatingJob(t *testing.T) {
	h := newPoolMaintenanceHarness(t)
	ctx := context.Background()
	project := ensurePoolMaintenanceProject(t, h, "ready-project", true, 1, nil)

	list := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintln(w, "http://127.0.0.1:3128")
	}))
	t.Cleanup(list.Close)
	src, derr := h.ingest.CreateSource(ctx, "ready-list", SourceConfig{
		Static: &StaticConfig{URL: list.URL},
	}, true)
	if derr != nil {
		t.Fatal(derr)
	}
	if _, derr := h.ingest.RefreshStaticSource(ctx, src.ID); derr != nil {
		t.Fatal(derr)
	}
	candidates, _, fresh := h.ingest.CachedStaticCandidates(src.ID)
	if !fresh || len(candidates) != 1 {
		t.Fatal("controlled static fixture did not produce one candidate")
	}
	_, params, err := h.maint.prewarmRequest(project)
	if err != nil {
		t.Fatal(err)
	}
	scopeHash := domain.EvidenceScopeHash(params.Validation)
	if err := h.ingest.RecordStaticEvidence(ctx, candidates[0], scopeHash,
		domain.Evidence{Outcome: "PASS", CompletedAt: h.now}, nil); err != nil {
		t.Fatal(err)
	}

	if err := h.maint.RunDue(ctx); err != nil {
		t.Fatal(err)
	}
	if jobs := poolMaintenanceJobs(t, h, project.ID); len(jobs) != 0 {
		t.Fatalf("target-met project created jobs: %+v", jobs)
	}
	due, err := h.ingest.store.ListPoolMaintenanceDue(ctx, h.now, 10)
	if err != nil || len(due) != 0 {
		t.Fatalf("target-met project was not deferred: %+v %v", due, err)
	}
	due, err = h.ingest.store.ListPoolMaintenanceDue(ctx,
		h.now.Add(PoolMaintenanceBackoff), 10)
	if err != nil || len(due) != 1 {
		t.Fatalf("deferred project did not become due: %+v %v", due, err)
	}
	if err := h.maint.RunDue(ctx); err != nil {
		t.Fatal(err)
	}
	if jobs := poolMaintenanceJobs(t, h, project.ID); len(jobs) != 0 {
		t.Fatalf("repeated target-met scan created jobs: %+v", jobs)
	}
}

func TestPoolMaintenanceTargetMetUsesConfiguredBackoff(t *testing.T) {
	h := newPoolMaintenanceHarness(t)
	h.maint.TargetMetBackoff = 5 * time.Minute
	ctx := context.Background()
	project := ensurePoolMaintenanceProject(t, h, "custom-backoff-project", true, 1, nil)
	list := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintln(w, "http://127.0.0.1:3128")
	}))
	t.Cleanup(list.Close)
	src, derr := h.ingest.CreateSource(ctx, "custom-backoff-list", SourceConfig{
		Static: &StaticConfig{URL: list.URL},
	}, true)
	if derr != nil {
		t.Fatal(derr)
	}
	if _, derr := h.ingest.RefreshStaticSource(ctx, src.ID); derr != nil {
		t.Fatal(derr)
	}
	candidates, _, fresh := h.ingest.CachedStaticCandidates(src.ID)
	if !fresh || len(candidates) != 1 {
		t.Fatal("controlled static fixture did not produce one candidate")
	}
	_, params, err := h.maint.prewarmRequest(project)
	if err != nil {
		t.Fatal(err)
	}
	scopeHash := domain.EvidenceScopeHash(params.Validation)
	if err := h.ingest.RecordStaticEvidence(ctx, candidates[0], scopeHash,
		domain.Evidence{Outcome: "PASS", CompletedAt: h.now}, nil); err != nil {
		t.Fatal(err)
	}
	if err := h.maint.RunDue(ctx); err != nil {
		t.Fatal(err)
	}
	due, err := h.ingest.store.ListPoolMaintenanceDue(ctx, h.now.Add(PoolMaintenanceBackoff), 10)
	if err != nil || len(due) != 0 {
		t.Fatalf("project should still be deferred at 30s: %+v %v", due, err)
	}
	due, err = h.ingest.store.ListPoolMaintenanceDue(ctx, h.now.Add(5*time.Minute), 10)
	if err != nil || len(due) != 1 {
		t.Fatalf("project should become due at 5m: %+v %v", due, err)
	}
}

func TestPoolMaintenanceGapStartsSharedPrewarmAndDeduplicates(t *testing.T) {
	h := newPoolMaintenanceHarness(t)
	ctx := context.Background()
	project := ensurePoolMaintenanceProject(t, h, "gap-project", true, 2, nil)
	src, derr := h.ingest.CreateSource(ctx, "gap-upload", SourceConfig{
		MaxPoolLeases: 1,
	}, true)
	if derr != nil {
		t.Fatal(derr)
	}
	if _, derr := h.ingest.ImportLines(ctx, src.ID,
		"http://127.0.0.1:3128\n"); derr != nil {
		t.Fatal(derr)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- h.maint.RunDue(ctx)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	jobs := poolMaintenanceJobs(t, h, project.ID)
	if len(jobs) != 1 || jobs[0].Kind != PrewarmJobKind || jobs[0].Target != 2 {
		t.Fatalf("concurrent gap scans produced jobs: %+v", jobs)
	}

	// A fresh service object sees the same durable next_run_at, so process
	// restart cannot immediately enqueue a second job for this project.
	restarted := NewPoolMaintenanceService(h.ingest.store, h.warm)
	restarted.Now = func() time.Time { return h.now.Add(time.Second) }
	if err := restarted.RunDue(ctx); err != nil {
		t.Fatal(err)
	}
	if jobs := poolMaintenanceJobs(t, h, project.ID); len(jobs) != 1 {
		t.Fatalf("restart duplicated prewarm: %+v", jobs)
	}
}

func TestPoolMaintenanceNewSourceJoinsNextCycle(t *testing.T) {
	h := newPoolMaintenanceHarness(t)
	ctx := context.Background()
	project := ensurePoolMaintenanceProject(t, h, "new-source-project", true, 1, nil)
	if err := h.maint.RunDue(ctx); err != nil {
		t.Fatal(err)
	}
	if jobs := poolMaintenanceJobs(t, h, project.ID); len(jobs) != 0 {
		t.Fatalf("empty inventory created jobs: %+v", jobs)
	}

	src, derr := h.ingest.CreateSource(ctx, "new-source-upload", SourceConfig{
		MaxPoolLeases: 1,
	}, true)
	if derr != nil {
		t.Fatal(derr)
	}
	if _, derr := h.ingest.ImportLines(ctx, src.ID,
		"http://127.0.0.1:3128\n"); derr != nil {
		t.Fatal(derr)
	}

	next := h.now.Add(PoolMaintenanceBackoff)
	h.ingest.Now = func() time.Time { return next }
	h.warm.Now = h.ingest.Now
	h.maint.Now = h.ingest.Now
	if err := h.maint.RunDue(ctx); err != nil {
		t.Fatal(err)
	}
	jobs := poolMaintenanceJobs(t, h, project.ID)
	if len(jobs) != 1 || jobs[0].State != sqlite.JobPending && jobs[0].State != sqlite.JobRunning {
		t.Fatalf("new upload did not resume automatic prewarm: %+v", jobs)
	}
}

func TestPoolMaintenanceDisableAndZeroTargetCancelOnlyAutomaticJobs(t *testing.T) {
	h := newPoolMaintenanceHarness(t)
	ctx := context.Background()
	autoProject := ensurePoolMaintenanceProject(t, h, "auto-project", true, 1, nil)
	claimed, err := h.ingest.store.ClaimPoolMaintenance(ctx, autoProject.ID,
		autoProject.ConfigVersion, h.now, PoolMaintenanceBackoff)
	if err != nil || !claimed {
		t.Fatalf("claim auto project = %v %v", claimed, err)
	}
	autoJob := domain.Job{ID: "auto-job", ProjectID: autoProject.ID, Kind: PrewarmJobKind}
	if err := h.ingest.store.CreateJob(ctx, autoJob, h.now); err != nil {
		t.Fatal(err)
	}
	if err := h.ingest.store.RecordPoolMaintenanceJob(ctx, autoProject.ID,
		autoJob.ID, autoProject.ConfigVersion, h.now); err != nil {
		t.Fatal(err)
	}

	manualProject := ensurePoolMaintenanceProject(t, h, "manual-project", true, 1, nil)
	manualJob := domain.Job{ID: "manual-job", ProjectID: manualProject.ID, Kind: PrewarmJobKind}
	if err := h.ingest.store.CreateJob(ctx, manualJob, h.now); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{autoProject.ID, manualProject.ID} {
		if _, err := h.ingest.store.DB().ExecContext(ctx, `
			UPDATE projects SET auto_prewarm_target = 0 WHERE project_id = ?`, id); err != nil {
			t.Fatal(err)
		}
	}

	if err := h.maint.RunDue(ctx); err != nil {
		t.Fatal(err)
	}
	automatic, err := h.ingest.store.GetJob(ctx, autoJob.ID)
	if err != nil || automatic.State != sqlite.JobCancelled {
		t.Fatalf("automatic job was not cancelled: %+v %v", automatic, err)
	}
	manual, err := h.ingest.store.GetJob(ctx, manualJob.ID)
	if err != nil || manual.State != sqlite.JobPending {
		t.Fatalf("manual job was cancelled: %+v %v", manual, err)
	}
	if jobs := poolMaintenanceJobs(t, h, autoProject.ID); len(jobs) != 1 {
		t.Fatalf("zero-target project started another job: %+v", jobs)
	}
}

func TestPoolMaintenanceConfigChangeCancelsOldAutomaticJob(t *testing.T) {
	h := newPoolMaintenanceHarness(t)
	ctx := context.Background()
	project := ensurePoolMaintenanceProject(t, h, "changed-auto", true, 1, nil)
	claimed, err := h.ingest.store.ClaimPoolMaintenance(ctx, project.ID,
		project.ConfigVersion, h.now, PoolMaintenanceBackoff)
	if err != nil || !claimed {
		t.Fatalf("claim = %v %v", claimed, err)
	}
	job := domain.Job{ID: "changed-auto-job", ProjectID: project.ID, Kind: PrewarmJobKind}
	if err := h.ingest.store.CreateJob(ctx, job, h.now); err != nil {
		t.Fatal(err)
	}
	if err := h.ingest.store.RecordPoolMaintenanceJob(ctx, project.ID,
		job.ID, project.ConfigVersion, h.now); err != nil {
		t.Fatal(err)
	}
	project.Defaults.AutoPrewarmTarget = 2
	if _, err := h.ingest.store.UpdateProject(ctx, project,
		project.ConfigVersion, h.now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	if err := h.maint.RunDue(ctx); err != nil {
		t.Fatal(err)
	}
	current, err := h.ingest.store.GetJob(ctx, job.ID)
	if err != nil || current.State != sqlite.JobCancelled {
		t.Fatalf("old automatic job survived config change: %+v %v", current, err)
	}
	if jobs := poolMaintenanceJobs(t, h, project.ID); len(jobs) != 1 {
		t.Fatalf("config change started another job during backoff: %+v", jobs)
	}
}

func TestPoolMaintenanceRespectsEachProjectAllowlist(t *testing.T) {
	h := newPoolMaintenanceHarness(t)
	ctx := context.Background()
	for i, id := range []string{"source-a", "source-b"} {
		src, derr := h.ingest.CreateSource(ctx, id, SourceConfig{
			MaxPoolLeases: 1,
		}, true)
		if derr != nil {
			t.Fatal(derr)
		}
		if _, derr := h.ingest.ImportLines(ctx, src.ID,
			fmt.Sprintf("http://127.0.0.1:%d\n", 32000+i)); derr != nil {
			t.Fatal(derr)
		}
	}
	first := ensurePoolMaintenanceProject(t, h, "scope-a", true, 1, []string{"source-a"})
	second := ensurePoolMaintenanceProject(t, h, "scope-b", true, 1, []string{"source-b"})

	if err := h.maint.RunDue(ctx); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{first.ID: "source-a", second.ID: "source-b"}
	for projectID, sourceID := range want {
		jobs := poolMaintenanceJobs(t, h, projectID)
		if len(jobs) != 1 {
			t.Fatalf("project %s jobs = %+v", projectID, jobs)
		}
		var params prewarmParams
		job := jobs[0]
		if err := json.Unmarshal([]byte(job.ParamsJSON), &params); err != nil {
			t.Fatal(err)
		}
		if len(params.Filters.SourceIDs) != 1 ||
			params.Filters.SourceIDs[0] != sourceID {
			t.Fatalf("project %s prewarm scope = %+v, want %s",
				projectID, params.Filters.SourceIDs, sourceID)
		}
	}
}

func TestPoolMaintenancePrivateSourceOnlyMaintainsOwningProject(t *testing.T) {
	h := newPoolMaintenanceHarness(t)
	ctx := context.Background()
	owner := ensurePoolMaintenanceProject(t, h, "private-owner", true, 1, nil)
	outsider := ensurePoolMaintenanceProject(t, h, "private-outsider", true, 1, nil)

	var hits atomic.Int64
	list := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = fmt.Fprintln(w, "http://127.0.0.1:3128")
	}))
	t.Cleanup(list.Close)
	src, derr := h.ingest.CreateSource(ctx, "private-auto-list", SourceConfig{
		Static: &StaticConfig{URL: list.URL},
		Access: &SourceAccessConfig{ProjectIDs: []string{owner.ID}},
	}, true)
	if derr != nil {
		t.Fatal(derr)
	}
	if _, derr := h.ingest.RefreshStaticSource(ctx, src.ID); derr != nil {
		t.Fatal(derr)
	}
	candidates, _, fresh := h.ingest.CachedStaticCandidates(src.ID)
	if !fresh || len(candidates) != 1 {
		t.Fatal("private static fixture did not produce one candidate")
	}
	_, params, err := h.maint.prewarmRequest(owner)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.ingest.RecordStaticEvidence(ctx, candidates[0],
		domain.EvidenceScopeHash(params.Validation),
		domain.Evidence{Outcome: "PASS", CompletedAt: h.now}, nil); err != nil {
		t.Fatal(err)
	}

	if err := h.maint.RunDue(ctx); err != nil {
		t.Fatal(err)
	}
	if jobs := poolMaintenanceJobs(t, h, owner.ID); len(jobs) != 0 {
		t.Fatalf("private source did not satisfy its owner's target: %+v", jobs)
	}
	if jobs := poolMaintenanceJobs(t, h, outsider.ID); len(jobs) != 0 {
		t.Fatalf("private source created work for an unauthorized project: %+v", jobs)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("private source triggered %d list downloads, want the one fixture refresh", got)
	}
}

func TestPoolMaintenanceExhaustedInventoryDoesNotCreateJobsAcrossRestart(t *testing.T) {
	h := newPoolMaintenanceHarness(t)
	ctx := context.Background()
	project := ensurePoolMaintenanceProject(t, h, "exhausted-project", true, 2, nil)
	src, derr := h.ingest.CreateSource(ctx, "exhausted-upload", SourceConfig{
		MaxPoolLeases: 1,
	}, true)
	if derr != nil {
		t.Fatal(derr)
	}
	if _, derr := h.ingest.ImportLines(ctx, src.ID,
		"http://127.0.0.1:3128\n"); derr != nil {
		t.Fatal(derr)
	}
	_, params, err := h.maint.prewarmRequest(project)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := h.warm.currentScope(ctx, project.ID, params)
	if err != nil || scope == nil {
		t.Fatalf("current scope = %+v, %v", scope, err)
	}
	page, err := h.warm.probePage(ctx, project.ID, params, *scope,
		scope.ScopeHash, nil, nil)
	if err != nil || len(page) != 1 {
		t.Fatalf("probe fixture = %d, %v", len(page), err)
	}
	if err := h.warm.putCandidateEvidence(ctx, page[0], scope.ScopeHash,
		domain.Evidence{Outcome: "PASS", CompletedAt: h.now}); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 4; i++ {
		next := h.now.Add(time.Duration(i+1) * PoolMaintenanceBackoff)
		h.ingest.Now = func() time.Time { return next }
		h.warm.Now = h.ingest.Now
		restarted := NewPoolMaintenanceService(h.ingest.store, h.warm)
		restarted.Now = h.ingest.Now
		if err := restarted.RunDue(ctx); err != nil {
			t.Fatal(err)
		}
		if jobs := poolMaintenanceJobs(t, h, project.ID); len(jobs) != 0 {
			t.Fatalf("exhausted inventory created jobs on scan %d: %+v", i, jobs)
		}
	}
	stats, err := h.ingest.store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.AttemptPass != 0 || stats.AttemptFail != 0 {
		t.Fatalf("exhaustion check performed probes: %+v", stats)
	}
}

func TestPoolMaintenanceNewlyEnabledPermittedSourceResumes(t *testing.T) {
	h := newPoolMaintenanceHarness(t)
	ctx := context.Background()
	project := ensurePoolMaintenanceProject(t, h, "enable-project", true, 1, nil)
	src, derr := h.ingest.CreateSource(ctx, "enable-upload", SourceConfig{
		MaxPoolLeases: 1,
	}, true)
	if derr != nil {
		t.Fatal(derr)
	}
	if _, derr := h.ingest.ImportLines(ctx, src.ID,
		"http://127.0.0.1:3128\n"); derr != nil {
		t.Fatal(derr)
	}
	src, derr = h.ingest.SetSourceEnabled(ctx, src.ID, false, src.ConfigVersion)
	if derr != nil {
		t.Fatal(derr)
	}
	if err := h.maint.RunDue(ctx); err != nil {
		t.Fatal(err)
	}
	if jobs := poolMaintenanceJobs(t, h, project.ID); len(jobs) != 0 {
		t.Fatalf("disabled source created work: %+v", jobs)
	}

	next := h.now.Add(PoolMaintenanceBackoff)
	h.ingest.Now = func() time.Time { return next }
	h.warm.Now = h.ingest.Now
	h.maint.Now = h.ingest.Now
	if _, derr := h.ingest.SetSourceEnabled(ctx, src.ID, true,
		src.ConfigVersion); derr != nil {
		t.Fatal(derr)
	}
	if err := h.maint.RunDue(ctx); err != nil {
		t.Fatal(err)
	}
	if jobs := poolMaintenanceJobs(t, h, project.ID); len(jobs) != 1 {
		t.Fatalf("newly enabled permitted source did not resume: %+v", jobs)
	}
}

func TestPoolMaintenanceStaticCacheExpiryHonorsFetchWindow(t *testing.T) {
	h := newPoolMaintenanceHarness(t)
	ctx := context.Background()
	project := ensurePoolMaintenanceProject(t, h, "cache-project", true, 2, nil)
	var hits atomic.Int64
	list := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = fmt.Fprintln(w, "http://127.0.0.1:3128")
	}))
	t.Cleanup(list.Close)
	src, derr := h.ingest.CreateSource(ctx, "cache-list", SourceConfig{
		Static: &StaticConfig{URL: list.URL},
	}, true)
	if derr != nil {
		t.Fatal(derr)
	}
	if _, derr := h.ingest.RefreshStaticSource(ctx, src.ID); derr != nil {
		t.Fatal(derr)
	}
	candidates, _, fresh := h.ingest.CachedStaticCandidates(src.ID)
	if !fresh || len(candidates) != 1 {
		t.Fatal("static fixture did not produce one candidate")
	}
	_, params, err := h.maint.prewarmRequest(project)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.ingest.RecordStaticEvidence(ctx, candidates[0],
		domain.EvidenceScopeHash(params.Validation),
		domain.Evidence{Outcome: "PASS", CompletedAt: h.now}, nil); err != nil {
		t.Fatal(err)
	}
	if err := h.maint.RunDue(ctx); err != nil {
		t.Fatal(err)
	}
	if jobs := poolMaintenanceJobs(t, h, project.ID); len(jobs) != 0 {
		t.Fatalf("fresh exhausted cache created work: %+v", jobs)
	}

	expired := h.now.Add(DefaultStaticCacheTTL + time.Second)
	futureFetch := expired.Add(time.Hour)
	if _, err := h.ingest.store.DB().ExecContext(ctx, `
		UPDATE source_fetch_state SET next_fetch_at = ? WHERE source_id = ?`,
		formatPoolMaintenanceTime(futureFetch), src.ID); err != nil {
		t.Fatal(err)
	}
	h.ingest.Now = func() time.Time { return expired }
	h.warm.Now = h.ingest.Now
	h.maint.Now = h.ingest.Now
	if err := h.maint.RunDue(ctx); err != nil {
		t.Fatal(err)
	}
	if jobs := poolMaintenanceJobs(t, h, project.ID); len(jobs) != 0 {
		t.Fatalf("expired cache bypassed fetch backoff: %+v", jobs)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("fetch-window defer downloaded %d times, want 1", got)
	}

	allowed := expired.Add(PoolMaintenanceBackoff + time.Second)
	if _, err := h.ingest.store.DB().ExecContext(ctx, `
		UPDATE source_fetch_state SET next_fetch_at = ? WHERE source_id = ?`,
		formatPoolMaintenanceTime(allowed.Add(-time.Second)), src.ID); err != nil {
		t.Fatal(err)
	}
	h.ingest.Now = func() time.Time { return allowed }
	h.warm.Now = h.ingest.Now
	h.maint.Now = h.ingest.Now
	if err := h.maint.RunDue(ctx); err != nil {
		t.Fatal(err)
	}
	jobs := poolMaintenanceJobs(t, h, project.ID)
	if len(jobs) != 1 {
		t.Fatalf("expired cache with an open fetch window did not resume: %+v", jobs)
	}
	waitForPoolMaintenanceJob(t, h, jobs[0].ID)
	if got := hits.Load(); got != 2 {
		t.Fatalf("resumed cache refresh downloads = %d, want 2", got)
	}
}

func TestPoolMaintenanceAccountSecretAndSharedWindowGateRefresh(t *testing.T) {
	h := newPoolMaintenanceHarness(t)
	ctx := context.Background()
	project := ensurePoolMaintenanceProject(t, h, "account-project", true, 2, nil)
	if err := h.ingest.ConfigureSourceSecrets(sourceSecretTestKey); err != nil {
		t.Fatal(err)
	}
	cfg := accountTestConfig()
	cfg.Access = &SourceAccessConfig{ProjectIDs: []string{project.ID}}
	src, derr := h.ingest.CreateSource(ctx, "automatic-account", cfg, false)
	if derr != nil {
		t.Fatal(derr)
	}
	if err := h.maint.RunDue(ctx); err != nil {
		t.Fatal(err)
	}
	if jobs := poolMaintenanceJobs(t, h, project.ID); len(jobs) != 0 {
		t.Fatalf("account without a key created work: %+v", jobs)
	}

	status, derr := h.ingest.SetSourceSecret(ctx, src.ID, src.ConfigVersion,
		"automatic-account-key")
	if derr != nil {
		t.Fatal(derr)
	}
	if _, derr := h.ingest.SetSourceEnabled(ctx, src.ID, true,
		status.ConfigVersion); derr != nil {
		t.Fatal(derr)
	}
	src, derr = h.ingest.GetSource(ctx, src.ID)
	if derr != nil {
		t.Fatal(derr)
	}
	key := domain.CredentialVersion(h.ingest.secret, "webshare-account-api",
		"automatic-account-key")
	if err := h.ingest.store.ClaimAccountFetch(ctx, key, h.now, time.Minute); err != nil {
		t.Fatal(err)
	}
	next := h.now.Add(PoolMaintenanceBackoff)
	h.ingest.Now = func() time.Time { return next }
	h.warm.Now = h.ingest.Now
	h.maint.Now = h.ingest.Now
	if err := h.maint.RunDue(ctx); err != nil {
		t.Fatal(err)
	}
	if jobs := poolMaintenanceJobs(t, h, project.ID); len(jobs) != 0 {
		t.Fatalf("shared account fetch window created work: %+v", jobs)
	}

	next = h.now.Add(time.Minute + time.Second)
	h.ingest.Now = func() time.Time { return next }
	h.warm.Now = h.ingest.Now
	h.maint.Now = h.ingest.Now
	if err := h.maint.RunDue(ctx); err != nil {
		t.Fatal(err)
	}
	if jobs := poolMaintenanceJobs(t, h, project.ID); len(jobs) != 1 {
		t.Fatalf("account with a key and open window did not resume: %+v", jobs)
	}
}

func TestPoolMaintenanceStaticFetchInProgressDoesNotCreateJob(t *testing.T) {
	h := newPoolMaintenanceHarness(t)
	ctx := context.Background()
	project := ensurePoolMaintenanceProject(t, h, "inflight-project", true, 2, nil)
	list := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintln(w, "http://127.0.0.1:3128")
	}))
	t.Cleanup(list.Close)
	src, derr := h.ingest.CreateSource(ctx, "inflight-list", SourceConfig{
		Static: &StaticConfig{URL: list.URL},
	}, true)
	if derr != nil {
		t.Fatal(derr)
	}
	flight, leader, capErr := h.ingest.static.beginRefresh(src.ID)
	if capErr != nil || !leader {
		t.Fatalf("in-flight fixture = %v %v", leader, capErr)
	}
	if err := h.maint.RunDue(ctx); err != nil {
		t.Fatal(err)
	}
	if jobs := poolMaintenanceJobs(t, h, project.ID); len(jobs) != 0 {
		t.Fatalf("in-flight refresh created duplicate work: %+v", jobs)
	}
	h.ingest.static.finishRefresh(src.ID, flight, nil, false, h.now)
}

func TestPoolMaintenanceBlacklistedFreshCacheDoesNotRefetch(t *testing.T) {
	h := newPoolMaintenanceHarness(t)
	ctx := context.Background()
	project := ensurePoolMaintenanceProject(t, h, "blacklist-project", true, 2, nil)
	var hits atomic.Int64
	list := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = fmt.Fprintln(w, "http://127.0.0.1:3128")
	}))
	t.Cleanup(list.Close)
	src, derr := h.ingest.CreateSource(ctx, "blacklist-list", SourceConfig{
		Static: &StaticConfig{URL: list.URL},
	}, true)
	if derr != nil {
		t.Fatal(derr)
	}
	if _, derr := h.ingest.RefreshStaticSource(ctx, src.ID); derr != nil {
		t.Fatal(derr)
	}
	candidates, _, fresh := h.ingest.CachedStaticCandidates(src.ID)
	if !fresh || len(candidates) != 1 {
		t.Fatal("static fixture did not produce one candidate")
	}
	if err := h.ingest.MarkStaticUnavailable(ctx, candidates[0], project.ID); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		next := h.now.Add(time.Duration(i+1) * PoolMaintenanceBackoff)
		h.ingest.Now = func() time.Time { return next }
		h.warm.Now = h.ingest.Now
		h.maint.Now = h.ingest.Now
		if err := h.maint.RunDue(ctx); err != nil {
			t.Fatal(err)
		}
		if jobs := poolMaintenanceJobs(t, h, project.ID); len(jobs) != 0 {
			t.Fatalf("fresh blacklisted cache created work: %+v", jobs)
		}
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("fresh blacklisted cache downloads = %d, want 1", got)
	}
}

func TestPoolMaintenanceContinuesUntriedCandidatesAfterBudgetStop(t *testing.T) {
	h := newPoolMaintenanceHarness(t)
	ctx := context.Background()
	project := ensurePoolMaintenanceProject(t, h, "budget-project", true, 2, nil)
	src, derr := h.ingest.CreateSource(ctx, "budget-upload", SourceConfig{
		MaxPoolLeases: 1,
	}, true)
	if derr != nil {
		t.Fatal(derr)
	}
	if _, derr := h.ingest.ImportLines(ctx, src.ID,
		"http://127.0.0.1:3128\nhttp://127.0.0.1:3129\n"); derr != nil {
		t.Fatal(derr)
	}
	_, params, err := h.maint.prewarmRequest(project)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := h.warm.currentScope(ctx, project.ID, params)
	if err != nil || scope == nil {
		t.Fatalf("current scope = %+v, %v", scope, err)
	}
	page, err := h.warm.probePage(ctx, project.ID, params, *scope,
		scope.ScopeHash, nil, nil)
	if err != nil || len(page) != 2 {
		t.Fatalf("probe fixture = %d, %v", len(page), err)
	}
	if err := h.warm.putCandidateEvidence(ctx, page[0], scope.ScopeHash,
		domain.Evidence{Outcome: "PASS", CompletedAt: h.now}); err != nil {
		t.Fatal(err)
	}
	old := domain.Job{ID: "budget-stopped", ProjectID: project.ID, Kind: PrewarmJobKind}
	if err := h.ingest.store.CreateJob(ctx, old, h.now); err != nil {
		t.Fatal(err)
	}
	if err := h.ingest.store.FinishJob(ctx, old.ID, sqlite.JobPartial,
		"attempt_limit", "", h.now); err != nil {
		t.Fatal(err)
	}

	if err := h.maint.RunDue(ctx); err != nil {
		t.Fatal(err)
	}
	jobs := poolMaintenanceJobs(t, h, project.ID)
	if len(jobs) != 2 {
		t.Fatalf("untried candidate did not start a new job: %+v", jobs)
	}
}

func TestPoolMaintenanceRunStopsWithCancelledContext(t *testing.T) {
	h := newPoolMaintenanceHarness(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() {
		h.maint.Run(ctx, nil)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("maintenance Run did not stop after cancellation")
	}
}

func waitForPoolMaintenanceJob(t *testing.T, h *poolMaintenanceHarness, jobID string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		job, err := h.ingest.store.GetJob(context.Background(), jobID)
		if err != nil {
			t.Fatal(err)
		}
		switch job.State {
		case sqlite.JobCompleted, sqlite.JobPartial, sqlite.JobFailed,
			sqlite.JobCancelled, sqlite.JobInterrupted:
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("job %s did not finish", jobID)
}

func formatPoolMaintenanceTime(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000000000Z07:00")
}
