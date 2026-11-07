package sqlite

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"proxypool/internal/domain"
)

func newPoolMaintenanceStoreAt(t *testing.T, path string) *Store {
	t.Helper()
	db, err := Open(path, 1024)
	if err != nil {
		t.Fatal(err)
	}
	store := New(db)
	ctx := context.Background()
	if err := store.Migrate(ctx); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := store.EnsurePoolMaintenanceSchema(ctx); err != nil {
		db.Close()
		t.Fatal(err)
	}
	return store
}

func createPoolMaintenanceProject(t *testing.T, store *Store, ctx context.Context,
	id string, enabled bool, target int, now time.Time) domain.Project {
	t.Helper()
	if err := store.EnsureProject(ctx, domain.Project{ID: id, Enabled: enabled}, now); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().ExecContext(ctx, `
		UPDATE projects
		SET auto_prewarm_target = ?, config_version = config_version + 1, updated_at = ?
		WHERE project_id = ?`, target, fmtTime(now), id); err != nil {
		t.Fatal(err)
	}
	project, err := store.GetProject(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return project
}

func createPoolMaintenanceJob(t *testing.T, store *Store, ctx context.Context,
	id, projectID string, now time.Time) {
	t.Helper()
	if err := store.CreateJob(ctx, domain.Job{
		ID: id, ProjectID: projectID, Kind: "prewarm",
	}, now); err != nil {
		t.Fatal(err)
	}
}

func TestPoolMaintenanceDueOrderClaimAndVersionGuard(t *testing.T) {
	store := newPoolMaintenanceStoreAt(t, filepath.Join(t.TempDir(), "pool.db"))
	ctx := context.Background()
	now := time.Date(2026, 11, 5, 9, 0, 0, 0, time.UTC)

	targetedA := createPoolMaintenanceProject(t, store, ctx, "project-a", true, 4, now)
	targetedB := createPoolMaintenanceProject(t, store, ctx, "project-b", true, 2, now)
	createPoolMaintenanceProject(t, store, ctx, "disabled", false, 3, now)
	createPoolMaintenanceProject(t, store, ctx, "target-zero", true, 0, now)

	due, err := store.ListPoolMaintenanceDue(ctx, now, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 || due[0].ProjectID != targetedA.ID ||
		due[0].ConfigVersion != targetedA.ConfigVersion || due[0].Target != 4 {
		t.Fatalf("first due page = %+v", due)
	}
	all, err := store.ListPoolMaintenanceDue(ctx, now, PoolMaintenanceScanLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 || all[0].ProjectID != targetedA.ID || all[1].ProjectID != targetedB.ID {
		t.Fatalf("due projects = %+v", all)
	}

	claimed, err := store.ClaimPoolMaintenance(ctx, targetedA.ID,
		targetedA.ConfigVersion, now, time.Second)
	if err != nil || !claimed {
		t.Fatalf("first claim = %v %v", claimed, err)
	}
	claimed, err = store.ClaimPoolMaintenance(ctx, targetedA.ID,
		targetedA.ConfigVersion, now.Add(time.Second), time.Second)
	if err != nil || claimed {
		t.Fatalf("claim ignored durable backoff: %v %v", claimed, err)
	}
	due, err = store.ListPoolMaintenanceDue(ctx, now.Add(10*time.Second), 10)
	if err != nil || len(due) != 1 || due[0].ProjectID != targetedB.ID {
		t.Fatalf("not-due project reentered scan: %+v %v", due, err)
	}
	due, err = store.ListPoolMaintenanceDue(ctx, now.Add(PoolMaintenanceMinBackoff), 10)
	if err != nil || len(due) != 2 || due[0].ProjectID != targetedB.ID ||
		due[1].ProjectID != targetedA.ID {
		t.Fatalf("backoff expiry/fair ordering = %+v %v", due, err)
	}

	// A stale scan result must not claim after the project version changed.
	updated := targetedB
	updated.Name = "changed"
	if _, err := store.UpdateProject(ctx, updated, updated.ConfigVersion, now); err != nil {
		t.Fatal(err)
	}
	claimed, err = store.ClaimPoolMaintenance(ctx, targetedB.ID,
		targetedB.ConfigVersion, now.Add(time.Minute), time.Second)
	if err != nil || claimed {
		t.Fatalf("stale project version claimed: %v %v", claimed, err)
	}
}

func TestPoolMaintenanceClaimRejectsActiveOrStoppedProjects(t *testing.T) {
	store := newPoolMaintenanceStoreAt(t, filepath.Join(t.TempDir(), "active.db"))
	ctx := context.Background()
	now := time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)

	active := createPoolMaintenanceProject(t, store, ctx, "active", true, 2, now)
	createPoolMaintenanceJob(t, store, ctx, "manual-active", active.ID, now)
	claimed, err := store.ClaimPoolMaintenance(ctx, active.ID,
		active.ConfigVersion, now, PoolMaintenanceMinBackoff)
	if err != nil || claimed {
		t.Fatalf("active prewarm was not deduplicated: %v %v", claimed, err)
	}

	disabled := createPoolMaintenanceProject(t, store, ctx, "disabled", true, 2, now)
	disabled.Enabled = false
	if _, err := store.UpdateProject(ctx, disabled, disabled.ConfigVersion, now); err != nil {
		t.Fatal(err)
	}
	current, err := store.GetProject(ctx, disabled.ID)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err = store.ClaimPoolMaintenance(ctx, disabled.ID,
		current.ConfigVersion, now, PoolMaintenanceMinBackoff)
	if err != nil || claimed {
		t.Fatalf("disabled project was claimed: %v %v", claimed, err)
	}

	zero := createPoolMaintenanceProject(t, store, ctx, "zero", true, 2, now)
	if _, err := store.DB().ExecContext(ctx, `
		UPDATE projects SET auto_prewarm_target = 0 WHERE project_id = ?`,
		zero.ID); err != nil {
		t.Fatal(err)
	}
	current, err = store.GetProject(ctx, zero.ID)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err = store.ClaimPoolMaintenance(ctx, zero.ID,
		current.ConfigVersion, now, PoolMaintenanceMinBackoff)
	if err != nil || claimed {
		t.Fatalf("zero-target project was claimed: %v %v", claimed, err)
	}
}

func TestPoolMaintenanceConcurrentClaimsCreateOneWindow(t *testing.T) {
	store := newPoolMaintenanceStoreAt(t, filepath.Join(t.TempDir(), "concurrent.db"))
	ctx := context.Background()
	now := time.Date(2026, 11, 5, 11, 0, 0, 0, time.UTC)
	project := createPoolMaintenanceProject(t, store, ctx, "shared", true, 3, now)

	const callers = 8
	var wg sync.WaitGroup
	results := make(chan bool, callers)
	errorsCh := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := store.ClaimPoolMaintenance(ctx, project.ID,
				project.ConfigVersion, now, PoolMaintenanceMinBackoff)
			results <- ok
			errorsCh <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errorsCh)
	wins := 0
	for ok := range results {
		if ok {
			wins++
		}
	}
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	if wins != 1 {
		t.Fatalf("concurrent claims = %d, want one", wins)
	}
}

func TestPoolMaintenanceActiveJobsDoNotConsumeDuePage(t *testing.T) {
	store := newPoolMaintenanceStoreAt(t, filepath.Join(t.TempDir(), "active-page.db"))
	ctx := context.Background()
	now := time.Date(2026, 11, 5, 11, 30, 0, 0, time.UTC)

	for i := 0; i < PoolMaintenanceScanLimit; i++ {
		id := "busy-" + fmt.Sprintf("%02d", i)
		project := createPoolMaintenanceProject(t, store, ctx, id, true, 1, now)
		createPoolMaintenanceJob(t, store, ctx, "job-"+id, project.ID, now)
	}
	available := createPoolMaintenanceProject(t, store, ctx, "z-available", true, 1, now)
	due, err := store.ListPoolMaintenanceDue(ctx, now, PoolMaintenanceScanLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 || due[0].ProjectID != available.ID {
		t.Fatalf("active jobs blocked the due page: %+v", due)
	}
}

func TestPoolMaintenanceMinBackoffSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restart.db")
	ctx := context.Background()
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)

	first := newPoolMaintenanceStoreAt(t, path)
	project := createPoolMaintenanceProject(t, first, ctx, "restart", true, 5, now)
	claimed, err := first.ClaimPoolMaintenance(ctx, project.ID,
		project.ConfigVersion, now, PoolMaintenanceMinBackoff)
	if err != nil || !claimed {
		t.Fatalf("initial claim = %v %v", claimed, err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second := newPoolMaintenanceStoreAt(t, path)
	t.Cleanup(func() { _ = second.Close() })
	due, err := second.ListPoolMaintenanceDue(ctx,
		now.Add(PoolMaintenanceMinBackoff-time.Second), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 0 {
		t.Fatalf("restart bypassed durable backoff: %+v", due)
	}
	due, err = second.ListPoolMaintenanceDue(ctx,
		now.Add(PoolMaintenanceMinBackoff), 10)
	if err != nil || len(due) != 1 || due[0].ProjectID != project.ID {
		t.Fatalf("restart did not restore due project: %+v %v", due, err)
	}
}

func TestPoolMaintenanceZeroToPositiveTargetStartsWithinBackoff(t *testing.T) {
	store := newPoolMaintenanceStoreAt(t, filepath.Join(t.TempDir(), "reenable.db"))
	ctx := context.Background()
	now := time.Date(2026, 11, 5, 12, 30, 0, 0, time.UTC)
	project := createPoolMaintenanceProject(t, store, ctx, "reenable", true, 2, now)
	claimed, err := store.ClaimPoolMaintenance(ctx, project.ID,
		project.ConfigVersion, now, PoolMaintenanceMinBackoff)
	if err != nil || !claimed {
		t.Fatalf("initial claim = %v %v", claimed, err)
	}

	project.Defaults.AutoPrewarmTarget = 0
	if _, err := store.UpdateProject(ctx, project, project.ConfigVersion, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	project.Defaults.AutoPrewarmTarget = 100
	if _, err := store.UpdateProject(ctx, project, project.ConfigVersion+1, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	due, err := store.ListPoolMaintenanceDue(ctx,
		now.Add(PoolMaintenanceMinBackoff-time.Second), 10)
	if err != nil || len(due) != 0 {
		t.Fatalf("reenabled project bypassed active launch window: %+v %v", due, err)
	}
	due, err = store.ListPoolMaintenanceDue(ctx, now.Add(PoolMaintenanceMinBackoff), 10)
	if err != nil || len(due) != 1 || due[0].Target != 100 {
		t.Fatalf("reenabled project did not become due in 30s: %+v %v", due, err)
	}
}

func TestPoolMaintenanceStopOnlyCancelsMappedAutomaticJobs(t *testing.T) {
	store := newPoolMaintenanceStoreAt(t, filepath.Join(t.TempDir(), "stop.db"))
	ctx := context.Background()
	now := time.Date(2026, 11, 5, 13, 0, 0, 0, time.UTC)

	auto := createPoolMaintenanceProject(t, store, ctx, "auto", true, 2, now)
	createPoolMaintenanceJob(t, store, ctx, "auto-job", auto.ID, now)
	if err := store.RecordPoolMaintenanceJob(ctx, auto.ID, "auto-job",
		auto.ConfigVersion, now); err != nil {
		t.Fatal(err)
	}
	manual := createPoolMaintenanceProject(t, store, ctx, "manual", true, 2, now)
	createPoolMaintenanceJob(t, store, ctx, "manual-job", manual.ID, now)

	for _, id := range []string{auto.ID, manual.ID} {
		if _, err := store.DB().ExecContext(ctx, `
			UPDATE projects SET auto_prewarm_target = 0 WHERE project_id = ?`, id); err != nil {
			t.Fatal(err)
		}
	}
	versioned := createPoolMaintenanceProject(t, store, ctx, "versioned", true, 2, now)
	createPoolMaintenanceJob(t, store, ctx, "versioned-job", versioned.ID, now)
	if err := store.RecordPoolMaintenanceJob(ctx, versioned.ID, "versioned-job",
		versioned.ConfigVersion, now); err != nil {
		t.Fatal(err)
	}
	versioned.Defaults.AutoPrewarmTarget = 3
	if _, err := store.UpdateProject(ctx, versioned, versioned.ConfigVersion,
		now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	jobs, err := store.PoolMaintenanceJobsToStop(ctx, 32)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 2 ||
		(jobs[0] != "auto-job" && jobs[0] != "versioned-job") ||
		(jobs[1] != "auto-job" && jobs[1] != "versioned-job") {
		t.Fatalf("stop candidates = %v, want auto/versioned jobs", jobs)
	}

	ok, err := store.CancelJob(ctx, "", "auto-job", now.Add(time.Second))
	if err != nil || !ok {
		t.Fatalf("cancel automatic job = %v %v", ok, err)
	}
	ok, err = store.CancelJob(ctx, "", "versioned-job", now.Add(time.Second))
	if err != nil || !ok {
		t.Fatalf("cancel version-changed automatic job = %v %v", ok, err)
	}
	jobs, err = store.PoolMaintenanceJobsToStop(ctx, 32)
	if err != nil || len(jobs) != 0 {
		t.Fatalf("terminal job remained stoppable: %v %v", jobs, err)
	}
	manualJob, err := store.GetJob(ctx, "manual-job")
	if err != nil || manualJob.State != JobPending {
		t.Fatalf("manual job changed state: %+v %v", manualJob, err)
	}
}

func TestPoolMaintenanceRecordRequiresOwnedPrewarmAndPreservesLastJob(t *testing.T) {
	store := newPoolMaintenanceStoreAt(t, filepath.Join(t.TempDir(), "record.db"))
	ctx := context.Background()
	now := time.Date(2026, 11, 5, 14, 0, 0, 0, time.UTC)
	project := createPoolMaintenanceProject(t, store, ctx, "owner", true, 1, now)
	claimed, err := store.ClaimPoolMaintenance(ctx, project.ID,
		project.ConfigVersion, now, PoolMaintenanceMinBackoff)
	if err != nil || !claimed {
		t.Fatalf("claim = %v %v", claimed, err)
	}
	createPoolMaintenanceJob(t, store, ctx, "owned", project.ID, now)

	if err := store.RecordPoolMaintenanceJob(ctx, "other", "owned",
		project.ConfigVersion, now); err == nil {
		t.Fatal("cross-project automatic job link was accepted")
	}
	if err := store.RecordPoolMaintenanceJob(ctx, project.ID, "owned",
		project.ConfigVersion, now); err != nil {
		t.Fatal(err)
	}
	var linked string
	if err := store.DB().QueryRowContext(ctx, `
		SELECT last_job_id FROM pool_maintenance_state WHERE project_id = ?`,
		project.ID).Scan(&linked); err != nil {
		t.Fatal(err)
	}
	if linked != "owned" {
		t.Fatalf("last_job_id = %q", linked)
	}
	if err := store.FinishPoolMaintenance(ctx, project.ID, "owned", "", now); err != nil {
		t.Fatal(err)
	}
	if err := store.DB().QueryRowContext(ctx, `
		SELECT last_job_id FROM pool_maintenance_state WHERE project_id = ?`,
		project.ID).Scan(&linked); err != nil {
		t.Fatal(err)
	}
	if linked != "owned" {
		t.Fatalf("scheduling completion lost the last automatic job: %q", linked)
	}
	var mapped int
	if err := store.DB().QueryRowContext(ctx, `
		SELECT COUNT(*) FROM pool_maintenance_jobs WHERE job_id = ?`,
		"owned").Scan(&mapped); err != nil {
		t.Fatal(err)
	}
	if mapped != 1 {
		t.Fatal("finish removed the automatic-job audit/stop mapping")
	}
}

func TestEnsurePoolMaintenanceSchemaAddsProjectColumnIdempotently(t *testing.T) {
	store := newPoolMaintenanceStoreAt(t, filepath.Join(t.TempDir(), "schema.db"))
	ctx := context.Background()
	if err := store.EnsurePoolMaintenanceSchema(ctx); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := store.DB().QueryRowContext(ctx, `
		SELECT COUNT(*) FROM pragma_table_info('projects')
		WHERE name = 'auto_prewarm_target'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("auto_prewarm_target columns = %d", n)
	}
}

func TestPoolMaintenanceNotFoundProjectIsNotClaimed(t *testing.T) {
	store := newPoolMaintenanceStoreAt(t, filepath.Join(t.TempDir(), "missing.db"))
	claimed, err := store.ClaimPoolMaintenance(context.Background(), "missing", 1,
		time.Now(), PoolMaintenanceMinBackoff)
	if err != nil || claimed {
		t.Fatalf("missing project claim = %v %v", claimed, err)
	}
}

func TestAccountFetchAvailableHonorsSharedWindow(t *testing.T) {
	store := newPoolMaintenanceStoreAt(t, filepath.Join(t.TempDir(), "account.db"))
	ctx := context.Background()
	now := time.Date(2026, 11, 5, 15, 0, 0, 0, time.UTC)
	available, err := store.AccountFetchAvailable(ctx, "shared-key", now)
	if err != nil || !available {
		t.Fatalf("missing account window = %v %v", available, err)
	}
	if err := store.ClaimAccountFetch(ctx, "shared-key", now, time.Minute); err != nil {
		t.Fatal(err)
	}
	available, err = store.AccountFetchAvailable(ctx, "shared-key",
		now.Add(59*time.Second))
	if err != nil || available {
		t.Fatalf("open account window reported available: %v %v", available, err)
	}
	available, err = store.AccountFetchAvailable(ctx, "shared-key",
		now.Add(time.Minute))
	if err != nil || !available {
		t.Fatalf("expired account window remained blocked: %v %v", available, err)
	}
}
