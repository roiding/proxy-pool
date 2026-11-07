package application

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"proxypool/internal/domain"
	"proxypool/internal/storage/sqlite"
)

const (
	// PoolMaintenanceInterval is deliberately short because a project may be
	// configured at any point; the per-project durable backoff prevents work
	// from actually being scheduled that often.
	PoolMaintenanceInterval = 5 * time.Second
	// PoolMaintenanceBackoff also acts as a durable launch lease. If a process
	// stops after claiming but before creating a job, a restarted scheduler
	// cannot launch a duplicate inside this window.
	PoolMaintenanceBackoff = sqlite.PoolMaintenanceMinBackoff
)

// PoolMaintenanceService keeps project-scoped ready inventory at each
// project's configured target. It never probes or downloads directly: all
// inventory work is delegated to PrewarmService.StartJob so shared background
// slots, project scope, blacklists, and job limits retain one implementation.
type PoolMaintenanceService struct {
	store            *sqlite.Store
	prewarm          *PrewarmService
	Now              func() time.Time
	TargetMetBackoff time.Duration
}

// NewPoolMaintenanceService wires the scheduler to the one shared prewarm
// service used by manual and automatic jobs.
func NewPoolMaintenanceService(store *sqlite.Store,
	prewarm *PrewarmService) *PoolMaintenanceService {
	return &PoolMaintenanceService{
		store:            store,
		prewarm:          prewarm,
		Now:              time.Now,
		TargetMetBackoff: PoolMaintenanceBackoff,
	}
}

func (m *PoolMaintenanceService) targetMetBackoff() time.Duration {
	if m != nil && m.TargetMetBackoff > 0 {
		return m.TargetMetBackoff
	}
	return PoolMaintenanceBackoff
}

func (m *PoolMaintenanceService) now() time.Time {
	if m != nil && m.Now != nil {
		return m.Now()
	}
	return time.Now()
}

// Run scans immediately and then at a bounded cadence until ctx is cancelled.
// Individual project work checks ctx throughout, so shutdown does not wait
// for a full page of projects.
func (m *PoolMaintenanceService) Run(ctx context.Context, log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	ticker := time.NewTicker(PoolMaintenanceInterval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		if err := m.RunDue(ctx); err != nil && ctx.Err() == nil {
			log.Warn("pool maintenance scan failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// RunDue performs one bounded scheduler pass. It first stops automatically
// created jobs for projects that were disabled or set back to target zero,
// then checks due projects in durable-deadline order.
func (m *PoolMaintenanceService) RunDue(ctx context.Context) error {
	if m == nil || m.store == nil || m.prewarm == nil {
		return errors.New("pool maintenance service is not configured")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var firstErr error
	record := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	stopJobs, err := m.store.PoolMaintenanceJobsToStop(ctx,
		sqlite.PoolMaintenanceScanLimit)
	if err != nil {
		return err
	}
	for _, jobID := range stopJobs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if derr := m.prewarm.CancelJob(ctx, jobID); derr != nil &&
			derr.Code != "JOB_NOT_ACTIVE" && derr.Code != "NOT_FOUND" {
			record(derr)
		}
	}

	projects, err := m.store.ListPoolMaintenanceDue(ctx, m.now(),
		sqlite.PoolMaintenanceScanLimit)
	if err != nil {
		return err
	}
	for _, due := range projects {
		if err := ctx.Err(); err != nil {
			return err
		}
		project, err := m.store.GetProject(ctx, due.ProjectID)
		if err != nil {
			record(err)
			continue
		}
		if project.ConfigVersion != due.ConfigVersion || !project.Enabled ||
			project.Defaults.AutoPrewarmTarget <= 0 {
			continue
		}
		record(m.maintainProject(ctx, project))
	}
	return firstErr
}

func (m *PoolMaintenanceService) maintainProject(ctx context.Context,
	project domain.Project) error {
	now := m.now()
	req, params, err := m.prewarmRequest(project)
	if err != nil {
		_ = m.store.FinishPoolMaintenance(ctx, project.ID, "", err.Error(), now)
		return err
	}
	scope, err := m.prewarm.currentScope(ctx, project.ID, params)
	if err != nil {
		_ = m.store.FinishPoolMaintenance(ctx, project.ID, "", err.Error(), now)
		return err
	}
	if scope == nil {
		return nil
	}
	ready, err := m.prewarm.currentReady(ctx, project.ID, params, *scope,
		scope.ScopeHash)
	if err != nil {
		_ = m.store.FinishPoolMaintenance(ctx, project.ID, "", err.Error(), now)
		return err
	}
	if ready >= project.Defaults.AutoPrewarmTarget {
		return m.deferProject(ctx, project.ID, project.ConfigVersion, now, m.targetMetBackoff())
	}
	work, err := m.hasPrewarmWork(ctx, project.ID, params, *scope)
	if err != nil {
		_ = m.store.FinishPoolMaintenance(ctx, project.ID, "", err.Error(), now)
		return err
	}
	if !work {
		return m.deferProject(ctx, project.ID, project.ConfigVersion, now,
			PoolMaintenanceBackoff)
	}
	claimed, err := m.store.ClaimPoolMaintenance(ctx, project.ID,
		project.ConfigVersion, now, PoolMaintenanceBackoff)
	if err != nil || !claimed {
		return err
	}

	// Claim rechecks the version, but a project edit can land immediately
	// afterward. Re-read once before start so a target-zero transition does
	// not create a job; StartJob then performs its own live scope read.
	current, err := m.store.GetProject(ctx, project.ID)
	if err != nil {
		_ = m.store.FinishPoolMaintenance(ctx, project.ID, "", err.Error(), now)
		return err
	}
	if !current.Enabled || current.Defaults.AutoPrewarmTarget <= 0 {
		_ = m.store.FinishPoolMaintenance(ctx, project.ID, "",
			"project disabled before prewarm start", now)
		return nil
	}
	if current.ConfigVersion != project.ConfigVersion {
		req, params, err = m.prewarmRequest(current)
		if err != nil {
			_ = m.store.FinishPoolMaintenance(ctx, project.ID, "", err.Error(), now)
			return err
		}
		scope, err = m.prewarm.currentScope(ctx, project.ID, params)
		if err != nil {
			_ = m.store.FinishPoolMaintenance(ctx, project.ID, "", err.Error(), now)
			return err
		}
		if scope == nil {
			return m.deferProject(ctx, project.ID, current.ConfigVersion, now,
				PoolMaintenanceBackoff)
		}
		ready, err = m.prewarm.currentReady(ctx, project.ID, params, *scope,
			scope.ScopeHash)
		if err != nil {
			_ = m.store.FinishPoolMaintenance(ctx, project.ID, "", err.Error(), now)
			return err
		}
		if ready >= current.Defaults.AutoPrewarmTarget {
			return m.deferProject(ctx, project.ID, current.ConfigVersion, now, m.targetMetBackoff())
		}
		work, err = m.hasPrewarmWork(ctx, project.ID, params, *scope)
		if err != nil {
			_ = m.store.FinishPoolMaintenance(ctx, project.ID, "", err.Error(), now)
			return err
		}
		if !work {
			return m.deferProject(ctx, project.ID, current.ConfigVersion, now,
				PoolMaintenanceBackoff)
		}
	}
	job, derr := m.prewarm.StartJob(ctx, project.ID, req)
	if derr != nil {
		_ = m.store.FinishPoolMaintenance(ctx, project.ID, "", derr.Error(), now)
		return derr
	}
	if err := m.store.RecordPoolMaintenanceJob(ctx, project.ID, job.ID,
		current.ConfigVersion, now); err != nil {
		// Without the mapping a later target-zero scan cannot distinguish this
		// automatic job from a manual one, so fail closed and cancel it.
		cancelCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = m.prewarm.CancelJob(cancelCtx, job.ID)
		cancel()
		finishCtx, finishCancel := context.WithTimeout(context.Background(), time.Second)
		_ = m.store.FinishPoolMaintenance(finishCtx, project.ID, "",
			err.Error(), m.now())
		finishCancel()
		return err
	}
	if err := m.store.FinishPoolMaintenance(ctx, project.ID, job.ID, "", now); err != nil {
		return err
	}
	slog.Info("automatic prewarm started", "job_id", job.ID,
		"project_id", project.ID, "target", current.Defaults.AutoPrewarmTarget, "ready", ready)

	// Cover the narrow window where the API changes the target between the
	// claim and job registration. The next scan would catch this too, but a
	// prompt cancellation avoids consuming background probe slots.
	after, err := m.store.GetProject(ctx, project.ID)
	if err != nil {
		return nil
	}
	if !after.Enabled || after.Defaults.AutoPrewarmTarget <= 0 ||
		after.ConfigVersion != current.ConfigVersion {
		if derr := m.prewarm.CancelJob(ctx, job.ID); derr != nil &&
			derr.Code != "JOB_NOT_ACTIVE" && derr.Code != "NOT_FOUND" {
			return derr
		}
	}
	return nil
}

func (m *PoolMaintenanceService) deferProject(ctx context.Context, projectID string,
	configVersion int64, now time.Time, backoff time.Duration) error {
	_, err := m.store.DeferPoolMaintenance(ctx, projectID, configVersion, now, backoff)
	return err
}

func (m *PoolMaintenanceService) hasPrewarmWork(ctx context.Context, projectID string,
	params prewarmParams, scope prewarmScope) (bool, error) {
	page, err := m.prewarm.probePage(ctx, projectID, params, scope,
		scope.ScopeHash, nil, nil)
	if err != nil {
		return false, err
	}
	if len(page) > 0 {
		return true, nil
	}
	return m.hasDueStaticRefresh(ctx, projectID, scope)
}

func (m *PoolMaintenanceService) hasDueStaticRefresh(ctx context.Context,
	projectID string, scope prewarmScope) (bool, error) {
	supply := m.prewarm.StaticSupply
	if supply == nil || scope.SourceEmpty || !staticKindAllowed(scope) {
		return false, nil
	}
	ids, err := supply.StaticSourcesForRefill(ctx, projectID, 64)
	if err != nil {
		return false, err
	}
	now := m.now()
	for _, id := range ids {
		if !scopeSourceAllowed(scope, id) {
			continue
		}
		src, err := m.store.GetSource(ctx, id)
		if err != nil || !src.Enabled {
			continue
		}
		var cfg SourceConfig
		if json.Unmarshal([]byte(src.ConfigJSON), &cfg) != nil ||
			!sourceProjectAllowed(cfg, projectID) {
			continue
		}
		if !supply.staticRefreshExecutable(id, src, now) {
			continue
		}
		if len(supply.fetchSlots) >= cap(supply.fetchSlots) {
			continue
		}
		state, err := m.store.SourceFetchState(ctx, id)
		if err != nil {
			return false, err
		}
		if state.NextFetchAt != nil && state.NextFetchAt.After(now) {
			continue
		}
		if src.AdapterType == AdapterAccount {
			token, derr := supply.readAccountToken(ctx, id)
			if derr != nil {
				continue
			}
			key := domain.CredentialVersion(supply.secret, "webshare-account-api", token)
			available, err := m.store.AccountFetchAvailable(ctx, key, now)
			if err != nil {
				return false, err
			}
			if !available {
				continue
			}
		}
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if _, fresh := supply.static.freshResult(id, src.ConfigVersion, now); fresh {
			continue
		}
		return true, nil
	}
	return false, nil
}

func (s *IngestService) staticRefreshExecutable(id string, src domain.Source,
	now time.Time) bool {
	s.static.mu.Lock()
	defer s.static.mu.Unlock()
	if _, ok := s.static.inFlight[id]; ok {
		return false
	}
	if len(s.static.inFlight) >= s.static.maxEntries {
		return false
	}
	if entry, ok := s.static.entries[id]; ok && entry.configVersion == src.ConfigVersion &&
		s.static.entryFresh(entry, now) {
		return false
	}
	return now.Sub(s.static.lastRefresh[id]) >= s.staticMinInterval(src)
}

// prewarmRequest mirrors StartJob's project inheritance and validation
// normalization before Ready is evaluated. The returned request is also used
// by StartJob, so the check and the eventual probe observe the same scope.
func (m *PoolMaintenanceService) prewarmRequest(project domain.Project) (
	PrewarmRequest, prewarmParams, error) {
	target := project.Defaults.AutoPrewarmTarget
	if target <= 0 {
		return PrewarmRequest{}, prewarmParams{},
			domain.InvalidRequest("auto prewarm target must be positive")
	}
	req := PrewarmRequest{Count: target}
	// A nil request means every source currently authorized for this project;
	// FilterAllowedSources also preserves an explicit empty allowlist as an
	// empty (not nil) scope.
	req.Filters.SourceIDs = project.FilterAllowedSources(nil)
	if project.Defaults.Validation.Mode != "" {
		validationScope := project.Defaults.Validation
		req.Validation = &validationScope
	}
	if derr := m.prewarm.validate(&req); derr != nil {
		return PrewarmRequest{}, prewarmParams{}, derr
	}
	params := prewarmParams{
		Count:      req.Count,
		DeadlineMS: req.DeadlineMS,
		Validation: *req.Validation,
		Filters:    req.Filters,
	}
	return req, params, nil
}
