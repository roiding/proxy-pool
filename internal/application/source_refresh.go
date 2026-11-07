package application

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"proxypool/internal/config"
	"proxypool/internal/domain"
	"proxypool/internal/storage/sqlite"
)

// RefreshConfig is only meaningful for refetchable list sources. Omission
// keeps legacy demand refresh behavior and the deployment's cache TTL.
type RefreshConfig struct {
	Mode          string `json:"mode"`
	IntervalMS    int64  `json:"interval_ms,omitempty"`
	CacheTTLMS    int64  `json:"cache_ttl_ms,omitempty"`
	MaxCandidates int    `json:"max_candidates,omitempty"`
}

func isListAdapter(adapter string) bool {
	return adapter == AdapterSite || adapter == AdapterStatic || adapter == AdapterAccount || adapter == AdapterProvider
}

func validateRefreshConfig(cfg *RefreshConfig, adapter string) *domain.Error {
	if cfg == nil {
		return nil
	}
	if !isListAdapter(adapter) {
		return domain.InvalidRequest("refresh is supported only for list sources")
	}
	if cfg.Mode != "demand" && cfg.Mode != "interval" {
		return domain.InvalidRequest("refresh mode must be demand or interval")
	}
	if cfg.Mode == "demand" && cfg.IntervalMS != 0 {
		return domain.InvalidRequest("demand refresh does not accept interval_ms")
	}
	if cfg.Mode == "interval" && (cfg.IntervalMS < 1000 || cfg.IntervalMS > 86400000) {
		return domain.InvalidRequest("refresh interval_ms must be within 1000..86400000")
	}
	if cfg.CacheTTLMS != 0 && (cfg.CacheTTLMS < 1000 || cfg.CacheTTLMS > 86400000) {
		return domain.InvalidRequest("refresh cache_ttl_ms must be within 1000..86400000")
	}
	if cfg.Mode == "interval" && (cfg.CacheTTLMS == 0 || cfg.CacheTTLMS < cfg.IntervalMS) {
		return domain.InvalidRequest("interval refresh requires cache_ttl_ms at least interval_ms")
	}
	if cfg.MaxCandidates < 0 || cfg.MaxCandidates > maxStaticCandidatesPerSource {
		return domain.InvalidRequest("refresh max_candidates must be within 1..%d when supplied", maxStaticCandidatesPerSource)
	}
	return nil
}

func (s *IngestService) sourceCacheTTL(cfg SourceConfig) time.Duration {
	// Network lists share one lifetime. Legacy per-source TTLs are accepted
	// for configuration compatibility, but cannot shorten or extend it.
	s.static.mu.Lock()
	defer s.static.mu.Unlock()
	return s.static.ttl
}

func sourceCandidateLimit(cfg SourceConfig) int {
	if cfg.Refresh != nil && cfg.Refresh.MaxCandidates > 0 {
		return cfg.Refresh.MaxCandidates
	}
	return maxStaticCandidatesPerSource
}

const SourceRefreshJobKind = "source_refresh"

// SourceRefreshService owns bounded asynchronous refresh operations. Its
// periodic scan runs synchronously through a small worker pool, not one
// permanent goroutine per source. It never probes or imports inventory.
type SourceRefreshService struct {
	store  *sqlite.Store
	ingest *IngestService
	cfg    config.Config
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	runs   map[string]context.CancelFunc
	Now    func() time.Time
}

func NewSourceRefreshService(store *sqlite.Store, ingest *IngestService, cfg config.Config) *SourceRefreshService {
	ctx, cancel := context.WithCancel(context.Background())
	return &SourceRefreshService{store: store, ingest: ingest, cfg: cfg, ctx: ctx, cancel: cancel, runs: map[string]context.CancelFunc{}, Now: time.Now}
}

func (r *SourceRefreshService) StartJob(ctx context.Context, sourceID string) (domain.Job, *domain.Error) {
	src, err := r.store.GetSource(ctx, sourceID)
	if err != nil {
		return domain.Job{}, domain.AsError(err)
	}
	if !src.Enabled || !isListAdapter(src.AdapterType) {
		return domain.Job{}, domain.InvalidRequest("refresh requires an enabled list source")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ctx.Err() != nil {
		return domain.Job{}, domain.E("SERVICE_SHUTTING_DOWN", 503, "source refresh service is shutting down")
	}
	params, _ := json.Marshal(map[string]any{"source_id": sourceID, "source_config_version": src.ConfigVersion})
	job := domain.Job{ID: domain.NewID("fetch_"), ProjectID: DefaultAcquireProject, Kind: SourceRefreshJobKind, ParamsJSON: string(params)}
	if len(r.runs) >= 2 {
		return domain.Job{}, domain.E("SOURCE_FETCH_CAPACITY", 503, "source refresh job capacity is full")
	}
	if err := r.store.CreateJobWithLimits(ctx, job, r.Now(), r.cfg.JobMaxActivePerProject, r.cfg.JobMaxActiveGlobal); err != nil {
		return domain.Job{}, domain.AsError(err)
	}
	runCtx, cancel := context.WithTimeout(r.ctx, 30*time.Second)
	r.runs[job.ID] = cancel
	go func() {
		defer cancel()
		defer func() { r.mu.Lock(); delete(r.runs, job.ID); r.mu.Unlock() }()
		if err := r.store.StartJobRun(runCtx, job.ID, r.Now()); err != nil {
			finalCtx, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			state, reason := sqlite.JobFailed, "JOB_START_FAILED"
			if runCtx.Err() != nil {
				state, reason = sqlite.JobCancelled, "cancelled"
			}
			_ = r.store.FinishJob(finalCtx, job.ID, state, reason, "", r.Now())
			return
		}
		current, err := r.store.GetSource(runCtx, sourceID)
		var res StaticRefreshResult
		var derr *domain.Error
		if err != nil || current.ConfigVersion != src.ConfigVersion || !current.Enabled {
			derr = domain.E("SOURCE_CONFIG_CHANGED", 409, "source changed before refresh started")
		} else {
			res, derr = r.ingest.RefreshStaticSource(runCtx, sourceID)
		}
		finalCtx, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		state, reason, lastError := sqlite.JobCompleted, "refreshed", ""
		if res.CacheHit {
			reason = "cache_reused"
		}
		if derr != nil {
			state, reason, lastError = sqlite.JobFailed, derr.Code, derr.Message
		}
		if runCtx.Err() != nil {
			state, reason = sqlite.JobCancelled, "cancelled"
		}
		if derr == nil {
			_ = r.store.SetJobProgress(finalCtx, job.ID, res.Retained, 0, 0, "", r.Now())
		}
		_ = r.store.FinishJob(finalCtx, job.ID, state, reason, lastError, r.Now())
	}()
	stored, err := r.store.GetJob(ctx, job.ID)
	if err != nil {
		return domain.Job{}, domain.StoreUnavailable(err)
	}
	return stored, nil
}

func (r *SourceRefreshService) CancelJob(ctx context.Context, jobID string) *domain.Error {
	r.mu.Lock()
	cancel := r.runs[jobID]
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	ok, err := r.store.CancelJob(ctx, "", jobID, r.Now())
	if err != nil {
		return domain.AsError(err)
	}
	if !ok {
		job, err := r.store.GetJob(ctx, jobID)
		if err != nil {
			return domain.AsError(err)
		}
		if job.State != sqlite.JobCancelled {
			return domain.E("JOB_NOT_ACTIVE", 409, "job %q is %s and can no longer be cancelled", jobID, job.State)
		}
	}
	return nil
}

func (r *SourceRefreshService) Run(ctx context.Context, log *slog.Logger) {
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(r.ctx, cancel)
	defer func() { stop(); cancel() }()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.ctx.Done():
			return
		case <-ticker.C:
		}
		if err := r.RunDue(ctx); err != nil && ctx.Err() == nil {
			log.Warn("source refresh scan failed", "error", err)
		}
	}
}

// RunDue is a bounded scan seam for deterministic scheduler tests.
func (r *SourceRefreshService) RunDue(ctx context.Context) error {
	if err := r.ingest.PurgeExpiredStaticCache(ctx); err != nil {
		return err
	}
	sources, err := r.store.ListIntervalSources(ctx, 32)
	if err != nil {
		return err
	}
	for _, src := range sources {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if r.ingest.static.hasFresh(src.ID, r.Now(), src.ConfigVersion) {
			continue
		}
		var cfg SourceConfig
		if json.Unmarshal([]byte(src.ConfigJSON), &cfg) != nil || cfg.Refresh == nil || cfg.Refresh.Mode != "interval" {
			continue
		}
		state, err := r.store.SourceFetchState(ctx, src.ID)
		if err != nil {
			return err
		}
		if state.NextFetchAt != nil && state.NextFetchAt.After(r.Now()) {
			continue
		}
		if state.ConfigVersion == src.ConfigVersion && state.NextRefreshAt != nil && state.NextRefreshAt.After(r.Now()) {
			continue
		}
		// RefreshStaticSource persists next_fetch_at before I/O. A second
		// scanner or a process restart respects that same reservation.
		_, derr := r.ingest.RefreshStaticSource(ctx, src.ID)
		if derr != nil && derr.Code == "ALLOCATION_STORE_UNAVAILABLE" {
			return derr
		}
	}
	return nil
}

func (r *SourceRefreshService) Shutdown(ctx context.Context) error {
	r.cancel()
	r.mu.Lock()
	for _, cancel := range r.runs {
		cancel()
	}
	r.mu.Unlock()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		r.mu.Lock()
		n := len(r.runs)
		r.mu.Unlock()
		if n == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func refreshErrorCode(err error) string {
	var derr *domain.Error
	if errors.As(err, &derr) {
		return derr.Code
	}
	return "SOURCE_UNAVAILABLE"
}

// nextSourceFetchAt unifies durable backoff for public/account snapshots and
// on-demand extraction. Unknown paid contracts are not implemented here.
func (s *IngestService) nextSourceFetchAt(ctx context.Context, id string, now time.Time, minimum time.Duration, derr *domain.Error) time.Time {
	wait := minimum
	if derr != nil {
		backoff := 30 * time.Second
		if state, err := s.store.SourceFetchState(ctx, id); err == nil {
			for i := 0; i < state.ConsecutiveFailures && backoff < 15*time.Minute; i++ {
				backoff *= 2
			}
		}
		backoff = min(backoff, 15*time.Minute)
		wait = max(wait, backoff, min(derr.RetryAfter, 24*time.Hour))
	}
	return now.Add(wait)
}
