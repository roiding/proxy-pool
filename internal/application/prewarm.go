package application

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"proxypool/internal/config"
	"proxypool/internal/domain"
	"proxypool/internal/geoip"
	"proxypool/internal/storage/sqlite"
	"proxypool/internal/validation"
)

// PrewarmJobKind is the durable job kind recorded in the jobs table.
const PrewarmJobKind = "prewarm"

// prewarmPageLimit bounds one probe round; the runner re-evaluates ready and
// its stop conditions between rounds, never inside a single giant batch.
const prewarmPageLimit = 32

// maxPrewarmReadyScan bounds how many eligible candidates a ready
// re-evaluation may inspect in Go per round.
const maxPrewarmReadyScan = 400

type PrewarmRequest struct {
	Count      int     `json:"count"`
	Filters    Filters `json:"filters"`
	DeadlineMS int64   `json:"deadline_ms"`
	// Validation optionally upgrades the prewarm scope to target TLS. The
	// normalized scope (including the runtime security hash) is frozen into
	// the persisted job params at start.
	Validation *domain.ValidationScope `json:"validation,omitempty"`
}

type PrewarmStatus struct {
	Running    bool       `json:"running"`
	JobID      string     `json:"job_id,omitempty"`
	Target     int        `json:"target"`
	Ready      int        `json:"ready"`
	Probed     int        `json:"probed"`
	Failed     int        `json:"failed"`
	State      string     `json:"state,omitempty"`
	StopReason string     `json:"stop_reason,omitempty"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	LastError  string     `json:"last_error,omitempty"`
}

// prewarmParams is the persisted job payload. The validation scope is stored
// fully normalized so a config change after enqueue cannot silently move a
// running job to a different evidence scope.
type prewarmParams struct {
	Count      int                    `json:"count"`
	DeadlineMS int64                  `json:"deadline_ms"`
	Validation domain.ValidationScope `json:"validation"`
	Filters    Filters                `json:"filters"`
}

// prewarmScope is the effective project scope recomputed from storage before
// every page. The serialized request parameters are a ceiling, not a frozen
// authorization snapshot: project enablement and its allowlist can change.
type prewarmScope struct {
	SourceIDs          []string
	Transports         []domain.Transport
	Countries          []string
	Kinds              []domain.ResourceKind
	Validation         domain.ValidationScope
	ScopeHash          string
	SourceUnrestricted bool
	SourceEmpty        bool
}

// PrewarmService refreshes local-inventory evidence until a current,
// project-scoped ready count is reached. Jobs are durable (jobs table), run
// detached from the creating HTTP connection, and never deliver proxies or
// write project CD: prewarmed evidence only shortens later acquire probes.
type PrewarmService struct {
	store        *sqlite.Store
	cfg          config.Config
	judgeURL     string
	rootCAs      *x509.CertPool
	health       *HealthRecorder
	geo          geoip.Resolver
	probeSlots   chan struct{}
	sourcePolicy sqlite.SourceCircuitPolicy
	Now          func() time.Time

	runsMu sync.Mutex
	// runs maps job IDs of this process to their cancel functions so
	// CancelJob terminates in-flight probe I/O immediately.
	runs           map[string]context.CancelFunc
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
}

func NewPrewarmService(store *sqlite.Store, cfg config.Config, judgeURL string,
	rootCAs *x509.CertPool, probeSlots chan struct{}, geo geoip.Resolver) *PrewarmService {
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	return &PrewarmService{
		store:      store,
		cfg:        cfg,
		judgeURL:   judgeURL,
		rootCAs:    rootCAs,
		health:     newHealthRecorder(store, cfg),
		geo:        geo,
		probeSlots: probeSlots,
		sourcePolicy: sqlite.SourceCircuitPolicy{
			FailureThreshold: cfg.SourceFailureThreshold,
			Base:             cfg.SourceCooldownBase(),
			Max:              cfg.SourceCooldownMax(),
		},
		runs:           map[string]context.CancelFunc{},
		shutdownCtx:    shutdownCtx,
		shutdownCancel: shutdownCancel,
		Now:            time.Now,
	}
}

func (p *PrewarmService) validate(req *PrewarmRequest) *domain.Error {
	if req.Count < 1 {
		req.Count = 1
	}
	if req.DeadlineMS <= 0 {
		req.DeadlineMS = p.cfg.PrewarmDeadlineMS
	}
	if req.DeadlineMS > p.cfg.PrewarmMaxDeadlineMS {
		req.DeadlineMS = p.cfg.PrewarmMaxDeadlineMS
	}
	if len(req.Filters.Transports) == 0 {
		req.Filters.Transports = []domain.Transport{domain.TransportHTTP, domain.TransportSOCKS5}
	}
	for _, t := range req.Filters.Transports {
		if t != domain.TransportHTTP && t != domain.TransportSOCKS5 {
			return domain.UnsupportedTransport(string(t))
		}
	}
	for _, k := range req.Filters.ResourceKinds {
		switch k {
		case domain.KindStatic, domain.KindSticky, domain.KindRotating:
		default:
			return domain.InvalidRequest("unknown resource_kind %q", string(k))
		}
	}
	req.Filters.Countries = normalizeCountries(req.Filters.Countries)
	for _, c := range req.Filters.Countries {
		if len(c) != 2 || c[0] < 'A' || c[0] > 'Z' || c[1] < 'A' || c[1] > 'Z' {
			return domain.InvalidRequest("country filter %q must be a 2-letter ISO code", c)
		}
	}
	if req.Validation == nil {
		req.Validation = &domain.ValidationScope{}
	}
	normalized, derr := validation.NormalizeOptions(req.Validation.Mode, req.Validation.TargetHostPort)
	if derr != nil {
		return derr
	}
	normalized.SecurityHash = validation.ProbeSecurityHash(p.probeOptions(normalized.Mode))
	*req.Validation = normalized
	return nil
}

// probeOptions mirrors the acquire-side runtime validation configuration so
// both paths key evidence under the identical security hash.
func (p *PrewarmService) probeOptions(mode string) validation.ProbeOptions {
	limits := validation.ProbeLimits{
		DialTimeout:  p.cfg.ConnectTimeout(),
		ProbeTotal:   p.cfg.ProbeTotalTimeout(),
		AllowPrivate: p.cfg.AllowPrivateAddresses,
		JudgeSecret:  p.cfg.JudgeSecret,
	}
	if mode != "tls" {
		limits.RootCAs = p.rootCAs
		limits.CAFingerprint = judgeCAPingerprint(p.cfg.JudgeCAPem)
	}
	return validation.ProbeOptions{JudgeURL: p.judgeURL, Limits: limits}
}

// StartJob enqueues a durable prewarm job for the project and returns its
// persisted row. The runner detaches from ctx: the job survives the HTTP
// connection that created it and can be polled and cancelled by ID.
func (p *PrewarmService) StartJob(ctx context.Context, projectID string,
	req PrewarmRequest) (domain.Job, *domain.Error) {
	if projectID == "" {
		projectID = DefaultAcquireProject
	}
	proj, err := p.store.GetProject(ctx, projectID)
	if err != nil {
		return domain.Job{}, domain.AsError(err)
	}
	// The job inherits the project's identity: its allowlist bounds every
	// candidate selection (nil = unrestricted, empty = nothing) and its
	// default validation scope applies when the request omits one. Project
	// CD is re-evaluated inside every ready/probe-page computation.
	req.Filters.SourceIDs = proj.FilterAllowedSources(req.Filters.SourceIDs)
	if (req.Validation == nil || req.Validation.Mode == "") && proj.Defaults.Validation.Mode != "" {
		def := proj.Defaults.Validation
		req.Validation = &def
	}
	if derr := p.validate(&req); derr != nil {
		return domain.Job{}, derr
	}
	params := prewarmParams{
		Count:      req.Count,
		DeadlineMS: req.DeadlineMS,
		Validation: *req.Validation,
		Filters:    req.Filters,
	}
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		return domain.Job{}, domain.AsError(err)
	}
	job := domain.Job{
		ID:         domain.NewID("warm_"),
		ProjectID:  projectID,
		Kind:       PrewarmJobKind,
		ParamsJSON: string(paramsJSON),
		Target:     params.Count,
	}
	// Cap counting and insertion are one transaction; otherwise concurrent
	// starters can each pass the same stale count and exceed both limits.
	if err := p.store.CreateJobWithLimits(ctx, job, p.Now(),
		p.cfg.JobMaxActivePerProject, p.cfg.JobMaxActiveGlobal); err != nil {
		var derr *domain.Error
		if errors.As(err, &derr) {
			return domain.Job{}, derr
		}
		return domain.Job{}, domain.StoreUnavailable(err)
	}
	p.runsMu.Lock()
	if p.shutdownCtx.Err() != nil {
		p.runsMu.Unlock()
		_, _ = p.store.CancelJob(context.Background(), "", job.ID, p.Now())
		return domain.Job{}, domain.E("SERVICE_SHUTTING_DOWN", 503,
			"prewarm service is shutting down")
	}
	p.runsMu.Unlock()
	// The durable deadline covers every phase of the job: queue admission,
	// page selection, batch probing, progress persistence, and finalization.
	// This total context is separate from each probe's short timeout.
	runCtx, cancel := context.WithTimeout(p.shutdownCtx,
		time.Duration(params.DeadlineMS)*time.Millisecond)
	p.runsMu.Lock()
	if p.shutdownCtx.Err() != nil {
		p.runsMu.Unlock()
		cancel()
		_, _ = p.store.CancelJob(context.Background(), "", job.ID, p.Now())
		return domain.Job{}, domain.E("SERVICE_SHUTTING_DOWN", 503,
			"prewarm service is shutting down")
	}
	p.runs[job.ID] = cancel
	p.runsMu.Unlock()
	go func() {
		defer func() {
			p.runsMu.Lock()
			delete(p.runs, job.ID)
			p.runsMu.Unlock()
			cancel()
		}()
		p.execute(runCtx, job.ID, projectID, params)
	}()
	stored, err := p.store.GetProjectJob(ctx, projectID, job.ID)
	if err != nil {
		return domain.Job{}, domain.AsError(err)
	}
	return stored, nil
}

// GetJob returns one job when the project owns it. An empty projectID is the
// deployment admin view and sees every job.
func (p *PrewarmService) GetJob(ctx context.Context, projectID, jobID string) (domain.Job, *domain.Error) {
	j, err := p.store.GetProjectJob(ctx, projectID, jobID)
	if err != nil {
		return domain.Job{}, domain.AsError(err)
	}
	return j, nil
}

// ListJobs returns the project's most recent jobs; empty projectID lists all.
func (p *PrewarmService) ListJobs(ctx context.Context, projectID string, limit int) ([]domain.Job, *domain.Error) {
	jobs, err := p.store.ListJobs(ctx, projectID, limit)
	if err != nil {
		return nil, domain.AsError(err)
	}
	return jobs, nil
}

// CancelJob marks the job cancelled in storage and terminates this process's
// in-flight runner through its context, aborting probe I/O immediately. The
// caller (API layer) enforces project ownership before invoking this.
func (p *PrewarmService) CancelJob(ctx context.Context, jobID string) *domain.Error {
	p.runsMu.Lock()
	cancel, tracked := p.runs[jobID]
	p.runsMu.Unlock()
	if tracked {
		cancel()
	}
	ok, err := p.store.CancelJob(ctx, "", jobID, p.Now())
	if err != nil {
		return domain.AsError(err)
	}
	if !ok {
		j, gerr := p.store.GetJob(ctx, jobID)
		if gerr != nil {
			return domain.AsError(gerr)
		}
		if j.State == sqlite.JobCancelled {
			return nil
		}
		return domain.E("JOB_NOT_ACTIVE", 409,
			"job %q is %s and can no longer be cancelled", jobID, j.State)
	}
	return nil
}

// RecoverInterrupted marks queued and running jobs INTERRUPTED. It is called
// once during service startup, never from StartJob: a new job must not rewrite
// another live runner's row.
func (p *PrewarmService) RecoverInterrupted(ctx context.Context) (int64, error) {
	return p.store.RecoverInterruptedJobs(ctx, "service_restart", p.Now())
}

// Shutdown cancels every runner owned by this process and waits, bounded by
// ctx, for their finalization. New jobs are rejected after shutdown starts.
func (p *PrewarmService) Shutdown(ctx context.Context) error {
	p.shutdownCancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		p.runsMu.Lock()
		n := len(p.runs)
		p.runsMu.Unlock()
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

// execute is the runner loop. Ready is recomputed from current evidence,
// filters, source state, and the project's fixed CD every round; the job ends
// on target, deadline, attempt budget, exhaustion, or cancellation.
func (p *PrewarmService) execute(ctx context.Context, jobID, projectID string, params prewarmParams) {
	now := p.Now()
	if err := p.store.StartJobRun(ctx, jobID, now); err != nil {
		// Cancelled before the runner claimed it: the row is already final.
		return
	}
	deadline := now.Add(time.Duration(params.DeadlineMS) * time.Millisecond)
	tried := map[string]bool{}
	probed, failed := 0, 0
	stop, lastErr := "", ""
	// persistProgress stores the recomputed ready count and real attempt
	// deltas. The loop stops when the row is already final (cancel won the
	// race) or persistence failed: a job whose progress cannot be written
	// must not keep probing silently.
	persistProgress := func(ready int, probedDelta, failedDelta int) bool {
		// A total deadline can fire while a batch is still writing evidence.
		// This tiny detached write is bounded and updates only RUNNING rows.
		writeCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		err := p.store.SetJobProgress(writeCtx, jobID, ready, probedDelta,
			failedDelta, "", p.Now())
		if err == nil {
			return true
		}
		var derr *domain.Error
		if errors.As(err, &derr) && derr.Code == "NOT_FOUND" {
			return false
		}
		lastErr = err.Error()
		stop = "progress_store_failed"
		return false
	}

	for {
		if ctx.Err() != nil {
			stop = sqlite.JobStopCanceled
			break
		}
		effective, derr := p.currentScope(ctx, projectID, params)
		if derr != nil {
			stop, lastErr = "project_scope_error", derr.Error()
			break
		}
		if effective == nil {
			stop = "project_state_changed"
			break
		}
		ready, rerr := p.currentReady(ctx, projectID, params, *effective, effective.ScopeHash)
		if rerr != nil {
			stop, lastErr = "error", rerr.Error()
			break
		}
		if !persistProgress(ready, 0, 0) {
			break
		}
		if ready >= params.Count {
			stop = "target_met"
			break
		}
		if !p.Now().Before(deadline) {
			stop = "deadline"
			break
		}
		if probed >= p.cfg.PrewarmMaxAttempts {
			stop = "attempt_limit"
			break
		}
		page, perr := p.probePage(ctx, projectID, params, *effective, effective.ScopeHash, tried)
		if perr != nil {
			stop, lastErr = "error", perr.Error()
			break
		}
		if budget := p.cfg.PrewarmMaxAttempts - probed; len(page) > budget {
			page = page[:budget]
		}
		if len(page) == 0 {
			if cooling, cerr := p.store.CountCoolingSources(ctx, p.Now()); cerr == nil && cooling > 0 {
				stop = "sources_cooling"
			} else {
				stop = "inventory_exhausted"
			}
			break
		}
		for _, cand := range page {
			tried[cand.Resource.ID] = true
		}
		batchProbed, batchFailed := p.probeBatch(ctx, jobID, page, effective.Validation,
			effective.ScopeHash)
		probed += batchProbed
		failed += batchFailed
		if !persistProgress(ready, batchProbed, batchFailed) {
			break
		}
		if batchProbed == 0 && ctx.Err() == nil {
			// Every probe in the page was rejected before I/O (probe-slot
			// starvation or reservation races); retrying the same page would
			// spin, so the job ends with its shortfall visible.
			stop = "no_probe_capacity"
			break
		}
		if ctx.Err() != nil {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				stop = "deadline"
			} else {
				stop = sqlite.JobStopCanceled
			}
			break
		}
	}

	// Final persistence must not inherit an expired job/probe deadline. It is
	// a short bounded write and only settles this already-running row.
	finalCtx, finalCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer finalCancel()
	// A cancel is already terminal; final readiness is still useful for a
	// normal deadline, but a failed final scope query must not overwrite the
	// cancellation reason.
	finalScope, sErr := p.currentScope(finalCtx, projectID, params)
	ready := 0
	var rerr error
	if sErr != nil {
		rerr = sErr
	} else if finalScope == nil {
		stop = "project_state_changed"
	} else {
		ready, rerr = p.currentReady(finalCtx, projectID, params, *finalScope,
			finalScope.ScopeHash)
	}
	if rerr != nil && lastErr == "" && stop != sqlite.JobStopCanceled {
		lastErr = rerr.Error()
	}
	state := sqlite.JobPartial
	switch {
	case stop == sqlite.JobStopCanceled:
		state = sqlite.JobCancelled
	case stop == "deadline":
		state = sqlite.JobPartial
	case lastErr != "":
		state = sqlite.JobFailed
	case ready >= params.Count:
		state = sqlite.JobCompleted
	}
	// A cancel that already finalized the row is the expected conflict; any
	// other FinishJob failure leaves the row active, and the next
	// RecoverInterruptedJobs pass marks it INTERRUPTED instead of silently
	// reporting a terminal state the store never recorded.
	for attempt := 0; attempt < 3; attempt++ {
		err := p.store.FinishJob(finalCtx, jobID, state, stop, lastErr, p.Now())
		if err == nil {
			return
		}
		var derr *domain.Error
		if errors.As(err, &derr) && derr.Code == "NOT_FOUND" {
			return
		}
		if finalCtx.Err() != nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// currentScope re-reads project enablement and allowlist before every page.
// The returned source set is nil for an unrestricted project, empty for a
// project that allows nothing, and otherwise the intersection with the
// request's requested sources. A disabled or removed project returns nil.
func (p *PrewarmService) currentScope(ctx context.Context, projectID string,
	params prewarmParams) (*prewarmScope, error) {
	proj, err := p.store.GetProject(ctx, projectID)
	if err != nil {
		return nil, err
	}
	if !proj.Enabled {
		return nil, nil
	}
	requested := params.Filters.SourceIDs
	allowed := proj.AllowedSourceIDs
	var sourceIDs []string
	switch {
	case allowed == nil:
		sourceIDs = requested
	case requested == nil:
		sourceIDs = append([]string(nil), allowed...)
	default:
		sourceIDs = make([]string, 0, len(requested))
		for _, sourceID := range requested {
			if proj.SourceAllowed(sourceID) {
				sourceIDs = append(sourceIDs, sourceID)
			}
		}
	}
	return &prewarmScope{
		SourceIDs:          sourceIDs,
		Transports:         append([]domain.Transport(nil), params.Filters.Transports...),
		Countries:          append([]string(nil), params.Filters.Countries...),
		Kinds:              append([]domain.ResourceKind(nil), params.Filters.ResourceKinds...),
		Validation:         params.Validation,
		ScopeHash:          domain.EvidenceScopeHash(params.Validation),
		SourceUnrestricted: sourceIDs == nil,
		SourceEmpty:        sourceIDs != nil && len(sourceIDs) == 0,
	}, nil
}

// currentReady counts inventory candidates that currently satisfy the job's
// scope: fresh PASS evidence for the exact scope/source version/incarnation,
// no fixed project-CD window on the connection, and matching filters.
func (p *PrewarmService) currentReady(ctx context.Context, projectID string,
	params prewarmParams, scope prewarmScope, scopeHash string) (int, error) {
	rows, err := p.store.SelectPrewarmCandidates(ctx, sqlite.PrewarmQuery{
		Profile:           params.Validation.Profile,
		Now:               p.Now(),
		Transports:        scope.Transports,
		Kinds:             scope.Kinds,
		SourceIDs:         scope.SourceIDs,
		Countries:         scope.Countries,
		ScopeUnrestricted: scope.SourceUnrestricted,
		ScopeEmpty:        scope.SourceEmpty,
		Limit:             maxPrewarmReadyScan,
	})
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	fresh, ferr := p.store.RecentEvidence(ctx, scopeHash, p.evidenceKeys(rows),
		p.Now().Add(-p.cfg.EffectiveEvidenceCacheTTL()))
	if ferr != nil {
		return 0, ferr
	}
	fps := make([]string, 0, len(rows))
	byFingerprint := map[string]domain.Candidate{}
	for _, c := range rows {
		fp := p.fingerprintConn(c.Resource.Conn)
		fps = append(fps, fp)
		byFingerprint[fp] = c
	}
	blocked, err := p.store.ProjectCooldownBlocked(ctx, projectID, p.Now(), fps)
	if err != nil {
		return 0, err
	}
	ready := 0
	for fp, c := range byFingerprint {
		if _, hit := fresh[fp]; !hit {
			continue
		}
		if _, cool := blocked[fp]; cool {
			continue
		}
		if !countryMatches(scope.Countries, c.Observation) {
			continue
		}
		ready++
	}
	return ready, nil
}

// probePage returns untried candidates that still need a live probe: no fresh
// PASS evidence and no fixed project-CD window.
func (p *PrewarmService) probePage(ctx context.Context, projectID string,
	params prewarmParams, scope prewarmScope, scopeHash string,
	tried map[string]bool) ([]domain.Candidate, error) {
	exclude := make([]string, 0, len(tried))
	for id := range tried {
		exclude = append(exclude, id)
	}
	rows, err := p.store.SelectPrewarmCandidates(ctx, sqlite.PrewarmQuery{
		Profile:           params.Validation.Profile,
		Now:               p.Now(),
		Transports:        scope.Transports,
		Kinds:             scope.Kinds,
		SourceIDs:         scope.SourceIDs,
		Countries:         scope.Countries,
		ScopeUnrestricted: scope.SourceUnrestricted,
		ScopeEmpty:        scope.SourceEmpty,
		ExcludeByIDs:      exclude,
		Limit:             prewarmPageLimit,
	})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	fresh, err := p.store.RecentEvidence(ctx, scopeHash, p.evidenceKeys(rows),
		p.Now().Add(-p.cfg.EffectiveEvidenceCacheTTL()))
	if err != nil {
		return nil, err
	}
	fps := make([]string, 0, len(rows))
	byFingerprint := map[string]domain.Candidate{}
	for _, c := range rows {
		fp := p.fingerprintConn(c.Resource.Conn)
		fps = append(fps, fp)
		byFingerprint[fp] = c
	}
	blocked, err := p.store.ProjectCooldownBlocked(ctx, projectID, p.Now(), fps)
	if err != nil {
		return nil, err
	}
	out := make([]domain.Candidate, 0, len(rows))
	for _, c := range rows {
		fp := p.fingerprintConn(c.Resource.Conn)
		if _, hit := fresh[fp]; hit {
			continue
		}
		if _, cool := blocked[fp]; cool {
			continue
		}
		out = append(out, c)
	}
	return out, nil
}

func (p *PrewarmService) evidenceKeys(rows []domain.Candidate) []sqlite.EvidenceKey {
	keys := make([]sqlite.EvidenceKey, 0, len(rows))
	for _, c := range rows {
		keys = append(keys, sqlite.EvidenceKey{
			ResourceID:          c.Resource.ID,
			Fingerprint:         p.fingerprintConn(c.Resource.Conn),
			SourceID:            c.Offer.SourceID,
			SourceConfigVersion: c.SourceConfigVersion,
			Incarnation:         c.Incarnation,
		})
	}
	return keys
}

func (p *PrewarmService) fingerprintConn(conn domain.Connection) string {
	return domain.ConnectionFingerprint(p.cfg.IdentitySecret, conn.Transport,
		conn.Host, conn.Port, conn.Username, conn.Password)
}

// probeBatch probes one bounded page with the shared probe-slot pool. The
// online acquire path always outranks prewarm for slots; prewarm only gets
// whatever capacity remains.
func (p *PrewarmService) probeBatch(ctx context.Context, jobID string,
	page []domain.Candidate, scope domain.ValidationScope, scopeHash string) (probed, failed int) {
	workers := p.cfg.ProbeSlots
	if workers > len(page) {
		workers = len(page)
	}
	if workers < 1 {
		workers = 1
	}
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		jobs    = make(chan domain.Candidate)
		stopped = make(chan struct{})
	)
	go func() {
		defer close(jobs)
		for _, cand := range page {
			select {
			case jobs <- cand:
			case <-stopped:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for cand := range jobs {
				if ctx.Err() != nil {
					return
				}
				attempted, pass := p.probeOne(ctx, jobID, scope, scopeHash, cand)
				if !attempted {
					continue
				}
				mu.Lock()
				probed++
				if !pass {
					failed++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	close(stopped)
	return probed, failed
}

// probeOne probes one candidate under the job's persisted scope. Every
// persistence write is incarnation-guarded so a concurrent delete + re-import
// cannot resurrect the retired object with stale evidence.
func (p *PrewarmService) probeOne(ctx context.Context, jobID string,
	scope domain.ValidationScope, scopeHash string, cand domain.Candidate) (attempted, pass bool) {
	select {
	case p.probeSlots <- struct{}{}:
		defer func() { <-p.probeSlots }()
	case <-ctx.Done():
		return false, false
	}
	now := p.Now()
	res, err := p.store.ReserveSlot(ctx, cand, sqlite.ReserveArgs{
		Owner:               jobID,
		Profile:             scope.Profile,
		Now:                 now,
		ReservationDeadline: now.Add(p.cfg.ProbeTotalTimeout() + 2*time.Second),
	})
	if err != nil {
		return false, false
	}
	defer func() { _ = p.store.ReleaseReservation(ctx, res.ReservationID, p.Now()) }()

	probeCtx, cancel := context.WithTimeout(ctx, p.cfg.ProbeTotalTimeout())
	ev, fail := validation.Probe(probeCtx, scope, res.Candidate.Resource.Conn,
		p.probeOptions(scope.Mode), jobID)
	cancel()
	if ctx.Err() != nil {
		// The job context ended while the probe was in flight. The I/O was
		// canceled, but this is not evidence about the candidate.
		return false, false
	}
	ev.Outcome = "PASS"
	detail := ""
	if fail != nil {
		ev.Outcome = "FAIL"
		detail = fail.Kind + ": " + fail.Detail
	}
	resourceID := res.Candidate.Resource.ID
	incarnation := res.Candidate.Incarnation
	if err := p.store.RecordAttemptAttributed(ctx, ev, resourceID, res.ContextKey,
		detail, incarnation, res.Candidate.Offer.SourceID,
		res.Candidate.SourceConfigVersion); err != nil {
		return true, false
	}
	fingerprint := p.fingerprintConn(res.Candidate.Resource.Conn)
	if fail == nil {
		_ = p.putCandidateEvidence(ctx, res.Candidate, scopeHash, ev)
		recordObservation(ctx, p.store, p.geo, resourceID, ev, p.Now())
		_ = p.health.RecordScoped(ctx, resourceID, res.ContextKey, scope.Profile,
			"PASS", incarnation)
		_, _ = p.store.RecordSourceOutcomeOwned(ctx, res.Candidate.Offer.SourceID,
			res.Candidate.SourceConfigVersion, "PASS", p.Now(), p.sourcePolicy)
		return true, true
	}
	if fail.Kind == validation.FailAuth {
		// Wrong credentials are final for this connection config: delete the
		// resource (no tombstone), drop its sticky snapshots, and expire its
		// cached evidence. Re-uploads return as unvalidated candidates.
		retired, rerr := p.store.RetireResourceOwned(ctx, resourceID,
			incarnation, res.Candidate.Offer.SourceID,
			res.Candidate.SourceConfigVersion, "AUTH_FAILED", p.Now())
		if rerr == nil && retired > 0 {
			_, _ = p.store.ExpireBindingsForFingerprints(ctx, []string{fingerprint}, p.Now())
			_ = p.store.InvalidateEvidence(ctx, []string{fingerprint})
		}
		return true, false
	}
	if fail.Attributed() {
		_ = p.health.RecordScoped(ctx, resourceID, res.ContextKey, scope.Profile,
			"FAIL", incarnation)
		_, _ = p.store.RecordSourceOutcomeOwned(ctx, res.Candidate.Offer.SourceID,
			res.Candidate.SourceConfigVersion, "FAIL", p.Now(), p.sourcePolicy)
		_ = p.putCandidateEvidence(ctx, res.Candidate, scopeHash, ev)
	}
	return true, false
}

func (p *PrewarmService) putCandidateEvidence(ctx context.Context,
	cand domain.Candidate, scopeHash string, ev domain.Evidence) error {
	return p.store.PutEvidenceProduction(ctx, sqlite.EvidenceKey{
		ResourceID:          cand.Resource.ID,
		Fingerprint:         p.fingerprintConn(cand.Resource.Conn),
		SourceID:            cand.Offer.SourceID,
		SourceConfigVersion: cand.SourceConfigVersion,
		Incarnation:         cand.Incarnation,
	}, scopeHash, ev)
}
