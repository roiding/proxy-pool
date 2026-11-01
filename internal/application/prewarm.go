package application

import (
	"context"
	"crypto/x509"
	"sync"
	"sync/atomic"
	"time"

	"proxypool/internal/config"
	"proxypool/internal/domain"
	"proxypool/internal/geoip"
	"proxypool/internal/storage/sqlite"
	"proxypool/internal/validation"
)

const maxPrewarmDeadline = 10 * time.Minute

type PrewarmRequest struct {
	Count      int     `json:"count"`
	Filters    Filters `json:"filters"`
	DeadlineMS int64   `json:"deadline_ms"`
}

type PrewarmStatus struct {
	Running    bool       `json:"running"`
	Target     int        `json:"target"`
	Ready      int        `json:"ready"`
	Probed     int        `json:"probed"`
	Failed     int        `json:"failed"`
	StopReason string     `json:"stop_reason,omitempty"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	LastError  string     `json:"last_error,omitempty"`
}

// PrewarmService fills a target number of recently validated resources
// before a business window. It only refreshes evidence and health; delivery
// still performs its own request-owned validation.
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

	mu     sync.Mutex
	status PrewarmStatus
}

func NewPrewarmService(store *sqlite.Store, cfg config.Config, judgeURL string,
	rootCAs *x509.CertPool, probeSlots chan struct{}, geo geoip.Resolver) *PrewarmService {
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
		Now: time.Now,
	}
}

func (p *PrewarmService) validate(req *PrewarmRequest) *domain.Error {
	if req.Count < 1 {
		req.Count = 1
	}
	if req.Count > p.cfg.PrewarmMaxAttempts {
		req.Count = p.cfg.PrewarmMaxAttempts
	}
	if req.DeadlineMS <= 0 {
		req.DeadlineMS = p.cfg.PrewarmDeadlineMS
	}
	if d := time.Duration(req.DeadlineMS) * time.Millisecond; d > maxPrewarmDeadline {
		req.DeadlineMS = maxPrewarmDeadline.Milliseconds()
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
	return nil
}

// Start launches one asynchronous prewarm run. Only one run may be active.
func (p *PrewarmService) Start(req PrewarmRequest) (bool, *domain.Error) {
	if derr := p.validate(&req); derr != nil {
		return false, derr
	}
	if !p.tryBegin(req.Count) {
		return false, domain.E("PREWARM_IN_PROGRESS", 409,
			"another prewarm run is already active")
	}
	go func() {
		_, err := p.run(context.Background(), req)
		p.finish(err)
	}()
	return true, nil
}

// RunOnce performs a synchronous prewarm run, used by tests and operators
// who need the result in-band.
func (p *PrewarmService) RunOnce(ctx context.Context, req PrewarmRequest) (PrewarmStatus, error) {
	if derr := p.validate(&req); derr != nil {
		return PrewarmStatus{}, derr
	}
	if !p.tryBegin(req.Count) {
		return p.Status(), domain.E("PREWARM_IN_PROGRESS", 409, "another prewarm run is already active")
	}
	_, err := p.run(ctx, req)
	p.finish(err)
	return p.Status(), err
}

func (p *PrewarmService) tryBegin(target int) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.status.Running {
		return false
	}
	now := p.Now()
	p.status = PrewarmStatus{Running: true, Target: target, StartedAt: &now}
	return true
}

func (p *PrewarmService) finish(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.Now()
	p.status.Running = false
	p.status.FinishedAt = &now
	if err != nil && p.status.LastError == "" {
		p.status.LastError = err.Error()
	}
}

func (p *PrewarmService) Status() PrewarmStatus {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.status
}

func (p *PrewarmService) addProgress(ready, probed, failed int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.status.Ready += ready
	p.status.Probed += probed
	p.status.Failed += failed
}

func (p *PrewarmService) setError(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.status.LastError = err.Error()
}

func (p *PrewarmService) setStopReason(reason string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.status.StopReason = reason
}

func (p *PrewarmService) run(ctx context.Context, req PrewarmRequest) (PrewarmStatus, error) {
	runID := domain.NewID("warm_")
	deadline := p.Now().Add(time.Duration(req.DeadlineMS) * time.Millisecond)
	processed := map[string]bool{}
	var ready atomic.Int32
	var firstErr error

	for ready.Load() < int32(req.Count) {
		if ctx.Err() != nil || p.Now().After(deadline) {
			p.setStopReason("deadline")
			break
		}
		if p.Status().Probed >= p.cfg.PrewarmMaxAttempts {
			p.setStopReason("attempt_limit")
			break
		}
		cands, err := p.store.SelectCandidates(ctx, sqlite.SelectionFilters{
			Profile:      validation.ProfileHTTPSBasicV1,
			Now:          p.Now(),
			UsefulCutoff: p.Now(),
			Transports:   req.Filters.Transports,
			Kinds:        req.Filters.ResourceKinds,
			SourceIDs:    req.Filters.SourceIDs,
			Countries:    req.Filters.Countries,
			ExcludeByIDs: mapKeys(processed),
			Limit:        50,
		})
		if err != nil {
			firstErr = err
			p.setError(err)
			p.setStopReason("error")
			break
		}
		if len(cands) == 0 {
			if cooling, err := p.store.CountCoolingSources(ctx, p.Now()); err == nil && cooling > 0 {
				p.setStopReason("sources_cooling")
			} else {
				p.setStopReason("inventory_exhausted")
			}
			break
		}
		for _, cand := range cands {
			processed[cand.Resource.ID] = true
		}
		workers := p.cfg.ProbeSlots
		if workers > len(cands) {
			workers = len(cands)
		}
		if workers < 1 {
			workers = 1
		}
		var wg sync.WaitGroup
		var mu sync.Mutex
		jobs := make(chan domain.Candidate)
		stop := make(chan struct{})
		go func() {
			defer close(jobs)
			for _, cand := range cands {
				select {
				case jobs <- cand:
				case <-stop:
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
					if ready.Load() >= int32(req.Count) || ctx.Err() != nil ||
						p.Now().After(deadline) || p.Status().Probed >= p.cfg.PrewarmMaxAttempts {
						return
					}
					pass, probed, err := p.probeOne(ctx, runID, cand)
					if err != nil {
						mu.Lock()
						if firstErr == nil {
							firstErr = err
						}
						mu.Unlock()
					}
					if probed {
						failed := 0
						if pass {
							ready.Add(1)
						} else {
							failed = 1
						}
						p.addProgress(boolInt(pass), 1, failed)
					}
				}
			}()
		}
		wg.Wait()
		close(stop)
		if firstErr != nil {
			break
		}
	}
	if ready.Load() >= int32(req.Count) {
		p.setStopReason("target_met")
	}
	p.retireDead(ctx)
	return p.Status(), firstErr
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// probeOne reserves the candidate (so acquire cannot probe it concurrently),
// probes once, records evidence/health/source outcome, and releases.
func (p *PrewarmService) probeOne(ctx context.Context, runID string, cand domain.Candidate) (bool, bool, error) {
	select {
	case p.probeSlots <- struct{}{}:
		defer func() { <-p.probeSlots }()
	case <-ctx.Done():
		return false, false, ctx.Err()
	}
	now := p.Now()
	res, err := p.store.ReserveSlot(ctx, cand, sqlite.ReserveArgs{
		Owner:               runID,
		Profile:             validation.ProfileHTTPSBasicV1,
		Now:                 now,
		ReservationDeadline: now.Add(p.cfg.ProbeTotalTimeout() + 2*time.Second),
	})
	if err != nil {
		return false, false, nil
	}
	defer func() {
		_ = p.store.ReleaseReservation(ctx, res.ReservationID, p.Now())
	}()

	probeCtx, cancel := context.WithTimeout(ctx, p.cfg.ProbeTotalTimeout())
	ev, fail := validation.ProbeHTTPSBasic(probeCtx, res.Candidate.Resource.Conn, p.judgeURL, validation.Limits{
		DialTimeout:  p.cfg.ConnectTimeout(),
		ProbeTotal:   p.cfg.ProbeTotalTimeout(),
		AllowPrivate: p.cfg.AllowPrivateAddresses,
		RootCAs:      p.rootCAs,
		JudgeSecret:  p.cfg.JudgeSecret,
	}, runID)
	cancel()
	outcome := "PASS"
	detail := ""
	if fail != nil {
		outcome = "FAIL"
		detail = fail.Kind + ": " + fail.Detail
	}
	ev.Outcome = outcome
	if err := p.store.RecordAttempt(ctx, ev, res.Candidate.Resource.ID, res.ContextKey, detail); err != nil {
		return false, true, err
	}
	if fail == nil {
		recordObservation(ctx, p.store, p.geo, res.Candidate.Resource.ID, ev, p.Now())
		_ = p.health.Record(ctx, res.Candidate.Resource.ID, res.ContextKey,
			validation.ProfileHTTPSBasicV1, "PASS")
		_, _ = p.store.RecordSourceOutcome(ctx, res.Candidate.Offer.SourceID, "PASS",
			p.Now(), p.sourcePolicy)
		return true, true, nil
	}
	if fail.Kind == validation.FailAuth {
		_, _ = p.store.RetireResource(ctx, res.Candidate.Resource.ID, "AUTH_FAILED", p.Now())
	}
	if fail.Attributed() {
		_ = p.health.Record(ctx, res.Candidate.Resource.ID, res.ContextKey,
			validation.ProfileHTTPSBasicV1, "FAIL")
		_, _ = p.store.RecordSourceOutcome(ctx, res.Candidate.Offer.SourceID, "FAIL",
			p.Now(), p.sourcePolicy)
	}
	return false, true, nil
}

// retireDead applies the same consecutive-failure threshold used elsewhere
// so a prewarm pass also cleans up supply it just proved dead.
func (p *PrewarmService) retireDead(ctx context.Context) {
	if p.cfg.NightlyRetireFailures <= 0 {
		return
	}
	for _, adapter := range []string{AdapterCustom, AdapterProvider} {
		ids, err := p.store.RetireCandidates(ctx, p.cfg.NightlyRetireFailures, adapter, "")
		if err != nil {
			continue
		}
		for _, id := range ids {
			_, _ = p.store.RetireResource(ctx, id, "EVICTED", p.Now())
		}
	}
}
