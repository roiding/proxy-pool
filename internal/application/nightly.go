package application

import (
	"context"
	"crypto/x509"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"proxypool/internal/config"
	"proxypool/internal/domain"
	"proxypool/internal/geoip"
	"proxypool/internal/storage/sqlite"
	"proxypool/internal/validation"
)

var ErrValidationRunning = errors.New("a validation pass is already running")

type ValidationStatus struct {
	Running        bool       `json:"running"`
	Mode           string     `json:"mode,omitempty"`
	SourceID       string     `json:"source_id,omitempty"`
	StartedAt      *time.Time `json:"started_at,omitempty"`
	Probed         int64      `json:"probed"`
	Retired        int64      `json:"retired"`
	LastError      string     `json:"last_error,omitempty"`
	LastFinishedAt *time.Time `json:"last_finished_at,omitempty"`
}

// NightlyValidator validates self-uploaded (custom) sources: one full pass per
// night at the configured time, plus operator-triggered passes for one source.
// Successful exact-scope proof is retained for later requests. Completed
// failed validations remove uploaded supply; cancellation is not a verdict.
// The validator never delivers connections or writes client cooldown.
type NightlyValidator struct {
	store      *sqlite.Store
	cfg        config.Config
	judgeURL   string
	rootCAs    *x509.CertPool
	health     *HealthRecorder
	geo        geoip.Resolver
	probeSlots chan struct{}

	statusMu sync.Mutex
	status   ValidationStatus
	Now      func() time.Time

	runMu     sync.Mutex
	runCancel context.CancelFunc
	runDone   chan struct{}
	shutdown  bool
}

func NewNightlyValidator(store *sqlite.Store, cfg config.Config, judgeURL string,
	rootCAs *x509.CertPool, cooldown CooldownConfig, probeSlots chan struct{},
	geo geoip.Resolver) *NightlyValidator {
	return &NightlyValidator{
		store:      store,
		cfg:        cfg,
		judgeURL:   judgeURL,
		rootCAs:    rootCAs,
		health:     &HealthRecorder{Store: store, Cooldown: cooldown, Now: time.Now},
		geo:        geo,
		probeSlots: probeSlots,
		Now:        time.Now,
	}
}

func (n *NightlyValidator) tryBegin(mode, sourceID string) bool {
	n.statusMu.Lock()
	defer n.statusMu.Unlock()
	if n.status.Running {
		return false
	}
	now := n.Now()
	n.status = ValidationStatus{Running: true, Mode: mode, SourceID: sourceID, StartedAt: &now}
	return true
}

func (n *NightlyValidator) addProgress(probed, retired int64) {
	n.statusMu.Lock()
	defer n.statusMu.Unlock()
	if n.status.Running {
		n.status.Probed += probed
		n.status.Retired += retired
	}
}

func (n *NightlyValidator) finish(passErr error) {
	n.statusMu.Lock()
	defer n.statusMu.Unlock()
	now := n.Now()
	n.status.Running = false
	n.status.LastFinishedAt = &now
	if passErr != nil {
		n.status.LastError = passErr.Error()
	}
}

func (n *NightlyValidator) Status() ValidationStatus {
	n.statusMu.Lock()
	defer n.statusMu.Unlock()
	return n.status
}

// RunOnce performs a synchronous full pass over every custom source.
func (n *NightlyValidator) RunOnce(ctx context.Context) (int, int, error) {
	if !n.tryBegin("nightly", "") {
		return 0, 0, ErrValidationRunning
	}
	probed, retired, err := n.runPass(ctx, "")
	n.finish(err)
	return probed, retired, err
}

// RunSourceOnce performs a synchronous pass over one custom source.
func (n *NightlyValidator) RunSourceOnce(ctx context.Context, sourceID string) (int, int, error) {
	if !n.tryBegin("manual", sourceID) {
		return 0, 0, ErrValidationRunning
	}
	probed, retired, err := n.runPass(ctx, sourceID)
	n.finish(err)
	return probed, retired, err
}

// StartSourceRun kicks off an asynchronous pass over one custom source. It
// returns false when another pass (nightly or manual) is already running.
func (n *NightlyValidator) StartSourceRun(sourceID string) (bool, error) {
	if !n.tryBegin("manual", sourceID) {
		return false, nil
	}
	n.runMu.Lock()
	if n.shutdown {
		n.runMu.Unlock()
		n.finish(errors.New("service shutting down"))
		return false, errors.New("service shutting down")
	}
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	n.runCancel, n.runDone = cancel, done
	n.runMu.Unlock()
	go func() {
		defer close(done)
		_, _, err := n.runPass(runCtx, sourceID)
		n.runMu.Lock()
		if n.runDone == done {
			n.runCancel, n.runDone = nil, nil
		}
		n.runMu.Unlock()
		n.finish(err)
	}()
	return true, nil
}

// Shutdown cancels the current async source pass and waits for workers to
// leave the probe path. Synchronous RunOnce callers own their own context.
func (n *NightlyValidator) Shutdown(ctx context.Context) error {
	n.runMu.Lock()
	n.shutdown = true
	cancel, done := n.runCancel, n.runDone
	n.runMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// runPass pages through the eligible candidates of the given source ("" = all
// custom sources), probing each one. Callers must have called tryBegin.
func (n *NightlyValidator) runPass(ctx context.Context, sourceID string) (int, int, error) {
	runID := domain.NewID("val_")
	processed := map[string]bool{}
	var retired int
	var probedCount atomic.Int64
	for {
		if ctx.Err() != nil {
			return int(probedCount.Load()), retired, ctx.Err()
		}
		candidates, err := n.store.SelectPeriodicCandidates(ctx, sqlite.PeriodicFilters{
			Profile:      validation.ProfileHTTPSBasicV1,
			Now:          n.Now(),
			Limit:        100,
			SourceID:     sourceID,
			ExcludeByIDs: mapKeys(processed),
		})
		if err != nil {
			return int(probedCount.Load()), retired, err
		}
		if len(candidates) == 0 {
			return int(probedCount.Load()), retired, nil
		}
		var wg sync.WaitGroup
		var errMu sync.Mutex
		var firstErr error
		workers := n.cfg.ProbeSlots
		if workers > len(candidates) {
			workers = len(candidates)
		}
		if workers < 1 {
			workers = 1
		}
		jobs := make(chan domain.Candidate)
		go func() {
			defer close(jobs)
			for _, cand := range candidates {
				select {
				case jobs <- cand:
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
					if err := n.probeOne(ctx, runID, cand); err != nil {
						errMu.Lock()
						if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
							// worker canceled before probing; not an attempt
						} else if firstErr == nil {
							firstErr = err
						}
						errMu.Unlock()
						if ctx.Err() != nil {
							continue
						}
					}
					probedCount.Add(1)
					n.addProgress(1, 0)
				}
			}()
		}
		wg.Wait()
		retired = int(n.Status().Retired)
		for _, cand := range candidates {
			processed[cand.Resource.ID] = true
		}
		if firstErr != nil {
			return int(probedCount.Load()), retired, firstErr
		}
	}
}

func mapKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func (n *NightlyValidator) probeOne(ctx context.Context, runID string, cand domain.Candidate) error {
	select {
	case n.probeSlots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-n.probeSlots }()
	contextKey := domain.ContextKey(cand.Resource.CredentialVersion, cand.Resource.Session)
	probeCtx, cancel := context.WithTimeout(ctx, n.cfg.ProbeTotalTimeout())
	opts := validation.ProbeOptions{JudgeURL: n.judgeURL, Limits: validation.ProbeLimits{
		DialTimeout:   n.cfg.ConnectTimeout(),
		ProbeTotal:    n.cfg.ProbeTotalTimeout(),
		AllowPrivate:  n.cfg.AllowPrivateAddresses,
		RootCAs:       n.rootCAs,
		JudgeSecret:   n.cfg.JudgeSecret,
		CAFingerprint: judgeCAPingerprint(n.cfg.JudgeCAPem),
	}}
	scope := domain.ValidationScope{Mode: "basic", Profile: validation.ProfileHTTPSBasicV1,
		SecurityHash: validation.ProbeSecurityHash(opts)}
	ev, fail := validation.Probe(probeCtx, scope, cand.Resource.Conn, opts, runID)
	cancel()
	outcome := "PASS"
	detail := ""
	if fail != nil {
		outcome = "FAIL"
		detail = fail.Kind + ": " + fail.Detail
	}
	ev.Outcome = outcome
	if fail == nil {
		recordObservation(ctx, n.store, n.geo, cand.Resource.ID, ev, n.Now())
	}
	// A canceled pass must not write a synthetic timeout attempt or stale
	// health. The probe was interrupted before a real proxy verdict.
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err := n.store.RecordAttemptAttributed(ctx, ev, cand.Resource.ID, contextKey,
		detail, cand.Incarnation, cand.Offer.SourceID,
		cand.SourceConfigVersion); err != nil {
		return err
	}
	if fail == nil {
		fingerprint := domain.ConnectionFingerprint(n.cfg.IdentitySecret,
			cand.Resource.Conn.Transport, cand.Resource.Conn.Host, cand.Resource.Conn.Port,
			cand.Resource.Conn.Username, cand.Resource.Conn.Password)
		if err := n.store.PutEvidenceProduction(ctx, sqlite.EvidenceKey{ResourceID: cand.Resource.ID,
			Fingerprint: fingerprint, SourceID: cand.Offer.SourceID,
			SourceConfigVersion: cand.SourceConfigVersion, Incarnation: cand.Incarnation},
			domain.EvidenceScopeHash(scope), ev); err != nil {
			return err
		}
	} else if probeHasVerdict(ctx, fail) {
		if err := retireFailedCandidate(ctx, n.store, nil, cand, n.cfg.IdentitySecret, n.Now(), DefaultAcquireProject); err != nil {
			return err
		}
		n.addProgress(0, 1)
	}
	if fail == nil || fail.Attributed() {
		return n.health.RecordScoped(ctx, cand.Resource.ID, contextKey,
			validation.ProfileHTTPSBasicV1, outcome, cand.Incarnation)
	}
	return nil
}

// Run blocks until ctx is done, executing one full pass per day at hh:mm local.
func (n *NightlyValidator) Run(ctx context.Context, at string, log *slog.Logger) {
	for {
		next := nextRunTime(at, time.Now())
		timer := time.NewTimer(next.Sub(time.Now()))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		start := time.Now()
		probed, retired, err := n.RunOnce(ctx)
		if err != nil && ctx.Err() == nil {
			log.Error("nightly validation pass failed", "error", err)
			continue
		}
		log.Info("nightly validation pass finished", "probed", probed, "retired", retired,
			"duration_ms", time.Since(start).Milliseconds())
	}
}

func nextRunTime(hhmm string, now time.Time) time.Time {
	t, err := time.Parse("15:04", hhmm)
	if err != nil {
		t, _ = time.Parse("15:04", "03:00")
	}
	next := time.Date(now.Year(), now.Month(), now.Day(), t.Hour(), t.Minute(), 0, 0, now.Location())
	if !next.After(now) {
		next = next.AddDate(0, 0, 1)
	}
	return next
}
