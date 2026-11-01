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
// It refreshes health evidence, retires repeatedly dead proxies, and never
// authorizes a delivery: every acquire still performs its own fresh validation.
type NightlyValidator struct {
	store          *sqlite.Store
	cfg            config.Config
	judgeURL       string
	rootCAs        *x509.CertPool
	health         *HealthRecorder
	probeSlots     chan struct{}
	retireFailures int

	statusMu sync.Mutex
	status   ValidationStatus
	Now      func() time.Time
}

func NewNightlyValidator(store *sqlite.Store, cfg config.Config, judgeURL string,
	rootCAs *x509.CertPool, cooldown CooldownConfig, probeSlots chan struct{}) *NightlyValidator {
	return &NightlyValidator{
		store:          store,
		cfg:            cfg,
		judgeURL:       judgeURL,
		rootCAs:        rootCAs,
		health:         &HealthRecorder{Store: store, Cooldown: cooldown, Now: time.Now},
		probeSlots:     probeSlots,
		retireFailures: cfg.NightlyRetireFailures,
		Now:            time.Now,
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
	go func() {
		_, _, err := n.runPass(context.Background(), sourceID)
		n.finish(err)
	}()
	return true, nil
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
					}
					probedCount.Add(1)
					n.addProgress(1, 0)
				}
			}()
		}
		wg.Wait()
		for _, cand := range candidates {
			processed[cand.Resource.ID] = true
		}
		pageRetired := n.retireFailed(ctx, sourceID)
		retired += pageRetired
		n.addProgress(0, int64(pageRetired))
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
	ev, fail := validation.ProbeHTTPSBasic(probeCtx, cand.Resource.Conn, n.judgeURL, validation.Limits{
		DialTimeout:  n.cfg.ConnectTimeout(),
		ProbeTotal:   n.cfg.ProbeTotalTimeout(),
		AllowPrivate: n.cfg.AllowPrivateAddresses,
		RootCAs:      n.rootCAs,
		JudgeSecret:  n.cfg.JudgeSecret,
	}, runID)
	cancel()
	outcome := "PASS"
	detail := ""
	if fail != nil {
		outcome = "FAIL"
		detail = fail.Kind + ": " + fail.Detail
	}
	ev.Outcome = outcome
	if err := n.store.RecordAttempt(ctx, ev, cand.Resource.ID, contextKey, detail); err != nil {
		return err
	}
	// Wrong/expired credentials are final evidence: retire immediately
	// instead of waiting for the consecutive-failure threshold.
	if fail != nil && fail.Kind == validation.FailAuth {
		if count, err := n.store.RetireResource(ctx, cand.Resource.ID, "AUTH_FAILED", n.Now()); err == nil && count > 0 {
			n.addProgress(0, int64(count))
		}
	}
	if fail == nil || fail.Attributed() {
		return n.health.Record(ctx, cand.Resource.ID, contextKey,
			validation.ProfileHTTPSBasicV1, outcome)
	}
	return nil
}

// retireFailed removes resources that reached the consecutive-failure
// threshold. Tombstones prevent the same uploaded list from reviving them.
func (n *NightlyValidator) retireFailed(ctx context.Context, sourceID string) int {
	if n.retireFailures <= 0 {
		return 0
	}
	resourceIDs, err := n.store.RetireCandidates(ctx, n.retireFailures, sourceID)
	if err != nil {
		return 0
	}
	retired := 0
	for _, resourceID := range resourceIDs {
		if count, err := n.store.RetireResource(ctx, resourceID, "EVICTED", n.Now()); err == nil {
			retired += count
		}
	}
	return retired
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
