package application

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"proxypool/internal/config"
	"proxypool/internal/domain"
	"proxypool/internal/storage/sqlite"
	"proxypool/internal/validation"
)

type AcquireRequest struct {
	Count        int         `json:"count"`
	Profile      string      `json:"profile"`
	Filters      Filters     `json:"filters"`
	Lease        LeasePolicy `json:"lease"`
	DeadlineMS   int64       `json:"deadline_ms"`
	AllowPartial *bool       `json:"allow_partial"`
}

type Filters struct {
	Transports    []domain.Transport    `json:"transports"`
	ResourceKinds []domain.ResourceKind `json:"resource_kinds"`
	SourceIDs     []string              `json:"source_ids"`
}

type LeasePolicy struct {
	Mode        string `json:"mode"`
	TTLMS       int64  `json:"ttl_ms"`
	MinUsableMS int64  `json:"min_usable_ms"`
}

type AcquiredItem struct {
	LeaseID    string            `json:"lease_id"`
	ResourceID string            `json:"resource_id"`
	SourceID   string            `json:"source_id"`
	Conn       domain.Connection `json:"connection"`
	Validation domain.Evidence   `json:"validation"`
	ExpiresAt  time.Time         `json:"lease_expires_at"`
}

type AcquireResult struct {
	RequestID       string         `json:"request_id"`
	Requested       int            `json:"requested"`
	Delivered       int            `json:"delivered"`
	Missing         int            `json:"missing"`
	Partial         bool           `json:"partial"`
	Items           []AcquiredItem `json:"items"`
	ShortfallReason string         `json:"shortfall_reason,omitempty"`
}

type AcquireService struct {
	store    *sqlite.Store
	cfg      config.Config
	judgeURL string
	rootCAs  *x509.CertPool
	health   *HealthRecorder
	Now      func() time.Time

	acquireSlots chan struct{}
	probeSlots   chan struct{}
}

func NewAcquireService(store *sqlite.Store, cfg config.Config, judgeURL string,
	rootCAs *x509.CertPool, probeSlots chan struct{}) *AcquireService {
	return &AcquireService{
		store:        store,
		cfg:          cfg,
		judgeURL:     judgeURL,
		rootCAs:      rootCAs,
		health:       newHealthRecorder(store, cfg),
		acquireSlots: make(chan struct{}, cfg.MaxActiveAcquires),
		probeSlots:   probeSlots,
		Now:          time.Now,
	}
}

func (s *AcquireService) validateRequest(req *AcquireRequest) *domain.Error {
	if req.Count < 1 {
		req.Count = 1
	}
	if req.Count > s.cfg.MaxCount {
		req.Count = s.cfg.MaxCount
	}
	if req.Profile == "" {
		req.Profile = validation.ProfileHTTPSBasicV1
	}
	if req.Profile != validation.ProfileHTTPSBasicV1 {
		return domain.UnsupportedProfile(req.Profile)
	}
	for _, t := range req.Filters.Transports {
		if t != domain.TransportHTTP {
			return domain.UnsupportedTransport(string(t))
		}
	}
	if len(req.Filters.Transports) == 0 {
		req.Filters.Transports = []domain.Transport{domain.TransportHTTP}
	}
	for _, k := range req.Filters.ResourceKinds {
		switch k {
		case domain.KindStatic, domain.KindSticky, domain.KindRotating:
		default:
			return domain.InvalidRequest("unknown resource_kind %q", string(k))
		}
	}
	if req.Lease.Mode == "" {
		req.Lease.Mode = "exclusive"
	}
	if req.Lease.TTLMS <= 0 {
		req.Lease.TTLMS = s.cfg.LeaseTTLMS
	}
	if req.Lease.TTLMS > s.cfg.MaxLeaseTTLMS {
		req.Lease.TTLMS = s.cfg.MaxLeaseTTLMS
	}
	if req.Lease.MinUsableMS <= 0 {
		req.Lease.MinUsableMS = s.cfg.MinUsableMS
	}
	if req.DeadlineMS <= 0 {
		req.DeadlineMS = s.cfg.AcquireDeadlineMS
	}
	if req.DeadlineMS > config.MaxAcquireDeadlineMS {
		req.DeadlineMS = config.MaxAcquireDeadlineMS
	}
	return nil
}

func (s *AcquireService) Acquire(ctx context.Context, req AcquireRequest) (AcquireResult, *domain.Error) {
	if derr := s.validateRequest(&req); derr != nil {
		return AcquireResult{}, derr
	}
	select {
	case s.acquireSlots <- struct{}{}:
		defer func() { <-s.acquireSlots }()
	default:
		return AcquireResult{}, domain.AcquireCapacity()
	}
	requestID := domain.NewID("req_")
	deadline := s.Now().Add(time.Duration(req.DeadlineMS) * time.Millisecond)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	run := &acquireRun{
		maxAttempts: maxCandidateAttempts(req.Count, s.cfg.MaxCandidateAttempts),
	}
	candidates := make(chan domain.Candidate, s.cfg.CandidatePage)
	var workers sync.WaitGroup
	workerCount := s.cfg.ParallelPerRequest
	if workerCount > req.Count {
		workerCount = req.Count
	}
	if workerCount < 1 {
		workerCount = 1
	}
	for i := 0; i < workerCount; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			s.workerLoop(ctx, run, candidates, req, requestID, deadline)
		}()
	}
	s.feedCandidates(ctx, run, candidates, req)
	workers.Wait()

	items := s.dispatchGuard(ctx, run, deadline)
	result := AcquireResult{
		RequestID: requestID,
		Requested: req.Count,
		Delivered: len(items),
		Missing:   req.Count - len(items),
		Items:     items,
	}
	result.Partial = len(items) > 0 && len(items) < req.Count && req.AllowPartial != nil && *req.AllowPartial
	if len(items) == 0 {
		return result, run.zeroShortfall()
	}
	if len(items) < req.Count {
		result.ShortfallReason = run.shortfallReason()
		if req.AllowPartial == nil || !*req.AllowPartial {
			for _, it := range items {
				_, _ = s.store.SetLeaseState(ctx, it.LeaseID, "EXPIRED", s.Now())
			}
			result.Items = nil
			result.Delivered = 0
			result.Partial = false
			return result, domain.InsufficientValidated(
				fmt.Sprintf("validated only %d of %d before deadline (%s)",
					len(items), req.Count, result.ShortfallReason))
		}
	}
	return result, nil
}

func maxCandidateAttempts(count, cap int) int {
	n := 5 * count
	if n < 5 {
		n = 5
	}
	if n > cap {
		n = cap
	}
	return n
}

type acquireRun struct {
	maxAttempts  int
	attempts     atomic.Int32
	delivered    atomic.Int32
	tried        sync.Map
	mu           sync.Mutex
	leases       []AcquiredItem
	failCounts   map[string]int
	inconclusive int
	attributed   int
	sawCandidate atomic.Bool
}

func (r *acquireRun) recordFailure(kind string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failCounts == nil {
		r.failCounts = map[string]int{}
	}
	r.failCounts[kind]++
	if kind == validation.FailJudge || kind == validation.FailBudget {
		r.inconclusive++
	} else {
		r.attributed++
	}
}

func (r *acquireRun) shortfallReason() string {
	return reasonFor(r.inconclusive, r.attributed)
}

func reasonFor(inconclusive, attributed int) string {
	if inconclusive > 0 && attributed == 0 {
		return "VALIDATION_INFRA_UNAVAILABLE"
	}
	if attributed > 0 {
		return "NO_VALIDATED_RESOURCES"
	}
	return "VALIDATION_BUDGET_EXHAUSTED"
}

func (r *acquireRun) zeroShortfall() *domain.Error {
	r.mu.Lock()
	inconclusive, attributed := r.inconclusive, r.attributed
	r.mu.Unlock()
	sawAny := r.sawCandidate.Load()
	if !sawAny {
		return domain.NoEligibleCandidates("inventory empty or exhausted")
	}
	if reasonFor(inconclusive, attributed) == "VALIDATION_INFRA_UNAVAILABLE" {
		return domain.ValidationInfra("all probes inconclusive")
	}
	return domain.InsufficientValidated(
		fmt.Sprintf("no candidate passed validation (%s)", reasonFor(inconclusive, attributed)))
}

func (s *AcquireService) feedCandidates(ctx context.Context, run *acquireRun, out chan<- domain.Candidate, req AcquireRequest) {
	defer close(out)
	for int(run.attempts.Load()) < run.maxAttempts {
		if ctx.Err() != nil {
			return
		}
		if run.delivered.Load() >= int32(req.Count) {
			return
		}
		exclude := run.excludeList()
		cands, err := s.store.SelectCandidates(ctx, sqlite.SelectionFilters{
			Profile:      req.Profile,
			Now:          s.Now(),
			UsefulCutoff: s.Now().Add(time.Duration(req.Lease.MinUsableMS)*time.Millisecond + s.cfg.ExpirySafetyMargin()),
			Transports:   req.Filters.Transports,
			Kinds:        req.Filters.ResourceKinds,
			SourceIDs:    req.Filters.SourceIDs,
			ExcludeByIDs: exclude,
			Limit:        s.cfg.CandidatePage,
		})
		if err != nil {
			return
		}
		if len(cands) == 0 {
			return
		}
		for _, c := range cands {
			if _, loaded := run.tried.LoadOrStore(c.Resource.ID, struct{}{}); loaded {
				continue
			}
			run.sawCandidate.Store(true)
			run.attempts.Add(1)
			select {
			case out <- c:
			case <-ctx.Done():
				return
			}
			if run.delivered.Load() >= int32(req.Count) {
				return
			}
		}
	}
}

func (r *acquireRun) excludeList() []string {
	var ids []string
	r.tried.Range(func(key, _ any) bool {
		if id, ok := key.(string); ok {
			ids = append(ids, id)
		}
		return true
	})
	return ids
}

func (s *AcquireService) workerLoop(ctx context.Context, run *acquireRun, candidates <-chan domain.Candidate, req AcquireRequest, requestID string, requestDeadline time.Time) {
	for cand := range candidates {
		if ctx.Err() != nil {
			return
		}
		if run.delivered.Load() >= int32(req.Count) {
			return
		}
		s.acquireSlotsProbe(ctx, func() {
			s.processCandidate(ctx, run, cand, req, requestID, requestDeadline)
		})
		if run.delivered.Load() >= int32(req.Count) {
			return
		}
	}
}

func (s *AcquireService) acquireSlotsProbe(ctx context.Context, fn func()) {
	select {
	case s.probeSlots <- struct{}{}:
		defer func() { <-s.probeSlots }()
		fn()
	case <-ctx.Done():
	}
}

func (s *AcquireService) processCandidate(ctx context.Context, run *acquireRun, cand domain.Candidate, req AcquireRequest, requestID string, requestDeadline time.Time) {
	now := s.Now()
	res, err := s.store.ReserveSlot(ctx, cand, sqlite.ReserveArgs{
		Owner:               requestID,
		Profile:             req.Profile,
		Now:                 now,
		ReservationDeadline: requestDeadline.Add(2 * time.Second),
	})
	if err != nil {
		return
	}
	probeCtx, cancelProbe := context.WithTimeout(ctx, s.cfg.ProbeTotalTimeout())
	ev, fail := validation.ProbeHTTPSBasic(probeCtx, res.Candidate.Resource.Conn, s.judgeURL, validation.Limits{
		DialTimeout:  s.cfg.ConnectTimeout(),
		ProbeTotal:   s.cfg.ProbeTotalTimeout(),
		AllowPrivate: s.cfg.AllowPrivateAddresses,
		RootCAs:      s.rootCAs,
		JudgeSecret:  s.cfg.JudgeSecret,
	}, requestID)
	cancelProbe()
	if derr := recordAttempt(ctx, s.store, ev, res, fail); derr != nil {
		_ = derr
	}
	if fail == nil {
		item, cerr := s.commitLease(ctx, res, ev, req, requestID)
		if cerr == nil {
			run.delivered.Add(1)
			run.mu.Lock()
			run.leases = append(run.leases, item)
			run.mu.Unlock()
			return
		}
		_ = s.store.ReleaseReservation(ctx, res.ReservationID, s.Now())
		return
	}
	_ = s.store.ReleaseReservation(ctx, res.ReservationID, s.Now())
	run.recordFailure(fail.Kind)
	if fail.Attributed() {
		_ = s.health.Record(ctx, res.Candidate.Resource.ID, res.ContextKey, req.Profile, "FAIL")
	}
}

func recordAttempt(ctx context.Context, store *sqlite.Store, ev domain.Evidence, res sqlite.Reserved, fail *validation.Failure) *domain.Error {
	detail := ""
	outcome := ev.Outcome
	if fail != nil {
		outcome = "FAIL"
		detail = fail.Kind + ": " + fail.Detail
	}
	if err := store.RecordAttempt(ctx, ev, res.Candidate.Resource.ID, res.ContextKey, detail); err != nil {
		return domain.StoreUnavailable(err)
	}
	_ = outcome
	return nil
}

func (s *AcquireService) commitLease(ctx context.Context, res sqlite.Reserved, ev domain.Evidence, req AcquireRequest, requestID string) (AcquiredItem, error) {
	now := s.Now()
	ttl := time.Duration(req.Lease.TTLMS) * time.Millisecond
	deadline := now.Add(ttl)
	if exp := res.Candidate.Offer.ProviderExpiresAt; exp != nil {
		if d := exp.Add(-s.cfg.ExpirySafetyMargin()); d.Before(deadline) {
			deadline = d
		}
	}
	if ret := res.Candidate.Offer.LocalRetentionDeadline; ret != nil && ret.Before(deadline) {
		deadline = *ret
	}
	if usable := deadline.Sub(now); usable < time.Duration(req.Lease.MinUsableMS)*time.Millisecond {
		return AcquiredItem{}, fmt.Errorf("usable window %s below required %dms", usable, req.Lease.MinUsableMS)
	}
	leaseID := domain.NewID("lease_")
	connJSON, err := json.Marshal(res.Candidate.Resource.Conn)
	if err != nil {
		return AcquiredItem{}, err
	}
	valJSON, err := json.Marshal(ev)
	if err != nil {
		return AcquiredItem{}, err
	}
	err = s.store.CommitLease(ctx, sqlite.CommitArgs{
		ReservationID:  res.ReservationID,
		Owner:          requestID,
		LeaseID:        leaseID,
		RequestID:      requestID,
		Tenant:         "default",
		Mode:           req.Lease.Mode,
		Deadline:       deadline,
		ConnJSON:       string(connJSON),
		ValidationJSON: string(valJSON),
		ExpectedFence:  res.Fence,
		ExpectedEpoch:  res.FailureEpoch,
		Profile:        req.Profile,
		Now:            now,
	})
	if err != nil {
		return AcquiredItem{}, err
	}
	return AcquiredItem{
		LeaseID:    leaseID,
		ResourceID: res.Candidate.Resource.ID,
		SourceID:   res.Candidate.Offer.SourceID,
		Conn:       res.Candidate.Resource.Conn,
		Validation: ev,
		ExpiresAt:  deadline,
	}, nil
}

// dispatchGuard rechecks every committed lease immediately before the response:
// still active, not expired, and evidence fresh within the configured bound.
func (s *AcquireService) dispatchGuard(ctx context.Context, run *acquireRun, requestDeadline time.Time) []AcquiredItem {
	now := s.Now()
	run.mu.Lock()
	items := run.leases
	run.mu.Unlock()
	kept := make([]AcquiredItem, 0, len(items))
	for _, it := range items {
		row, err := s.store.GetLease(ctx, it.LeaseID)
		if err != nil || row.Lease.State != domain.LeaseActive {
			continue
		}
		if !row.Lease.Deadline.After(now) {
			_, _ = s.store.SetLeaseState(ctx, it.LeaseID, "EXPIRED", now)
			continue
		}
		var ev domain.Evidence
		if err := json.Unmarshal([]byte(row.ValidationJSON), &ev); err != nil {
			continue
		}
		if now.Sub(ev.CompletedAt) > s.cfg.MaxProofAge() {
			_, _ = s.store.SetLeaseState(ctx, it.LeaseID, "REVOKED", now)
			continue
		}
		kept = append(kept, it)
	}
	return kept
}
