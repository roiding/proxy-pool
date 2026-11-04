package application

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"proxypool/internal/config"
	"proxypool/internal/domain"
	"proxypool/internal/geoip"
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
	// CD is the per-consumer proxy cooldown in seconds. The URL query
	// parameter cd overrides it. An explicit zero disables the key default.
	CDSeconds  *int64 `json:"cd"`
	ConsumerID string `json:"-"`
	// KeyID is the authenticating api key (the new API identity carries the
	// project id as the CD subject in ConsumerID and the key separately).
	KeyID string `json:"-"`
	// ProjectID is resolved from the authenticated consumer before any
	// candidate work starts; it scopes project CD and defaults.
	ProjectID string `json:"-"`
	// Validation carries the exact normalized scope (mode, target, profile)
	// with a runtime security hash; nil means the default basic scope.
	Validation *domain.ValidationScope `json:"-"`
	// SessionID is the sticky binding key; sticky enforcement lands with the
	// binding service, ordinary requests must leave it empty.
	SessionID  string `json:"-"`
	CooldownMS int64  `json:"-"`
}

type Filters struct {
	Transports    []domain.Transport    `json:"transports"`
	ResourceKinds []domain.ResourceKind `json:"resource_kinds"`
	SourceIDs     []string              `json:"source_ids"`
	Countries     []string              `json:"countries"`
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
	// ProxyURI is the convenience form of Conn: encoded userinfo and a
	// bracketed IPv6 host. Connection remains the standard data source.
	ProxyURI string `json:"proxy_uri"`
	// ProviderExpiresAt is the supplier's own expiry; null means unknown.
	// Lease, cache, and binding deadlines never substitute for it.
	ProviderExpiresAt *time.Time `json:"provider_expires_at"`
	// The identity fields are internal commit/binding guards.
	SourceConfigVersion int64               `json:"-"`
	Incarnation         string              `json:"-"`
	Ephemeral           bool                `json:"-"`
	Observed            *domain.Observation `json:"observed,omitempty"`
	Validation          domain.Evidence     `json:"validation"`
	ExpiresAt           time.Time           `json:"lease_expires_at"`
	// BindingDeadline is the durable sticky binding deadline backing this
	// delivery; binding expiry is a top-level session field on the wire.
	BindingDeadline time.Time `json:"-"`
	// Sticky responses expose whether this request reused its existing
	// binding or had to establish a new one, plus the binding version.
	SessionBindingVersion int64  `json:"-"`
	SessionChanged        bool   `json:"-"`
	SessionChangeReason   string `json:"-"`
}

type AcquireResult struct {
	RequestID string         `json:"request_id"`
	Requested int            `json:"requested"`
	Delivered int            `json:"delivered"`
	Missing   int            `json:"missing"`
	Partial   bool           `json:"partial"`
	Items     []AcquiredItem `json:"items"`
	// ShortfallReason is the internal name of the wire `reason` field; the
	// API serializer maps it so no legacy alias reaches the response.
	ShortfallReason string `json:"-"`
	CooldownSeconds int64  `json:"cooldown_seconds,omitempty"`
	SessionID       string `json:"session_id,omitempty"`
	// SessionChanged stays a plain bool because service callers and tests
	// read it directly; the wire field is emitted explicitly (including
	// false) by the API serializer for sticky responses only.
	SessionChanged bool   `json:"-"`
	ChangeReason   string `json:"change_reason,omitempty"`
	BindingVersion int64  `json:"binding_version,omitempty"`
	// BindingExpiresAt is the durable binding deadline, independent of the
	// lease and proof validity windows.
	BindingExpiresAt time.Time `json:"binding_expires_at,omitempty"`
}

type AcquireService struct {
	store              *sqlite.Store
	cfg                config.Config
	judgeURL           string
	judgeCAFingerprint string
	rootCAs            *x509.CertPool
	// Target TLS trust material. Production leaves both empty: system
	// roots only, never the judge CA. Tests inject an isolated pool.
	targetRootCAs       *x509.CertPool
	targetCAFingerprint string
	health              *HealthRecorder
	geo                 geoip.Resolver
	Now                 func() time.Time
	// Refiller is optional; when set, an empty candidate page triggers one
	// demand refill from an eligible provider channel before giving up.
	Refiller SourceRefiller

	sourcePolicy sqlite.SourceCircuitPolicy
	acquireSlots chan struct{}
	probeSlots   chan struct{}
}

type SourceRefiller interface {
	// FetchOnDemand returns request-local candidates for one vendor call;
	// on-demand supply never touches the inventory.
	FetchOnDemand(ctx context.Context, sourceID string, wanted int) ([]domain.Candidate, *domain.Error)
}

// RefillScope narrows a demand refill to the request's transport and
// country filters; candidates outside the scope are useless work.
type RefillScope struct {
	Transports []domain.Transport
	Countries  []string
}

// SourceBatchRefiller is the richer refill surface: scoped fetches plus
// one multi-source, multi-batch call with its own vendor-call budget. The
// supply side implements it; acquire falls back to per-source
// FetchOnDemand when only the base interface is available.
type SourceBatchRefiller interface {
	SourceRefiller
	// Scoped fetches request-local candidates for one source inside the
	// given scope. Implementations return only in-scope candidates.
	Scoped(ctx context.Context, sourceID string, wanted int, scope RefillScope) ([]domain.Candidate, *domain.Error)
	// Batches fetches across sourceIDs with bounded vendor calls and stops
	// early once the wanted count is met.
	Batches(ctx context.Context, sourceIDs []string, wanted int, scope RefillScope) ([]domain.Candidate, *domain.Error)
}

type StaticBatchChecker interface {
	StaticCandidateCurrent(sourceID string, sourceConfigVersion int64, batchID string) bool
}

// maxRefillBatches bounds the vendor calls one acquire request may spend on
// demand refill, so a request can never fan out unbounded across channels.
const maxRefillBatches = 3

// finalizeBudget is the slice of the request's total timeout reserved for
// dispatch finalization (lease rechecks + response assembly). Candidate
// work must stop before it starts, so a probe that finishes inside the
// work window can never be discarded by a deadline-expired finalize step.
func finalizeBudget(total time.Duration) time.Duration {
	b := total / 4
	if b > 2*time.Second {
		b = 2 * time.Second
	}
	if b < 100*time.Millisecond {
		b = 100 * time.Millisecond
	}
	if b*2 > total {
		b = total / 2
	}
	if b <= 0 {
		b = total
	}
	return b
}

// DefaultAcquireProject is the CD subject for the deployment master token,
// mirroring the API's session identity.
const DefaultAcquireProject = "default"

func NewAcquireService(store *sqlite.Store, cfg config.Config, judgeURL string,
	rootCAs *x509.CertPool, probeSlots chan struct{}, geo geoip.Resolver) *AcquireService {
	return &AcquireService{
		store:              store,
		cfg:                cfg,
		judgeURL:           judgeURL,
		judgeCAFingerprint: judgeCAPingerprint(cfg.JudgeCAPem),
		rootCAs:            rootCAs,
		health:             newHealthRecorder(store, cfg),
		geo:                geo,
		sourcePolicy: sqlite.SourceCircuitPolicy{
			FailureThreshold: cfg.SourceFailureThreshold,
			Base:             cfg.SourceCooldownBase(),
			Max:              cfg.SourceCooldownMax(),
		},
		acquireSlots: make(chan struct{}, cfg.MaxActiveAcquires),
		probeSlots:   probeSlots,
		Now:          time.Now,
	}
}

func (s *AcquireService) validateRequest(req *AcquireRequest) *domain.Error {
	if req.Count < 1 {
		return domain.InvalidRequest("count must be at least 1")
	}
	if req.Count > s.cfg.MaxCount {
		return domain.InvalidRequest("count %d exceeds the maximum of %d", req.Count, s.cfg.MaxCount)
	}
	scope := domain.ValidationScope{Mode: "basic", Profile: validation.ProfileHTTPSBasicV1}
	if req.Validation != nil {
		scope = *req.Validation
		if scope.TargetHostPort != "" && scope.Mode != "tls" {
			return domain.InvalidRequest("validation target is only valid for mode tls")
		}
		if scope.Mode == "tls" && scope.TargetHostPort == "" {
			return domain.InvalidRequest("validation mode tls requires target host:port")
		}
	}
	switch scope.Mode {
	case "basic":
		scope.Mode = "basic"
		scope.Profile = validation.ProfileHTTPSBasicV1
		scope.TargetHostPort = ""
	case "tls":
		scope.Profile = validation.ProfileTargetTLSV1
	default:
		return domain.UnsupportedProfile(scope.Mode)
	}
	// The security hash always reflects the running judge/CA configuration so
	// a config change can never reuse cached evidence from the old regime.
	opts := s.probeOptions(scope.Mode)
	scope.SecurityHash = validation.ProbeSecurityHash(opts)
	req.Validation = &scope
	req.Profile = scope.Profile
	for _, t := range req.Filters.Transports {
		if t != domain.TransportHTTP && t != domain.TransportSOCKS5 {
			return domain.UnsupportedTransport(string(t))
		}
	}
	if len(req.Filters.Transports) == 0 {
		req.Filters.Transports = []domain.Transport{domain.TransportHTTP, domain.TransportSOCKS5}
	}
	req.Filters.Countries = normalizeCountries(req.Filters.Countries)
	for _, c := range req.Filters.Countries {
		if len(c) != 2 || c[0] < 'A' || c[0] > 'Z' || c[1] < 'A' || c[1] > 'Z' {
			return domain.InvalidRequest("country filter %q must be a 2-letter ISO code", c)
		}
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
	switch req.Lease.Mode {
	case "", "exclusive", "shared":
		if req.SessionID != "" {
			return domain.InvalidRequest("session_id is only valid with mode sticky")
		}
	case "ordinary":
		req.Lease.Mode = "exclusive"
		if req.SessionID != "" {
			return domain.InvalidRequest("session_id is only valid with mode sticky")
		}
	case "sticky":
		if strings.TrimSpace(req.SessionID) == "" {
			return domain.InvalidRequest("sticky mode requires session_id")
		}
		if req.ProjectID == "" {
			return domain.InvalidRequest("sticky mode requires a project credential")
		}
		req.SessionID = strings.TrimSpace(req.SessionID)
		if req.Count != 1 {
			return domain.InvalidRequest("sticky sessions deliver exactly one connection")
		}
	default:
		return domain.InvalidRequest("unknown lease mode %q", req.Lease.Mode)
	}
	if req.DeadlineMS < 0 {
		return domain.InvalidRequest("timeout must not be negative")
	}
	if req.DeadlineMS <= 0 {
		req.DeadlineMS = s.cfg.AcquireDeadlineMS
	}
	if req.DeadlineMS > s.cfg.MaxAcquireDeadlineMS {
		return domain.InvalidRequest("timeout %dms exceeds the maximum of %dms",
			req.DeadlineMS, s.cfg.MaxAcquireDeadlineMS)
	}
	if req.CooldownMS < 0 {
		return domain.InvalidRequest("cd must not be negative")
	}
	if req.CDSeconds != nil {
		if *req.CDSeconds < 0 {
			return domain.InvalidRequest("cd must not be negative")
		}
		if *req.CDSeconds > s.cfg.MaxDownstreamCooldownMS/1000 {
			return domain.InvalidRequest("cd %d seconds exceeds the maximum of %d seconds",
				*req.CDSeconds, s.cfg.MaxDownstreamCooldownMS/1000)
		}
		req.CooldownMS = *req.CDSeconds * 1000
	}
	if req.CooldownMS > s.cfg.MaxDownstreamCooldownMS {
		return domain.InvalidRequest("cd exceeds the maximum of %d seconds",
			s.cfg.MaxDownstreamCooldownMS/1000)
	}
	return nil
}

// probeOptions assembles the runtime validation configuration. Basic probes
// trust the deployment judge CA; target TLS uses the system roots, so the two
// regimes produce different security hashes.
func (s *AcquireService) probeOptions(mode string) validation.ProbeOptions {
	limits := validation.ProbeLimits{
		DialTimeout:  s.cfg.ConnectTimeout(),
		ProbeTotal:   s.cfg.ProbeTotalTimeout(),
		AllowPrivate: s.cfg.AllowPrivateAddresses,
		JudgeSecret:  s.cfg.JudgeSecret,
	}
	if mode != "tls" {
		limits.RootCAs = s.rootCAs
		limits.CAFingerprint = s.judgeCAFingerprint
	} else {
		// Target TLS keeps its own trust store: production nil = system
		// roots, tests may inject an isolated CA. The judge CA never mixes
		// in, and the security hash changes with the injected material.
		limits.RootCAs = s.targetRootCAs
		limits.CAFingerprint = s.targetCAFingerprint
	}
	return validation.ProbeOptions{JudgeURL: s.judgeURL, Limits: limits}
}

// SetTargetRootCAs overrides the trust store for target-TLS validation
// probes. fingerprint must digest the same PEM the pool was built from so
// evidence cache keys change with the material. Production code must not
// call this: system roots are the default and the judge CA never applies.
func (s *AcquireService) SetTargetRootCAs(pool *x509.CertPool, fingerprint string) {
	s.targetRootCAs = pool
	s.targetCAFingerprint = fingerprint
}

// judgeCAPingerprint digests the source PEM because x509.CertPool cannot be
// enumerated; it feeds the scope security hash.
func judgeCAPingerprint(pem string) string {
	if pem == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(pem))
	return hex.EncodeToString(sum[:])
}

func (s *AcquireService) Acquire(ctx context.Context, req AcquireRequest) (AcquireResult, *domain.Error) {
	req.ProjectID = s.resolveProjectID(ctx, req)
	req.KeyID = s.effectiveKeyID(ctx, req)
	// Project defaults and the allowlist are applied before validation: a
	// project-configured target-TLS default must never silently degrade to
	// basic when the request omits validation.
	if req.ProjectID != "" {
		if proj, perr := s.store.GetProject(ctx, req.ProjectID); perr == nil {
			if req.CooldownMS == 0 && proj.Defaults.CooldownMS > 0 {
				req.CooldownMS = proj.Defaults.CooldownMS
			}
			if req.DeadlineMS == 0 && proj.Defaults.TimeoutMS > 0 {
				req.DeadlineMS = proj.Defaults.TimeoutMS
			}
			if req.Validation == nil && proj.Defaults.Validation.Mode != "" {
				def := proj.Defaults.Validation
				req.Validation = &def
			}
			// An explicit request cd wins; the project default outranks the
			// key's legacy default; cd=0 explicitly disables a new window.
			if req.CooldownMS == 0 && req.CDSeconds == nil && proj.Defaults.CooldownMS == 0 {
				if key, kerr := s.store.APIKeyByID(ctx, req.KeyID); kerr == nil &&
					key.DefaultCooldownMS > 0 {
					req.CooldownMS = key.DefaultCooldownMS
				}
			}
			// nil = unrestricted, an empty list = nothing allowed (an empty
			// intersection is never "all").
			req.Filters.SourceIDs = proj.FilterAllowedSources(req.Filters.SourceIDs)
		}
	}
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
	total := time.Duration(req.DeadlineMS) * time.Millisecond
	deadline := s.Now().Add(total)
	// overallCtx keeps the caller's cancellation and lives until the total
	// deadline; work stops a finalization slice early, and finalization runs
	// on overallCtx so a delivery that already committed survives the work
	// deadline but never a canceled or fully expired request.
	overallCtx, cancelOverall := context.WithDeadline(ctx, deadline)
	defer cancelOverall()
	fBudget := finalizeBudget(total)
	workCtx, cancelWork := context.WithDeadline(overallCtx, deadline.Add(-fBudget))
	defer cancelWork()

	if req.Lease.Mode == "sticky" {
		result, derr := s.acquireSticky(workCtx, overallCtx, &req, requestID, deadline)
		if derr != nil {
			return result, derr
		}
		return result, derr
	}
	return s.runOrdinary(workCtx, overallCtx, &req, requestID, deadline)
}

func (s *AcquireService) runOrdinary(ctx context.Context, overallCtx context.Context,
	req *AcquireRequest,
	requestID string, deadline time.Time) (AcquireResult, *domain.Error) {
	return s.runOrdinaryWithPending(ctx, overallCtx, req, requestID, deadline, nil)
}

func (s *AcquireService) runOrdinaryWithPending(ctx context.Context,
	overallCtx context.Context, req *AcquireRequest, requestID string,
	deadline time.Time, pending *sqlite.StickyBindingCommit) (AcquireResult, *domain.Error) {
	run := &acquireRun{
		maxAttempts:    maxCandidateAttempts(req.Count, s.cfg.MaxCandidateAttempts),
		pendingBinding: pending,
	}
	defer s.cleanupRunReservations(requestID, run)
	if req.Filters.SourceIDs != nil && len(req.Filters.SourceIDs) == 0 {
		run.reasonOverride = "SOURCE_SCOPE_EMPTY"
	}
	// Buffered to the attempt budget so the feeder never blocks on a full
	// queue after the workers have stopped consuming (that stall used to
	// hold successful batches until the request deadline).
	candidates := make(chan domain.Candidate, run.maxAttempts)
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
			s.workerLoop(ctx, overallCtx, run, candidates, *req, requestID, deadline)
		}()
	}
	s.feedCandidates(ctx, run, candidates, *req)
	workers.Wait()
	if run.delivered.Load() < int32(req.Count) && (ctx.Err() != nil || overallCtx.Err() != nil) {
		run.recordRequestBudgetExhausted()
	}

	run.mu.Lock()
	selectionErr, reserveErr := run.selectionErr, run.reserveErr
	run.mu.Unlock()
	if selectionErr != nil {
		return AcquireResult{}, domain.StoreUnavailable(selectionErr)
	}
	if reserveErr != nil {
		return AcquireResult{}, domain.StoreUnavailable(reserveErr)
	}
	items, derr := s.finalizeDelivery(overallCtx, run, *req, requestID, req.Count)
	if derr != nil {
		return AcquireResult{}, derr
	}
	// A final eligibility/capacity rejection is a normal skip, not proof the
	// whole supply class is exhausted. Re-enter the bounded scheduler while
	// budget and attempts remain so independent sources can fill the gap.
	for len(items) < req.Count && ctx.Err() == nil &&
		overallCtx.Err() == nil &&
		int(run.attempts.Load()) < run.maxAttempts {
		run.mu.Lock()
		run.eligibilitySkipped = 0
		run.mu.Unlock()
		more, ferr := s.finalizeDelivery(overallCtx, run, *req, requestID,
			req.Count-len(items))
		if ferr != nil {
			return AcquireResult{}, ferr
		}
		if len(more) == 0 {
			break
		}
		items = append(items, more...)
		break
	}
	if items == nil {
		items = []AcquiredItem{}
	}
	result := AcquireResult{
		RequestID:       requestID,
		Requested:       req.Count,
		Delivered:       len(items),
		Missing:         req.Count - len(items),
		Items:           items,
		CooldownSeconds: req.CooldownMS / 1000,
	}
	if len(items) == 0 {
		result.ShortfallReason = run.zeroReason()
		return result, nil
	}
	if len(items) < req.Count {
		result.ShortfallReason = run.shortfallReason()
		result.Partial = true
	}
	return result, nil
}

// finalizeDelivery selects the actual response set and commits lease, fixed
// project CD, and any sticky binding in one transaction. Candidates that were
// probed but not selected never consume CD.
func (s *AcquireService) finalizeDelivery(ctx context.Context, run *acquireRun,
	req AcquireRequest, requestID string, count int) ([]AcquiredItem, *domain.Error) {
	now := s.Now()
	run.mu.Lock()
	run.leases = nil
	run.mu.Unlock()
	items := run.usablePreparedItems(count, now, s.cfg.MaxProofAge(), s.fingerprintConn)
	if len(items) == 0 {
		return []AcquiredItem{}, nil
	}
	return s.commitPrepared(ctx, run, items, req, requestID, nil)
}

// finalizeStickyDelivery commits the selected connection together with the
// binding CAS. The claim was made before candidate work and is the sole
// sticky-owned write in this transaction.
func (s *AcquireService) finalizeStickyDelivery(ctx context.Context, run *acquireRun,
	req AcquireRequest, requestID string, claim domain.StickyBinding,
	ciphertext string) ([]AcquiredItem, *domain.Error) {
	items := run.usablePreparedItems(1, s.Now(), s.cfg.MaxProofAge(), s.fingerprintConn)
	if len(items) != 1 {
		return []AcquiredItem{}, nil
	}
	item := items[0]
	if ciphertext == "" {
		var err error
		ciphertext, err = s.encryptBindingConn(item.Conn)
		if err != nil {
			return nil, domain.StoreUnavailable(err)
		}
	}
	binding := domain.StickyBinding{
		ProjectID: req.ProjectID, SessionID: req.SessionID, Owner: requestID,
		Fingerprint: s.fingerprintConn(item.Conn), ResourceID: item.ResourceID,
		Incarnation: item.Incarnation, SourceID: item.SourceID,
		SourceConfigVersion:  item.SourceConfigVersion,
		ConnectionCiphertext: ciphertext, Validation: item.Validation,
		Deadline: item.ExpiresAt,
	}
	commit := &sqlite.StickyBindingCommit{
		Binding: binding, ExpectedVersion: claim.Version, Owner: requestID,
	}
	committed, derr := s.commitPrepared(ctx, run, items, req, requestID, commit)
	if derr != nil {
		return nil, derr
	}
	return committed, nil
}

// commitPrepared writes exactly the selected response set: lease, fixed
// project CD, and (for sticky rebinds) the binding CAS in one transaction.
// Eligibility losses are partial-result skips; real storage errors are 5xx.
func (s *AcquireService) commitPrepared(ctx context.Context, run *acquireRun,
	items []AcquiredItem, req AcquireRequest, requestID string,
	binding *sqlite.StickyBindingCommit) ([]AcquiredItem, *domain.Error) {
	now := s.Now()
	candidates := make([]sqlite.DeliveryCandidate, 0, len(items))
	byFP := map[string]preparedCandidate{}
	run.mu.Lock()
	for _, p := range run.prepared {
		byFP[s.fingerprintConn(p.item.Conn)] = p
	}
	run.mu.Unlock()
	for _, item := range items {
		fp := s.fingerprintConn(item.Conn)
		p, ok := byFP[fp]
		if !ok {
			continue
		}
		if p.candidate.Ephemeral && p.candidate.Offer.ExpiryBasis != "ondemand" {
			if checker, ok := s.Refiller.(StaticBatchChecker); ok &&
				!checker.StaticCandidateCurrent(item.SourceID, item.SourceConfigVersion,
					item.Incarnation) {
				run.mu.Lock()
				run.eligibilitySkipped++
				run.mu.Unlock()
				continue
			}
		}
		deadline := s.leaseDeadline(p.candidate, s.Now(), req)
		if deadline.Before(item.ExpiresAt) {
			item.ExpiresAt = deadline
		}
		if usable := item.ExpiresAt.Sub(s.Now()); usable < time.Duration(req.Lease.MinUsableMS)*time.Millisecond {
			continue
		}
		connJSON, err := json.Marshal(item.Conn)
		if err != nil {
			return nil, domain.StoreUnavailable(err)
		}
		valJSON, err := json.Marshal(item.Validation)
		if err != nil {
			return nil, domain.StoreUnavailable(err)
		}
		candidates = append(candidates, sqlite.DeliveryCandidate{
			LeaseID:             item.LeaseID,
			ReservationID:       p.reservationID,
			ResourceID:          item.ResourceID,
			OfferID:             p.offerID,
			ContextKey:          p.contextKey,
			SourceID:            item.SourceID,
			SourceConfigVersion: item.SourceConfigVersion,
			Incarnation:         item.Incarnation,
			Ephemeral:           item.Ephemeral,
			Fingerprint:         fp,
			ConnJSON:            string(connJSON),
			ValidationJSON:      string(valJSON),
			DeadlineText:        item.ExpiresAt.UTC().Format("2006-01-02T15:04:05.000000000Z07:00"),
			BindingDeadlineText: s.stickyBindingDeadline(p.candidate, now).UTC().
				Format("2006-01-02T15:04:05.000000000Z07:00"),
			MaxPoolLeases:    p.candidate.Offer.MaxPoolLeases,
			ExpectedEpoch:    p.failureEpoch,
			ExpectedEvidence: item.Validation,
			MinUsable:        time.Duration(req.Lease.MinUsableMS) * time.Millisecond,
		})
	}
	if len(candidates) == 0 {
		return []AcquiredItem{}, nil
	}
	commitBinding := binding
	if commitBinding != nil && binding.Binding.ConnectionCiphertext == "" {
		ciphertext, err := s.encryptBindingConn(candidates[0].Conn())
		if err != nil {
			return nil, domain.StoreUnavailable(err)
		}
		commitBinding = &sqlite.StickyBindingCommit{
			Binding:         binding.Binding,
			ExpectedVersion: binding.ExpectedVersion,
			Owner:           binding.Owner,
		}
		commitBinding.Binding.ProjectID = req.ProjectID
		commitBinding.Binding.SessionID = req.SessionID
		commitBinding.Binding.Fingerprint = candidates[0].Fingerprint
		commitBinding.Binding.ResourceID = candidates[0].ResourceID
		commitBinding.Binding.Incarnation = candidates[0].Incarnation
		commitBinding.Binding.SourceID = candidates[0].SourceID
		commitBinding.Binding.SourceConfigVersion = candidates[0].SourceConfigVersion
		commitBinding.Binding.ConnectionCiphertext = ciphertext
		commitBinding.Binding.Validation = candidates[0].ExpectedEvidence
		if p, ok := byFP[candidates[0].Fingerprint]; ok {
			commitBinding.Binding.Deadline = s.stickyBindingDeadline(p.candidate, now)
		} else {
			commitBinding.Binding.Deadline = mustParseTime(candidates[0].DeadlineText)
		}
	}
	if commitBinding == nil && run.pendingBinding != nil {
		seed := run.pendingBinding
		ciphertext, err := s.encryptBindingConn(candidates[0].Conn())
		if err != nil {
			return nil, domain.StoreUnavailable(err)
		}
		commitBinding = &sqlite.StickyBindingCommit{
			Binding: domain.StickyBinding{
				ProjectID: req.ProjectID, SessionID: req.SessionID,
				Owner: seed.Owner, Fingerprint: candidates[0].Fingerprint,
				ResourceID:  candidates[0].ResourceID,
				Incarnation: candidates[0].Incarnation, SourceID: candidates[0].SourceID,
				SourceConfigVersion:  candidates[0].SourceConfigVersion,
				ConnectionCiphertext: ciphertext,
				Validation:           candidates[0].ExpectedEvidence,
				Deadline:             s.stickyBindingDeadline(byFP[candidates[0].Fingerprint].candidate, now),
			},
			ExpectedVersion: seed.ExpectedVersion, Owner: seed.Owner,
		}
	}
	final, err := s.store.CommitFinalDelivery(ctx, sqlite.CommitArgs{
		RequestID:        requestID,
		Owner:            requestID,
		Tenant:           "default",
		Mode:             req.Lease.Mode,
		ConsumerID:       req.ConsumerID,
		KeyID:            req.KeyID,
		ProjectID:        req.ProjectID,
		DeliveryCooldown: time.Duration(req.CooldownMS) * time.Millisecond,
		Now:              now,
		IdentitySecret:   s.cfg.IdentitySecret,
		DeliverySafety:   s.cfg.ExpirySafetyMargin(),
		MaxProofAge:      s.cfg.MaxProofAge(),
	}, candidates, commitBinding)
	if err != nil {
		return nil, domain.StoreUnavailable(err)
	}
	committed := map[string]AcquiredItem{}
	for _, c := range final.Items {
		item := AcquiredItem{
			LeaseID: c.LeaseID, ResourceID: c.ResourceID, SourceID: c.SourceID,
			SourceConfigVersion: c.SourceConfigVersion, Incarnation: c.Incarnation,
			Ephemeral: c.Ephemeral, ExpiresAt: mustParseTime(c.DeadlineText),
		}
		_ = json.Unmarshal([]byte(c.ConnJSON), &item.Conn)
		_ = json.Unmarshal([]byte(c.ValidationJSON), &item.Validation)
		item.ProxyURI = proxyURIFrom(item.Conn)
		committed[c.Fingerprint] = item
	}
	out := make([]AcquiredItem, 0, len(committed))
	for _, item := range items {
		if got, ok := committed[s.fingerprintConn(item.Conn)]; ok {
			got.Observed = item.Observed
			got.ProviderExpiresAt = item.ProviderExpiresAt
			out = append(out, got)
		}
	}
	return out, nil
}

func mustParseTime(value string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, value)
	return t
}

// resolveProjectID resolves the CD subject's project: an explicit ProjectID
// wins, then a legacy key id, then the new-API project id in ConsumerID.
func (s *AcquireService) resolveProjectID(ctx context.Context, req AcquireRequest) string {
	if req.ProjectID != "" {
		return req.ProjectID
	}
	if req.ConsumerID == "" {
		return ""
	}
	if key, err := s.store.APIKeyByID(ctx, req.ConsumerID); err == nil && key.ProjectID != "" {
		return key.ProjectID
	}
	// The deployment master token's CD subject is the default project, the
	// same subject an authenticated admin session uses.
	switch req.ConsumerID {
	case "admin", "master":
		return DefaultAcquireProject
	}
	// Any other consumer id is its own implicit CD scope: cooldown queries
	// do not require a projects row, and project defaults simply don't apply.
	return req.ConsumerID
}

// effectiveKeyID finds the authenticating api key for commit-time key
// rechecks; requests without a key (master token/session) have none.
func (s *AcquireService) effectiveKeyID(ctx context.Context, req AcquireRequest) string {
	if req.KeyID != "" {
		return req.KeyID
	}
	if _, err := s.store.APIKeyByID(ctx, req.ConsumerID); err == nil {
		return req.ConsumerID
	}
	return ""
}

func (s *AcquireService) fingerprintConn(conn domain.Connection) string {
	return domain.ConnectionFingerprint(s.cfg.IdentitySecret, conn.Transport,
		conn.Host, conn.Port, conn.Username, conn.Password)
}

// leaseDeadline applies the request lease TTL and every known supplier/local
// expiry bound. Final dispatch calls it again, so a proof that ages while a
// slower sibling is probing cannot overstate the usable window.
func (s *AcquireService) leaseDeadline(cand domain.Candidate, now time.Time,
	req AcquireRequest) time.Time {
	deadline := now.Add(time.Duration(req.Lease.TTLMS) * time.Millisecond)
	if exp := cand.Offer.ProviderExpiresAt; exp != nil {
		if d := exp.Add(-s.cfg.ExpirySafetyMargin()); d.Before(deadline) {
			deadline = d
		}
	}
	if ret := cand.Offer.LocalRetentionDeadline; ret != nil && ret.Before(deadline) {
		deadline = *ret
	}
	return deadline
}

// stickyBindingDeadline is independent of the lease TTL: it identifies the
// logical session, while a lease grants one delivery window. Known supplier
// and local retention expiry remain binding upper bounds.
func (s *AcquireService) stickyBindingDeadline(cand domain.Candidate, now time.Time) time.Time {
	deadline := now.Add(s.cfg.StickyBindingTTL())
	if exp := cand.Offer.ProviderExpiresAt; exp != nil {
		if tightened := exp.Add(-s.cfg.ExpirySafetyMargin()); tightened.Before(deadline) {
			deadline = tightened
		}
	}
	if ret := cand.Offer.LocalRetentionDeadline; ret != nil && ret.Before(deadline) {
		deadline = *ret
	}
	return deadline
}

// evidenceKey binds a candidate to the evidence guards: exact connection,
// upstream source version, and object incarnation.
func (s *AcquireService) evidenceKey(cand domain.Candidate) sqlite.EvidenceKey {
	return sqlite.EvidenceKey{
		ResourceID:          cand.Resource.ID,
		Fingerprint:         s.fingerprintConn(cand.Resource.Conn),
		SourceID:            cand.Offer.SourceID,
		SourceConfigVersion: cand.SourceConfigVersion,
		Incarnation:         cand.Incarnation,
	}
}

// FingerprintOf exposes the canonical connection fingerprint (address alias
// normalized, credentials included, project/source/tenant excluded).
func (s *AcquireService) FingerprintOf(conn domain.Connection) string {
	return s.fingerprintConn(conn)
}

// filterProjectCooldown drops candidates already inside the project's fixed
// cooldown window before any probe starts. The reservation transaction
// rechecks the same window, so a query failure here stays safe.
func (s *AcquireService) filterProjectCooldown(ctx context.Context,
	req AcquireRequest, cands []domain.Candidate) ([]domain.Candidate, error) {
	if req.ProjectID == "" || len(cands) == 0 {
		return cands, nil
	}
	fps := make([]string, 0, len(cands))
	for _, c := range cands {
		fps = append(fps, s.fingerprintConn(c.Resource.Conn))
	}
	blocked, err := s.store.ProjectCooldownBlocked(ctx, req.ProjectID, s.Now(), fps)
	if err != nil {
		return nil, err
	}
	if len(blocked) == 0 {
		return cands, nil
	}
	kept := cands[:0]
	for _, c := range cands {
		if _, hit := blocked[s.fingerprintConn(c.Resource.Conn)]; !hit {
			kept = append(kept, c)
		}
	}
	return kept, nil
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
	maxAttempts int
	// pendingBinding is set only by a sticky rebind. The final transaction
	// updates the claimed binding together with the lease and fixed CD.
	pendingBinding *sqlite.StickyBindingCommit
	// skipValidation makes sticky reuse preparation probe-free: it only checks
	// whether the persisted evidence is still reusable for the exact scope.
	skipValidation bool
	// reasonOverride forces the zero-delivery explanation (e.g. an empty
	// authorized source scope) ahead of candidate-derived classification.
	reasonOverride string
	attempts       atomic.Int32
	delivered      atomic.Int32
	tried          sync.Map
	mu             sync.Mutex
	leases         []AcquiredItem
	prepared       []preparedCandidate
	failCounts     map[string]int
	inconclusive   int
	attributed     int
	sawCandidate   atomic.Bool
	// requestBudgetExhausted records that candidate work stopped because the
	// request work window or caller context ended. Ordinary inventory
	// exhaustion must stay distinct from this request-scoped outcome.
	requestBudgetExhausted atomic.Bool
	countrySkip            atomic.Int32
	capacitySkip           atomic.Int32
	refilled               bool
	selectionErr           error
	reserveErr             error
	eligibilitySkipped     int
}

type preparedCandidate struct {
	item          AcquiredItem
	offerID       string
	reservationID string
	contextKey    string
	failureEpoch  int64
	candidate     domain.Candidate
}

func (r *acquireRun) recordFailure(kind string) {
	if kind == validation.FailBudget {
		r.requestBudgetExhausted.Store(true)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failCounts == nil {
		r.failCounts = map[string]int{}
	}
	r.failCounts[kind]++
	if kind == validation.FailProxy || kind == validation.FailAuth {
		r.attributed++
	} else {
		r.inconclusive++
	}
}

func (r *acquireRun) recordRequestBudgetExhausted() {
	r.requestBudgetExhausted.Store(true)
}

// requestContextEndedError narrows SQL cancellation handling to errors caused
// by the request context ending. In particular, a canceled reservation can
// surface as ErrTxDone after the driver's rollback races the caller, but an
// arbitrary trigger/database error remains a storage outage.
func requestContextEndedError(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() == nil {
		return false
	}
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, sql.ErrTxDone)
}

// usablePreparedItems returns one item per canonical connection, in probe
// completion order, while preserving the request attempt budget. Durable
// lease/CD writes happen only after this request chooses the response set.
func (r *acquireRun) usablePreparedItems(count int, now time.Time,
	maxProofAge time.Duration, fingerprint func(domain.Connection) string) []AcquiredItem {
	r.mu.Lock()
	defer r.mu.Unlock()
	seen := map[string]bool{}
	out := make([]AcquiredItem, 0, count)
	for _, p := range r.prepared {
		ev := p.item.Validation
		if ev.CompletedAt.IsZero() || now.Sub(ev.CompletedAt) > maxProofAge ||
			!p.item.ExpiresAt.After(now) {
			continue
		}
		fp := fingerprint(p.item.Conn)
		if fp == "" || seen[fp] {
			continue
		}
		seen[fp] = true
		item := p.item
		item.Validation = deliveryEvidence(ev, maxProofAge)
		item.ProviderExpiresAt = utcTimePtr(p.candidate.Offer.ProviderExpiresAt)
		item.ProxyURI = proxyURIFrom(item.Conn)
		out = append(out, item)
		if len(out) >= count {
			break
		}
	}
	return out
}

const reservationCleanupTimeout = 250 * time.Millisecond

// releaseOwnedReservationsBounded is a synchronous post-request cleanup. It
// uses a fresh short deadline because the request context may already be
// canceled, and the owner predicate keeps it from touching newer work.
func (s *AcquireService) releaseOwnedReservationsBounded(owner string,
	reservationIDs []string) error {
	if owner == "" || len(reservationIDs) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), reservationCleanupTimeout)
	defer cancel()
	return s.store.ReleaseOwnedReservations(ctx, owner, reservationIDs)
}

func (s *AcquireService) releaseReservationBounded(owner, reservationID string) {
	_ = s.releaseOwnedReservationsBounded(owner, []string{reservationID})
}

// cleanupRunReservations releases only reservations prepared by this run.
// Reservations already converted to leases are COMMITTED and are ignored by
// the owner-scoped store update.
func (s *AcquireService) cleanupRunReservations(owner string, run *acquireRun) {
	run.mu.Lock()
	ids := make([]string, 0, len(run.prepared))
	for _, p := range run.prepared {
		if p.reservationID != "" {
			ids = append(ids, p.reservationID)
		}
	}
	run.mu.Unlock()
	_ = s.releaseOwnedReservationsBounded(owner, ids)
}

// deliveryEvidence normalizes fresh proof into the delivered wire shape:
// UTC timestamps plus a proof-reuse deadline that stays independent of the
// lease and cache windows.
func deliveryEvidence(ev domain.Evidence, maxProofAge time.Duration) domain.Evidence {
	ev.StartedAt = ev.StartedAt.UTC()
	ev.CompletedAt = ev.CompletedAt.UTC()
	valid := ev.CompletedAt.Add(maxProofAge)
	ev.ValidUntil = &valid
	return ev
}

func utcTimePtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	utc := t.UTC()
	return &utc
}

// proxyURIFrom renders the convenience URI from the canonical connection:
// credentials are URI-encoded exactly once and IPv6 hosts are bracketed.
func proxyURIFrom(conn domain.Connection) string {
	u := url.URL{Scheme: string(conn.Transport),
		Host: net.JoinHostPort(conn.Host, strconv.Itoa(conn.Port))}
	if conn.Username != "" || conn.Password != "" {
		u.User = url.UserPassword(conn.Username, conn.Password)
	}
	return u.String()
}

func (r *acquireRun) shortfallReason() string {
	r.mu.Lock()
	inconclusive, attributed := r.inconclusive, r.attributed
	r.mu.Unlock()
	return reasonFor(inconclusive, attributed, r.requestBudgetExhausted.Load())
}

func reasonFor(inconclusive, attributed int, requestBudgetExhausted bool) string {
	if requestBudgetExhausted {
		return "VALIDATION_BUDGET_EXHAUSTED"
	}
	if inconclusive > 0 && attributed == 0 {
		return "VALIDATION_INFRA_UNAVAILABLE"
	}
	if attributed > 0 {
		return "NO_VALIDATED_RESOURCES"
	}
	return "NO_ELIGIBLE_CANDIDATES"
}

func (r *acquireRun) zeroReason() string {
	if r.reasonOverride != "" {
		return r.reasonOverride
	}
	r.mu.Lock()
	inconclusive, attributed := r.inconclusive, r.attributed
	r.mu.Unlock()
	sawAny := r.sawCandidate.Load()
	countrySkipped := r.countrySkip.Load()
	if r.requestBudgetExhausted.Load() {
		return "VALIDATION_BUDGET_EXHAUSTED"
	}
	if !sawAny {
		return "NO_ELIGIBLE_CANDIDATES"
	}
	if r.capacitySkip.Load() > 0 {
		return "NO_ELIGIBLE_CANDIDATES"
	}
	if inconclusive > 0 && attributed == 0 {
		return "VALIDATION_INFRA_UNAVAILABLE"
	}
	if attributed > 0 {
		return "NO_VALIDATED_RESOURCES"
	}
	if countrySkipped > 0 {
		return "COUNTRY_FILTER_UNMET"
	}
	return "NO_ELIGIBLE_CANDIDATES"
}

func (s *AcquireService) feedCandidates(ctx context.Context, run *acquireRun, out chan<- domain.Candidate, req AcquireRequest) {
	defer close(out)
	if req.Filters.SourceIDs != nil && len(req.Filters.SourceIDs) == 0 {
		return
	}
	scope := RefillScope{Transports: req.Filters.Transports, Countries: req.Filters.Countries}
	// On-demand API is the default preference, not a hard-only path. Give it
	// the first bounded chance whenever the project may use provider sources;
	// uploaded/static inventory remains the fallback in the same request.
	if req.Filters.SourceIDs == nil {
		if ids, err := s.store.ProviderSourcesForRefill(ctx, s.Now(), nil,
			maxRefillBatches); err == nil && len(ids) > 0 {
			s.refillFromSources(ctx, run, out, req, scope, ids,
				req.Count-int(run.delivered.Load()))
		}
	}
	for int(run.attempts.Load()) < run.maxAttempts {
		if ctx.Err() != nil {
			run.recordRequestBudgetExhausted()
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
			Countries:    req.Filters.Countries,
			ExcludeByIDs: exclude,
			ConsumerID:   req.ConsumerID,
			Limit:        s.cfg.CandidatePage,
		})
		if err != nil {
			if requestContextEndedError(ctx, err) {
				run.recordRequestBudgetExhausted()
				return
			}
			run.mu.Lock()
			run.selectionErr = err
			run.mu.Unlock()
			return
		}
		cands, ferr := s.filterProjectCooldown(ctx, req, cands)
		if ferr != nil {
			if requestContextEndedError(ctx, ferr) {
				run.recordRequestBudgetExhausted()
				return
			}
			run.mu.Lock()
			run.selectionErr = ferr
			run.mu.Unlock()
			return
		}
		for _, c := range cands {
			if int(run.attempts.Load()) >= run.maxAttempts {
				break
			}
			if _, loaded := run.tried.LoadOrStore(c.Resource.ID, struct{}{}); loaded {
				continue
			}
			run.sawCandidate.Store(true)
			run.attempts.Add(1)
			select {
			case out <- c:
			case <-ctx.Done():
				run.recordRequestBudgetExhausted()
				return
			}
			if run.delivered.Load() >= int32(req.Count) {
				return
			}
		}
		// A page shorter than the selection limit means inventory is
		// exhausted for these filters — the refill trigger even when this
		// page was not empty. Delivery wins earlier via the count checks.
		if len(cands) < s.cfg.CandidatePage {
			run.refilled = false
			s.refillDemand(ctx, run, out, req, scope)
			return
		}
	}
}

// refillDemand spends the request's bounded vendor-call budget on demand
// batches once inventory cannot satisfy the request. It prefers the batch
// surface (scoped, multi-source, early-stop) and falls back to one
// FetchOnDemand call per source.
func (s *AcquireService) refillDemand(ctx context.Context, run *acquireRun,
	out chan<- domain.Candidate, req AcquireRequest, scope RefillScope) {
	if s.Refiller == nil || run.refilled {
		return
	}
	run.refilled = true
	if ctx.Err() != nil {
		run.recordRequestBudgetExhausted()
		return
	}
	if run.delivered.Load() >= int32(req.Count) ||
		int(run.attempts.Load()) >= run.maxAttempts {
		return
	}
	wanted := req.Count - int(run.delivered.Load())
	if wanted < 1 {
		wanted = 1
	}
	// On-demand API supply is the default preference, but an unavailable API
	// must not starve eligible static/uploaded supply. Try a bounded API batch
	// first, then a bounded static batch in the same request budget.
	ids, err := s.store.ProviderSourcesForRefill(ctx, s.Now(), req.Filters.SourceIDs, maxRefillBatches)
	if err == nil && len(ids) > 0 {
		if s.refillFromSources(ctx, run, out, req, scope, ids, wanted) {
			return
		}
	}
	staticIDs, serr := s.store.StaticSourcesForRefill(ctx, s.Now(), req.Filters.SourceIDs, maxRefillBatches)
	if serr == nil && len(staticIDs) > 0 {
		_ = s.refillFromSources(ctx, run, out, req, scope, staticIDs, wanted)
	}
}

// refillFromSources spends one bounded source list and reports whether any
// candidate was handed to a worker. The caller can then try the next supply
// class without ever exceeding the per-request vendor-call budget.
func (s *AcquireService) refillFromSources(ctx context.Context, run *acquireRun,
	out chan<- domain.Candidate, req AcquireRequest, scope RefillScope,
	ids []string, wanted int) bool {
	before := run.attempts.Load()
	if br, ok := s.Refiller.(SourceBatchRefiller); ok {
		cands, derr := br.Batches(ctx, ids, wanted, scope)
		if derr == nil {
			s.feedRefilled(ctx, run, out, req, cands)
			return run.attempts.Load() > before
		}
		// A batch-level failure from one source must not consume the
		// remaining request budget. Retry individual sources within the
		// same fixed budget so the next channel can still contribute.
		for _, id := range ids {
			if ctx.Err() != nil {
				run.recordRequestBudgetExhausted()
				break
			}
			if run.delivered.Load() >= int32(req.Count) {
				break
			}
			temps, ferr := br.Scoped(ctx, id, wanted, scope)
			if ferr != nil {
				continue
			}
			s.feedRefilled(ctx, run, out, req, temps)
			if run.attempts.Load() > before {
				break
			}
		}
		return run.attempts.Load() > before
	}
	for _, id := range ids {
		if ctx.Err() != nil {
			run.recordRequestBudgetExhausted()
			break
		}
		if run.delivered.Load() >= int32(req.Count) ||
			int(run.attempts.Load()) >= run.maxAttempts {
			break
		}
		if !s.providerSupports(ctx, id, req.Filters.Transports) {
			continue
		}
		temps, derr := s.Refiller.FetchOnDemand(ctx, id, wanted)
		if derr != nil {
			continue
		}
		s.feedRefilled(ctx, run, out, req, temps)
	}
	return run.attempts.Load() > before
}

// feedRefilled hands refilled candidates to the workers; the project CD
// filter and the request attempt budget bound every batch.
func (s *AcquireService) feedRefilled(ctx context.Context, run *acquireRun,
	out chan<- domain.Candidate, req AcquireRequest, cands []domain.Candidate) {
	var ferr error
	cands, ferr = s.filterProjectCooldown(ctx, req, cands)
	if ferr != nil {
		run.mu.Lock()
		run.selectionErr = ferr
		run.mu.Unlock()
		return
	}
	for _, c := range cands {
		if ctx.Err() != nil {
			run.recordRequestBudgetExhausted()
			return
		}
		if run.delivered.Load() >= int32(req.Count) ||
			int(run.attempts.Load()) >= run.maxAttempts {
			return
		}
		if _, loaded := run.tried.LoadOrStore(c.Resource.ID, struct{}{}); loaded {
			continue
		}
		run.sawCandidate.Store(true)
		run.attempts.Add(1)
		select {
		case out <- c:
		case <-ctx.Done():
			run.recordRequestBudgetExhausted()
			return
		}
	}
}

// providerSupports reports whether a provider channel's declared protocol
// can satisfy the request's transport filter, so demand refill does not
// spend a vendor call on a channel that cannot match.
func (s *AcquireService) providerSupports(ctx context.Context, sourceID string, transports []domain.Transport) bool {
	src, err := s.store.GetSource(ctx, sourceID)
	if err != nil {
		return false
	}
	var cfg SourceConfig
	if err := json.Unmarshal([]byte(src.ConfigJSON), &cfg); err != nil || cfg.Provider == nil {
		return false
	}
	transport := domain.TransportHTTP
	if cfg.Provider.Protocol == "socks5" {
		transport = domain.TransportSOCKS5
	}
	for _, t := range transports {
		if t == transport {
			return true
		}
	}
	return false
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

func (s *AcquireService) workerLoop(ctx context.Context, overallCtx context.Context,
	run *acquireRun, candidates <-chan domain.Candidate, req AcquireRequest, requestID string, requestDeadline time.Time) {
	for cand := range candidates {
		if ctx.Err() != nil {
			run.recordRequestBudgetExhausted()
			return
		}
		if run.delivered.Load() >= int32(req.Count) {
			return
		}
		s.acquireSlotsProbe(ctx, run, func() {
			s.processCandidate(ctx, overallCtx, run, cand, req, requestID, requestDeadline)
		})
		if run.delivered.Load() >= int32(req.Count) {
			return
		}
	}
}

func (s *AcquireService) acquireSlotsProbe(ctx context.Context, run *acquireRun, fn func()) {
	select {
	case s.probeSlots <- struct{}{}:
		defer func() { <-s.probeSlots }()
		fn()
	case <-ctx.Done():
		run.recordRequestBudgetExhausted()
	}
}

func (s *AcquireService) processCandidate(ctx context.Context, overallCtx context.Context,
	run *acquireRun, cand domain.Candidate, req AcquireRequest, requestID string, requestDeadline time.Time) {
	now := s.Now()
	res, err := s.store.ReserveSlot(ctx, cand, sqlite.ReserveArgs{
		Owner:               requestID,
		Profile:             req.Profile,
		Now:                 now,
		ReservationDeadline: requestDeadline.Add(2 * time.Second),
		ConsumerID:          req.ConsumerID,
		Cooldown:            time.Duration(req.CooldownMS) * time.Millisecond,
		ProjectID:           req.ProjectID,
		IdentitySecret:      s.cfg.IdentitySecret,
		KeyID:               req.KeyID,
	})
	if err != nil {
		if requestContextEndedError(ctx, err) {
			run.recordRequestBudgetExhausted()
			return
		}
		if errors.Is(err, sqlite.ErrCapacity) {
			run.capacitySkip.Add(1)
			return
		}
		if !errors.Is(err, sqlite.ErrNotEligible) && !errors.Is(err, sqlite.ErrStale) {
			run.mu.Lock()
			if run.reserveErr == nil {
				run.reserveErr = err
			}
			run.mu.Unlock()
		}
		return
	}
	probeCtx, cancelProbe := context.WithTimeout(ctx, s.cfg.ProbeTotalTimeout())
	defer cancelProbe()
	var (
		ev        domain.Evidence
		fail      *validation.Failure
		fromCache bool
	)
	// Recent pool-verified evidence inside the effective cache TTL replaces
	// the probe for inventory candidates only; every API/static request-local
	// batch is verified live.
	if res.Candidate.Ephemeral {
		// no probe before the final transaction
	} else if s.cfg.EffectiveEvidenceCacheTTL() > 0 {
		hits, err := s.store.RecentEvidence(ctx,
			domain.EvidenceScopeHash(*req.Validation),
			[]sqlite.EvidenceKey{s.evidenceKey(res.Candidate)},
			s.Now().Add(-s.cfg.EffectiveEvidenceCacheTTL()))
		if err == nil {
			if hit, ok := hits[s.fingerprintConn(res.Candidate.Resource.Conn)]; ok {
				ev, fail = hit, nil
				fromCache = true
				// The cached result keeps its validation content but gets a
				// fresh attempt identity for this request's audit trail.
				ev.AttemptID = domain.NewID("att_")
				ev.RequestID = requestID
			}
		}
	}
	if !fromCache && !run.skipValidation {
		ev, fail = validation.Probe(probeCtx, *req.Validation, res.Candidate.Resource.Conn,
			s.probeOptions(req.Validation.Mode), requestID)
	}
	cancelProbe()
	if run.skipValidation {
		s.releaseReservationBounded(requestID, res.ReservationID)
		if cachedOK, cachedEV := s.stickyEvidenceStillValid(ctx, res, &req); cachedOK {
			item, cerr := s.prepareCandidate(ctx, res, cachedEV, req, requestID)
			if cerr == nil {
				run.delivered.Add(1)
				run.mu.Lock()
				run.prepared = append(run.prepared, preparedCandidate{
					item: item, offerID: res.Candidate.Offer.ID,
					reservationID: res.ReservationID,
					contextKey:    res.ContextKey, failureEpoch: res.FailureEpoch,
					candidate: res.Candidate,
				})
				run.mu.Unlock()
			}
		} else {
			run.recordFailure(validation.FailTarget)
		}
		return
	}
	scopeHash := domain.EvidenceScopeHash(*req.Validation)
	if fail == nil {
		// Fresh PASS evidence is cached per scope+incarnation so short-TTL
		// reuse can skip the next probe; API-fetched ephemeral candidates
		// are always probed because a vendor batch must be verified live.
		if !fromCache && !res.Candidate.Ephemeral && s.cfg.EffectiveEvidenceCacheTTL() > 0 {
			_ = s.store.PutEvidenceProduction(ctx, s.evidenceKey(res.Candidate),
				scopeHash, ev)
		}
	} else if fail.Attributed() && !res.Candidate.Ephemeral && s.cfg.EffectiveEvidenceCacheTTL() > 0 {
		// The newest negative result hides any older PASS for the same key.
		ev.Outcome = "FAIL"
		_ = s.store.PutEvidenceProduction(ctx, s.evidenceKey(res.Candidate),
			scopeHash, ev)
	}
	if derr := recordAttempt(overallCtx, s.store, ev, res, fail); derr != nil {
		if overallCtx.Err() != nil {
			// Deadline/cancellation is request-scoped, not a storage outage.
			s.releaseReservationBounded(requestID, res.ReservationID)
			return
		}
		run.mu.Lock()
		if run.reserveErr == nil {
			run.reserveErr = fmt.Errorf("record validation attempt: %w", derr)
		}
		run.mu.Unlock()
	}
	if fail == nil {
		obs := recordObservation(ctx, s.store, s.geo, res.Candidate.Resource.ID, ev, s.Now())
		s.recordSourceOutcome(ctx, res.Candidate.Offer.SourceID,
			res.Candidate.SourceConfigVersion, "PASS")
		if !countryMatches(req.Filters.Countries, obs) {
			// A wrong/unverifiable exit country is not a proxy failure:
			// release the slot and try another candidate.
			s.releaseReservationBounded(requestID, res.ReservationID)
			run.countrySkip.Add(1)
			return
		}
		item, cerr := s.prepareCandidate(overallCtx, res, ev, req, requestID)
		if cerr != nil {
			s.releaseReservationBounded(requestID, res.ReservationID)
			return
		}
		item.Observed = obs
		// A successful probe is a fresh PASS for health ranking. It does not
		// open a CD window; reservation release happens in the final
		// transaction so a late response never commits an orphan lease.
		if !res.Candidate.Ephemeral {
			_ = s.health.RecordScoped(ctx, res.Candidate.Resource.ID, res.ContextKey,
				req.Profile, "PASS", res.Candidate.Incarnation)
		}
		run.delivered.Add(1)
		run.mu.Lock()
		run.prepared = append(run.prepared, preparedCandidate{
			item: item, offerID: res.Candidate.Offer.ID, reservationID: res.ReservationID,
			contextKey:   res.ContextKey,
			failureEpoch: res.FailureEpoch, candidate: res.Candidate,
		})
		run.mu.Unlock()
		return
	}
	s.releaseReservationBounded(requestID, res.ReservationID)
	run.recordFailure(fail.Kind)
	if fail.Kind == validation.FailAuth && !res.Candidate.Ephemeral {
		fingerprint := s.fingerprintConn(res.Candidate.Resource.Conn)
		retired, err := s.store.RetireResourceOwned(ctx, res.Candidate.Resource.ID,
			res.Candidate.Incarnation, res.Candidate.Offer.SourceID,
			res.Candidate.SourceConfigVersion, "AUTH_FAILED", s.Now())
		if err == nil && retired > 0 {
			_, _ = s.store.ExpireBindingsForFingerprints(ctx, []string{fingerprint}, s.Now())
			_ = s.store.InvalidateEvidence(ctx, []string{fingerprint})
		}
	}
	if fail.Attributed() {
		if !res.Candidate.Ephemeral {
			_ = s.health.RecordScoped(ctx, res.Candidate.Resource.ID, res.ContextKey,
				req.Profile, "FAIL", res.Candidate.Incarnation)
		}
		s.recordSourceOutcome(ctx, res.Candidate.Offer.SourceID,
			res.Candidate.SourceConfigVersion, "FAIL")
	}
}

func (s *AcquireService) recordSourceOutcome(ctx context.Context, sourceID string,
	sourceConfigVersion int64, outcome string) {
	_, _ = s.store.RecordSourceOutcomeOwned(ctx, sourceID, sourceConfigVersion,
		outcome, s.Now(), s.sourcePolicy)
}

func normalizeCountries(in []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(in))
	for _, c := range in {
		c = strings.ToUpper(strings.TrimSpace(c))
		if c == "" || seen[c] {
			continue
		}
		seen[c] = true
		out = append(out, c)
	}
	return out
}

func recordAttempt(ctx context.Context, store *sqlite.Store, ev domain.Evidence, res sqlite.Reserved, fail *validation.Failure) *domain.Error {
	detail := ""
	if fail != nil {
		ev.Outcome = "FAIL"
		detail = fail.Kind + ": " + fail.Detail
	}
	if err := store.RecordAttemptAttributed(ctx, ev, res.Candidate.Resource.ID,
		res.ContextKey, detail, res.Candidate.Incarnation,
		res.Candidate.Offer.SourceID, res.SourceConfigVersion); err != nil {
		return domain.StoreUnavailable(err)
	}
	return nil
}

func countryMatches(filters []string, obs *domain.Observation) bool {
	if len(filters) == 0 {
		return true
	}
	if obs == nil || obs.CountryCode == "" {
		return false
	}
	for _, c := range filters {
		if strings.EqualFold(c, obs.CountryCode) {
			return true
		}
	}
	return false
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
		ReservationID:               res.ReservationID,
		Owner:                       requestID,
		LeaseID:                     leaseID,
		RequestID:                   requestID,
		Tenant:                      "default",
		Mode:                        req.Lease.Mode,
		Deadline:                    deadline,
		ConnJSON:                    string(connJSON),
		ValidationJSON:              string(valJSON),
		ExpectedFence:               res.Fence,
		ExpectedEpoch:               res.FailureEpoch,
		Profile:                     req.Profile,
		ConsumerID:                  req.ConsumerID,
		ProjectID:                   req.ProjectID,
		KeyID:                       req.KeyID,
		Fingerprint:                 s.fingerprintConn(res.Candidate.Resource.Conn),
		SourceID:                    res.Candidate.Offer.SourceID,
		ExpectedSourceConfigVersion: res.SourceConfigVersion,
		Ephemeral:                   res.Candidate.Ephemeral,
		MaxPoolLeases:               res.Candidate.Offer.MaxPoolLeases,
		DeliveryCooldown:            time.Duration(req.CooldownMS) * time.Millisecond,
		Now:                         now,
	})
	if err != nil {
		return AcquiredItem{}, err
	}
	return AcquiredItem{
		LeaseID:             leaseID,
		ResourceID:          res.Candidate.Resource.ID,
		SourceID:            res.Candidate.Offer.SourceID,
		SourceConfigVersion: res.Candidate.SourceConfigVersion,
		Incarnation:         res.Candidate.Incarnation,
		Ephemeral:           res.Candidate.Ephemeral,
		Conn:                res.Candidate.Resource.Conn,
		Validation:          ev,
		ExpiresAt:           deadline,
	}, nil
}

// prepareCandidate is a side-effect-free pre-dispatch step. It deliberately
// does not insert a lease or CD row: only the final selected response set is
// committed, so a slow/removed sibling cannot strand an irreversible window.
func (s *AcquireService) prepareCandidate(ctx context.Context, res sqlite.Reserved,
	ev domain.Evidence, req AcquireRequest, requestID string) (AcquiredItem, error) {
	now := s.Now()
	deadline := s.leaseDeadline(res.Candidate, now, req)
	if usable := deadline.Sub(now); usable < time.Duration(req.Lease.MinUsableMS)*time.Millisecond {
		return AcquiredItem{}, fmt.Errorf("usable window %s below required %dms",
			usable, req.Lease.MinUsableMS)
	}
	conn := res.Candidate.Resource.Conn
	leaseID := domain.NewID("lease_")
	// InsertDirectLease remains for legacy callers; final delivery owns this
	// ID and writes it atomically with CD/binding.
	if conn.Host == "" {
		return AcquiredItem{}, fmt.Errorf("candidate has no connection")
	}
	return AcquiredItem{
		LeaseID:             leaseID,
		ResourceID:          res.Candidate.Resource.ID,
		SourceID:            res.Candidate.Offer.SourceID,
		SourceConfigVersion: res.SourceConfigVersion,
		Incarnation:         res.Candidate.Incarnation,
		Ephemeral:           res.Candidate.Ephemeral,
		Conn:                conn,
		Validation:          ev,
		ExpiresAt:           deadline,
	}, nil
}

// stickyEvidenceStillValid checks the exact persisted PASS for a bound
// resource without accepting a basic PASS as target-TLS or a different
// source version/incarnation. The final transaction repeats this guard.
func (s *AcquireService) stickyEvidenceStillValid(ctx context.Context,
	res sqlite.Reserved, req *AcquireRequest) (bool, domain.Evidence) {
	if req.Validation == nil {
		return false, domain.Evidence{}
	}
	scopeHash := domain.EvidenceScopeHash(*req.Validation)
	hits, err := s.store.RecentEvidence(ctx, scopeHash,
		[]sqlite.EvidenceKey{s.evidenceKey(res.Candidate)}, s.Now().Add(-s.cfg.MaxProofAge()))
	if err != nil {
		return false, domain.Evidence{}
	}
	ev, ok := hits[s.fingerprintConn(res.Candidate.Resource.Conn)]
	if !ok {
		return false, domain.Evidence{}
	}
	if ev.Mode != req.Validation.Mode || ev.Target != req.Validation.TargetHostPort ||
		ev.SecurityHash != req.Validation.SecurityHash {
		return false, domain.Evidence{}
	}
	ev.CacheHit = true
	return true, ev
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
