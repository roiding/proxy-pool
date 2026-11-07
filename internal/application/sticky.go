package application

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"strings"
	"time"

	"proxypool/internal/domain"
	"proxypool/internal/storage/sqlite"
	"proxypool/internal/validation"
)

// stickyBindingKey derives the binding cipher key from the deployment
// identity secret with its own domain separation; rotation of the deployment
// secret invalidates every stored binding ciphertext instead of leaking.
func stickyBindingKey(secret string) []byte {
	sum := sha256.Sum256([]byte("proxy-pool/sticky-binding/v1\x00" + secret))
	return sum[:]
}

func (s *AcquireService) encryptBindingConn(conn domain.Connection) (string, error) {
	plain, err := json.Marshal(conn)
	if err != nil {
		return "", err
	}
	gcm, err := newStickyGCM(s.cfg.IdentitySecret)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	sealed := gcm.Seal(nonce, nonce, plain, nil)
	return base64.RawStdEncoding.EncodeToString(sealed), nil
}

func (s *AcquireService) decryptBindingConn(ciphertext string) (domain.Connection, error) {
	var conn domain.Connection
	raw, err := base64.RawStdEncoding.DecodeString(ciphertext)
	if err != nil {
		return conn, err
	}
	gcm, err := newStickyGCM(s.cfg.IdentitySecret)
	if err != nil {
		return conn, err
	}
	if len(raw) < gcm.NonceSize() {
		return conn, fmt.Errorf("binding ciphertext too short")
	}
	plain, err := gcm.Open(nil, raw[:gcm.NonceSize()], raw[gcm.NonceSize():], nil)
	if err != nil {
		return conn, err
	}
	if err := json.Unmarshal(plain, &conn); err != nil {
		return conn, err
	}
	return conn, nil
}

func newStickyGCM(secret string) (cipher.AEAD, error) {
	block, err := aes.NewCipher(stickyBindingKey(secret))
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// acquireSticky serves one sticky session request.
//
// The project+session binding slot is claimed atomically before any vendor
// fetch, so two concurrent first requests never buy the same upstream twice:
// one claims and fetches, the other waits bounded for the claim to resolve.
// A live binding is reused without re-probing and without opening a new CD
// window; an expired or unusable binding is rebound inside the remaining
// request budget, and the rebind passes through the ordinary CD/eligibility
// path, so a fresh connection never bypasses an open cooldown. Rebinds are
// written with a version CAS, and only the claim owner may release a failed
// claim, so slow old writers cannot overwrite a newer binding.
func (s *AcquireService) acquireSticky(ctx context.Context, overallCtx context.Context,
	req *AcquireRequest, requestID string, deadline time.Time) (AcquireResult, *domain.Error) {
	now := s.Now()
	claimDeadline := deadline.Add(-finalizeBudget(deadline.Sub(now)))
	if ctx.Err() != nil {
		return inconclusiveStickyResult(requestID, req), nil
	}
	binding, reused, claimed, err := s.store.ClaimStickySession(ctx,
		req.ProjectID, req.SessionID, requestID, claimDeadline, now)
	if err != nil {
		return AcquireResult{}, domain.StoreUnavailable(err)
	}
	if reused {
		if res, derr := s.deliverStickyReuse(ctx, req, binding, requestID); derr == nil {
			return res, nil
		} else if res, inconclusive := s.inconclusiveRefreshResult(ctx, requestID, req, derr); inconclusive {
			return res, nil
		} else if derr.Status >= 500 {
			return AcquireResult{}, derr
		}
		// A live binding can fail current eligibility or its persisted proof
		// can be superseded. Convert that exact version to CLAIMING; using the
		// generic claim here would simply return the same ACTIVE row again and
		// leave the request waiting until its HTTP timeout.
		ok, cerr := s.store.ClaimStickyRebind(ctx, req.ProjectID, req.SessionID,
			requestID, binding.Version, claimDeadline, s.Now())
		if cerr != nil {
			return AcquireResult{}, domain.StoreUnavailable(cerr)
		}
		if !ok {
			return s.waitForStickyClaim(ctx, overallCtx, req, requestID, binding, claimDeadline)
		}
		binding = domain.StickyBinding{
			ProjectID: req.ProjectID, SessionID: req.SessionID, Owner: requestID,
			Version: binding.Version + 1, State: sqlite.BindingClaiming,
			Deadline: claimDeadline,
		}
		claimed = true
	}
	if !claimed {
		return s.waitForStickyClaim(ctx, overallCtx, req, requestID, binding, claimDeadline)
	}
	return s.rebindSticky(ctx, overallCtx, req, requestID, binding)
}

// inconclusiveRefreshResult reports whether a failed sticky reuse must be
// returned as an explained business shortfall instead of a rebind trigger:
// probe-budget exhaustion and caller cancellation prove nothing about the
// binding itself, so the recoverable binding, its proof, and its CD must
// stay untouched. A canceled request context turns even a storage failure
// into the same explained shortfall because the outcome is unknowable to
// this caller; the binding remains recoverable either way.
func (s *AcquireService) inconclusiveRefreshResult(ctx context.Context,
	requestID string, req *AcquireRequest, derr *domain.Error) (AcquireResult, bool) {
	if derr == nil {
		return AcquireResult{}, false
	}
	if derr.Code != "VALIDATION_BUDGET_EXHAUSTED" && ctx.Err() == nil {
		return AcquireResult{}, false
	}
	return inconclusiveStickyResult(requestID, req), true
}

func inconclusiveStickyResult(requestID string, req *AcquireRequest) AcquireResult {
	return AcquireResult{
		RequestID: requestID, Requested: req.Count, Delivered: 0,
		Missing:         req.Count,
		Items:           []AcquiredItem{},
		ShortfallReason: "VALIDATION_BUDGET_EXHAUSTED",
	}
}

// detachedContext backs post-commit response assembly and claim cleanup with
// a short independent deadline: an already-canceled request context must not
// strand its own CLAIMING row until the old claim deadline nor hide the
// durable binding facts of an already-committed delivery.
func (s *AcquireService) detachedContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

// failStickyClaimOwned releases this request's own claim on a detached
// context. The store keeps its owner+version+CLAIMING guards, so the
// release stays owned, bounded, idempotent, and never touches a newer claim.
func (s *AcquireService) failStickyClaimOwned(projectID, sessionID, owner string,
	version int64) {
	ctx, cancel := s.detachedContext()
	defer cancel()
	_ = s.store.FailStickyClaim(ctx, projectID, sessionID, owner, version, s.Now())
}

// waitForStickyClaim waits bounded for another request's rebind to resolve
// instead of fetching a duplicate paid connection. When the claim resolves
// to a usable binding, the caller reuses it; when the claim lapses, this
// request takes the slot over and fetches.
func (s *AcquireService) waitForStickyClaim(ctx context.Context, overallCtx context.Context,
	req *AcquireRequest, requestID string, binding domain.StickyBinding, claimDeadline time.Time) (AcquireResult, *domain.Error) {
	busy := func() (AcquireResult, *domain.Error) {
		return AcquireResult{
			RequestID: requestID, Requested: req.Count, Delivered: 0,
			Missing:         req.Count,
			Items:           []AcquiredItem{},
			ShortfallReason: "STICKY_SESSION_BUSY",
		}, nil
	}
	for {
		if binding.State == sqlite.BindingActive && binding.Deadline.After(s.Now()) {
			if res, derr := s.deliverStickyReuse(ctx, req, binding, requestID); derr == nil {
				return res, nil
			} else if res, inconclusive := s.inconclusiveRefreshResult(ctx, requestID, req, derr); inconclusive {
				return res, nil
			} else if derr.Status >= 500 {
				return AcquireResult{}, derr
			}
			// Unusable snapshot: try to take the slot over with a fresh claim.
		}
		if s.Now().After(claimDeadline) || ctx.Err() != nil {
			return busy()
		}
		select {
		case <-time.After(25 * time.Millisecond):
		case <-ctx.Done():
			return busy()
		}
		if ctx.Err() != nil {
			return busy()
		}
		next, reused, claimed, err := s.store.ClaimStickySession(ctx,
			req.ProjectID, req.SessionID, requestID, claimDeadline, s.Now())
		if err != nil {
			return AcquireResult{}, domain.StoreUnavailable(err)
		}
		if reused {
			if res, derr := s.deliverStickyReuse(ctx, req, next, requestID); derr == nil {
				return res, nil
			} else if derr.Status >= 500 {
				return AcquireResult{}, derr
			}
			ok, cerr := s.store.ClaimStickyRebind(ctx, req.ProjectID, req.SessionID,
				requestID, next.Version, claimDeadline, s.Now())
			if cerr != nil {
				return AcquireResult{}, domain.StoreUnavailable(cerr)
			}
			if ok {
				claim := domain.StickyBinding{
					ProjectID: req.ProjectID, SessionID: req.SessionID,
					Owner: requestID, Version: next.Version + 1,
					State: sqlite.BindingClaiming, Deadline: claimDeadline,
				}
				return s.rebindSticky(ctx, overallCtx, req, requestID, claim)
			}
			continue
		}
		if claimed {
			return s.rebindSticky(ctx, overallCtx, req, requestID, next)
		}
		binding = next
	}
}

// rebindSticky fetches one connection through the ordinary path (which
// enforces project CD, key state, and all eligibility guards) and binds it
// to the session with a version CAS. The claim is released on failure so the
// next request can retry immediately.
func (s *AcquireService) rebindSticky(ctx context.Context, overallCtx context.Context,
	req *AcquireRequest, requestID string, claim domain.StickyBinding) (AcquireResult, *domain.Error) {
	runReq := *req
	runReq.Lease.Mode = "exclusive"
	if claim.Deadline.Sub(s.Now()) <= 0 {
		s.failStickyClaimOwned(req.ProjectID, req.SessionID, requestID, claim.Version)
		return inconclusiveStickyResult(requestID, req), nil
	}
	commit := &sqlite.StickyBindingCommit{
		Binding: domain.StickyBinding{
			ProjectID: req.ProjectID, SessionID: req.SessionID, Owner: requestID,
		},
		ExpectedVersion: claim.Version,
		Owner:           requestID,
	}
	result, derr := s.runOrdinaryWithPending(ctx, overallCtx, &runReq, requestID,
		claim.Deadline, commit)
	if derr != nil || result.Delivered != 1 || len(result.Items) != 1 {
		s.failStickyClaimOwned(req.ProjectID, req.SessionID, requestID, claim.Version)
		if derr != nil {
			return result, derr
		}
		// A valid empty result is the ordinary business shortfall; never
		// leave the session claim in CLAIMING for later waiters.
		return result, nil
	}
	result.SessionID = req.SessionID
	result.SessionChanged = true
	result.ChangeReason = "REBIND"
	result.BindingVersion = claim.Version + 1
	result.Items[0].SessionBindingVersion = claim.Version + 1
	result.Items[0].SessionChanged = true
	result.Items[0].SessionChangeReason = "REBIND"
	ctx, cancel := s.detachedContext()
	defer cancel()
	if binding, err := s.store.GetBinding(ctx, req.ProjectID, req.SessionID); err == nil &&
		binding.State == sqlite.BindingActive {
		result.BindingVersion = binding.Version
		result.BindingExpiresAt = binding.Deadline
		result.Items[0].SessionBindingVersion = binding.Version
	}
	return result, nil
}

const stickyProofRefreshedCode = "STICKY_BINDING_REFRESHED"

// deliverStickyReuse hands out the bound connection again. It deliberately
// opens no new CD window and does not extend the binding: reuse inside the
// binding's lifetime neither resets the cooldown nor grants a longer lease
// than the binding's own deadline. Disabled projects or keys cannot reuse.
func (s *AcquireService) deliverStickyReuse(ctx context.Context, req *AcquireRequest,
	b domain.StickyBinding, requestID string) (AcquireResult, *domain.Error) {
	return s.deliverStickyReuseAttempt(ctx, req, b, requestID, true)
}

func (s *AcquireService) deliverStickyReuseAttempt(ctx context.Context, req *AcquireRequest,
	b domain.StickyBinding, requestID string, retryRefreshed bool) (AcquireResult, *domain.Error) {
	now := s.Now()
	if proj, err := s.store.GetProject(ctx, req.ProjectID); err != nil || !proj.Enabled {
		return AcquireResult{}, domain.NotFound("project")
	}
	if req.KeyID != "" {
		if key, err := s.store.APIKeyByID(ctx, req.KeyID); err != nil || !key.Enabled {
			return AcquireResult{}, domain.NotFound("api key")
		}
	}
	conn, err := s.decryptBindingConn(b.ConnectionCiphertext)
	if err != nil {
		return AcquireResult{}, domain.E("STICKY_BINDING_UNUSABLE", 409, "sticky binding cannot be reused")
	}
	src, err := s.store.GetSource(ctx, b.SourceID)
	if err != nil {
		return AcquireResult{}, domain.AsError(err)
	}
	if !src.Enabled || src.ConfigVersion != b.SourceConfigVersion {
		return AcquireResult{}, domain.E("STICKY_BINDING_UNAVAILABLE", 409, "sticky source is unavailable")
	}
	if b.Incarnation != "" && !isEphemeralBinding(b) {
		ok, err := s.stickyResourceLive(ctx, b)
		if err != nil {
			return AcquireResult{}, domain.StoreUnavailable(err)
		}
		if !ok {
			return AcquireResult{}, domain.E("STICKY_BINDING_UNAVAILABLE", 409, "sticky resource is unavailable")
		}
	}
	project, err := s.store.GetProject(ctx, req.ProjectID)
	if err != nil {
		return AcquireResult{}, domain.AsError(err)
	}
	if !project.SourceAllowed(b.SourceID) {
		return AcquireResult{}, domain.E("STICKY_BINDING_UNAVAILABLE", 409, "sticky source is outside project scope")
	}
	if req.Filters.SourceIDs != nil && !containsString(req.Filters.SourceIDs, b.SourceID) {
		return AcquireResult{}, domain.E("STICKY_BINDING_UNAVAILABLE", 409, "sticky source is outside request scope")
	}
	if !transportAllowed(req.Filters.Transports, conn.Transport) {
		return AcquireResult{}, domain.E("STICKY_BINDING_UNAVAILABLE", 409, "sticky protocol does not match")
	}
	if req.Validation == nil || b.Validation.Mode != req.Validation.Mode ||
		b.Validation.Target != req.Validation.TargetHostPort ||
		b.Validation.Profile != req.Validation.Profile ||
		b.Validation.SecurityHash != req.Validation.SecurityHash {
		return AcquireResult{}, domain.E("STICKY_BINDING_UNAVAILABLE", 409, "sticky validation scope does not match")
	}
	circuitOpen, err := s.store.SourceCircuitOpen(ctx, b.SourceID, now)
	if err != nil {
		return AcquireResult{}, domain.StoreUnavailable(err)
	}
	if circuitOpen {
		return AcquireResult{}, domain.E("STICKY_BINDING_UNAVAILABLE", 409, "sticky source circuit is cooling")
	}
	if !b.Deadline.After(now) {
		return AcquireResult{}, domain.E("STICKY_BINDING_EXPIRED", 409, "sticky binding expired")
	}

	proofAge := time.Duration(0)
	ev := b.Validation
	staticProofRequired := false
	proofFresh := !ev.CompletedAt.IsZero() && !ev.CompletedAt.After(now) &&
		(proofAge == 0 || now.Sub(ev.CompletedAt) <= proofAge)
	if b.ResourceID != "" && !isEphemeralBinding(b) {
		scopeHash := domain.EvidenceScopeHash(*req.Validation)
		hits, err := s.store.RecentEvidence(ctx, scopeHash,
			[]sqlite.EvidenceKey{{
				ResourceID: b.ResourceID, Fingerprint: b.Fingerprint,
				SourceID: b.SourceID, SourceConfigVersion: b.SourceConfigVersion,
				Incarnation: b.Incarnation,
			}}, time.Time{})
		if err != nil {
			return AcquireResult{}, domain.StoreUnavailable(err)
		}
		current, ok := hits[b.Fingerprint]
		if !ok || !current.CompletedAt.Equal(ev.CompletedAt) {
			proofFresh = false
		}
	}
	if proofFresh && len(req.Filters.Countries) > 0 && b.ResourceID != "" && !isEphemeralBinding(b) {
		obs, err := s.store.ResourceObservation(ctx, b.ResourceID)
		if err != nil {
			return AcquireResult{}, domain.StoreUnavailable(err)
		}
		if !countryMatches(req.Filters.Countries, obs) ||
			obs.ExitIP != ev.ObservedExitIP {
			return AcquireResult{}, domain.E("STICKY_BINDING_UNAVAILABLE", 409,
				"sticky country observation no longer matches")
		}
	}
	if strings.HasPrefix(b.Incarnation, "static_") {
		if static, ok := s.Refiller.(StaticBindingEvidenceProvider); ok {
			candidate, present := static.StaticBindingCandidate(b.SourceID, b.SourceConfigVersion, b.Fingerprint)
			staticProofRequired = present
			if proofFresh {
				proofFresh = false
				if present {
					if evidence, ok := s.Refiller.(StaticEvidenceProvider); ok {
						current, _, hit := evidence.StaticRecentEvidence(candidate, domain.EvidenceScopeHash(*req.Validation), ev.CompletedAt)
						// A later PASS with another exit cannot authorize the old
						// private observation/country. Revalidate the bound connection
						// instead of presenting yesterday's exit as the current one.
						proofFresh = hit && current.ObservedExitIP == ev.ObservedExitIP
					}
				}
			}
		}
	}
	if proofFresh && len(req.Filters.Countries) > 0 &&
		!s.stickyCountryMatches(b, req.Filters.Countries) {
		return AcquireResult{}, domain.E("STICKY_BINDING_UNAVAILABLE", 409,
			"sticky country cannot be revalidated")
	}

	leaseDeadline := now.Add(time.Duration(req.Lease.TTLMS) * time.Millisecond)
	if leaseDeadline.After(b.Deadline) {
		leaseDeadline = b.Deadline
	}
	retention, providerExpiry, err := s.store.StickyReuseRetention(ctx, b.SourceID, b.ResourceID,
		b.Incarnation, b.SourceConfigVersion)
	if err != nil {
		return AcquireResult{}, domain.StoreUnavailable(err)
	}
	if retention != nil && leaseDeadline.After(*retention) {
		leaseDeadline = *retention
	}
	if !leaseDeadline.After(now) ||
		leaseDeadline.Sub(now) < s.cfg.MinUsable() {
		return AcquireResult{}, domain.E("STICKY_BINDING_EXPIRED", 409,
			"sticky binding has no usable current retention")
	}
	revalidated := false
	if !proofFresh {
		var derr *domain.Error
		ev, derr = s.revalidateStickyBinding(ctx, req, b, conn, requestID)
		if derr != nil {
			if retryRefreshed && derr.Code == stickyProofRefreshedCode {
				return s.retryRefreshedSticky(ctx, req, b, requestID)
			}
			return AcquireResult{}, derr
		}
		revalidated = true
	} else {
		ev.CacheHit = true
	}
	ev = deliveryEvidence(ev, proofAge)
	connJSON, err := json.Marshal(conn)
	if err != nil {
		return AcquireResult{}, domain.AsError(err)
	}
	leaseID := domain.NewID("lease_")
	if strings.HasPrefix(b.Incarnation, "static_") {
		if static, ok := s.Refiller.(StaticBindingEvidenceProvider); ok {
			release, eligible, err := static.LockStaticBindingDelivery(ctx, b.SourceID, b.SourceConfigVersion, b.Fingerprint, domain.EvidenceScopeHash(*req.Validation), ev, staticProofRequired)
			if err != nil {
				return AcquireResult{}, domain.E("VALIDATION_BUDGET_EXHAUSTED", 409, "static binding guard canceled")
			}
			defer release()
			if !eligible {
				return AcquireResult{}, domain.E("STICKY_BINDING_UNAVAILABLE", 409, "static binding proof was superseded before delivery")
			}
		}
	}
	finalDeadline, err := s.store.CommitStickyReuse(ctx, sqlite.StickyReuseArgs{
		ProjectID: req.ProjectID, SessionID: req.SessionID,
		ExpectedVersion: b.Version, KeyID: req.KeyID, ConsumerID: req.ConsumerID,
		Fingerprint: b.Fingerprint, SourceID: b.SourceID,
		SourceConfigVersion: b.SourceConfigVersion, ResourceID: b.ResourceID,
		Incarnation: b.Incarnation, ScopeHash: domain.EvidenceScopeHash(*req.Validation),
		Proof: ev, MaxProofAge: proofAge,
		RequestID: requestID, LeaseID: leaseID, Mode: req.Lease.Mode,
		LeaseDeadline: leaseDeadline, ConnJSON: string(connJSON), Now: now,
		MinUsable: s.cfg.MinUsable(), BindingCiphertext: b.ConnectionCiphertext,
		DeliverySafety: s.cfg.ExpirySafetyMargin(), ValidateCountry: len(req.Filters.Countries) > 0,
		CountryExitIP: ev.ObservedExitIP, Revalidated: revalidated,
		CountryCodes: append([]string(nil), req.Filters.Countries...),
	})
	if err != nil {
		if retryRefreshed && errors.Is(err, sqlite.ErrStale) {
			return s.retryRefreshedSticky(ctx, req, b, requestID)
		}
		if errors.Is(err, sqlite.ErrNotEligible) || errors.Is(err, sqlite.ErrStale) {
			return AcquireResult{}, domain.E("STICKY_BINDING_EXPIRED", 409,
				"sticky binding is no longer eligible")
		}
		return AcquireResult{}, domain.StoreUnavailable(err)
	}
	item := AcquiredItem{
		LeaseID:               leaseID,
		ProxyURI:              proxyURIFrom(conn),
		ProviderExpiresAt:     utcTimePtr(providerExpiry),
		ResourceID:            stickyDeliveryResourceID(b),
		SourceID:              b.SourceID,
		Conn:                  conn,
		Validation:            ev,
		ExpiresAt:             finalDeadline.UTC(),
		SessionBindingVersion: b.Version,
	}
	if ev.ObservedExitIP != "" {
		item.Observed = s.observationForEvidence(ev, s.Now())
	}
	return AcquireResult{
		RequestID:        requestID,
		Requested:        1,
		Delivered:        1,
		Missing:          0,
		Items:            []AcquiredItem{item},
		CooldownSeconds:  req.CooldownMS / 1000,
		SessionID:        req.SessionID,
		BindingVersion:   b.Version,
		BindingExpiresAt: b.Deadline,
	}, nil
}

// retryRefreshedSticky resolves an A/B proof-refresh race without turning a
// late stale writer into a new allocation. A concurrent refresh updates only
// the proof and leaves the binding version/identity intact, so re-reading that
// same ACTIVE binding lets the late request reuse the newer proof.
func (s *AcquireService) retryRefreshedSticky(ctx context.Context, req *AcquireRequest,
	previous domain.StickyBinding, requestID string) (AcquireResult, *domain.Error) {
	current, err := s.store.GetBinding(ctx, req.ProjectID, req.SessionID)
	if err != nil {
		return AcquireResult{}, domain.StoreUnavailable(err)
	}
	if current.State != sqlite.BindingActive || current.Version != previous.Version ||
		current.ConnectionCiphertext != previous.ConnectionCiphertext ||
		current.Fingerprint != previous.Fingerprint {
		return AcquireResult{}, domain.E("STICKY_BINDING_UNAVAILABLE", 409,
			"sticky binding changed during proof refresh")
	}
	return s.deliverStickyReuseAttempt(ctx, req, current, requestID, false)
}

// revalidateStickyBinding probes the already-bound connection when its
// persisted proof aged out or was superseded. This is not a rebind: no vendor
// fetch is made, no project CD window is renewed, and the binding deadline is
// unchanged. Only a successful fresh proof replaces the persisted proof.
func (s *AcquireService) revalidateStickyBinding(ctx context.Context,
	req *AcquireRequest, b domain.StickyBinding, conn domain.Connection,
	requestID string) (domain.Evidence, *domain.Error) {
	probeCtx, cancel := context.WithTimeout(ctx, s.cfg.ProbeTotalTimeout())
	defer cancel()
	var (
		ev       domain.Evidence
		fail     *validation.Failure
		acquired bool
	)
	select {
	case s.probeSlots <- struct{}{}:
		acquired = true
		ev, fail = validation.Probe(probeCtx, *req.Validation, conn,
			s.probeOptions(req.Validation.Mode), requestID)
		<-s.probeSlots
	case <-probeCtx.Done():
	}
	if !acquired {
		return domain.Evidence{}, domain.E("VALIDATION_BUDGET_EXHAUSTED", 409,
			"sticky proof refresh did not obtain a probe slot")
	}
	detail := ""
	if fail != nil {
		ev.Outcome = "FAIL"
		detail = fail.Kind + ": " + fail.Detail
	}
	contextKey := "sticky|" + req.SessionID
	attemptSource := b.SourceID
	attemptVersion := b.SourceConfigVersion
	if fail != nil && (fail.Kind == validation.FailBudget || ctx.Err() != nil) {
		attemptSource, attemptVersion = "", 0
	}
	if err := s.store.RecordAttemptAttributed(ctx, ev, b.ResourceID, contextKey,
		detail, b.Incarnation, attemptSource, attemptVersion); err != nil {
		if ctx.Err() != nil {
			return domain.Evidence{}, domain.E("VALIDATION_BUDGET_EXHAUSTED", 409,
				"sticky proof refresh was canceled")
		}
		return domain.Evidence{}, domain.StoreUnavailable(err)
	}
	if strings.HasPrefix(b.Incarnation, "static_") && (fail == nil || fail.Attributed()) && ctx.Err() == nil {
		if static, ok := s.Refiller.(StaticBindingEvidenceProvider); ok {
			if candidate, present := static.StaticBindingCandidate(b.SourceID, b.SourceConfigVersion, b.Fingerprint); present {
				if evidence, ok := s.Refiller.(StaticEvidenceProvider); ok {
					if probeHasVerdict(ctx, fail) {
						if ingest, ok := s.Refiller.(*IngestService); ok {
							_ = ingest.MarkStaticUnavailable(ctx, candidate, req.ProjectID)
						}
					} else {
						_ = evidence.RecordStaticEvidence(ctx, candidate, domain.EvidenceScopeHash(*req.Validation), ev, buildObservation(s.geo, ev, s.Now()))
					}
				}
			}
		}
	}
	if fail != nil {
		if fail.Kind == validation.FailBudget || ctx.Err() != nil {
			return domain.Evidence{}, domain.E("VALIDATION_BUDGET_EXHAUSTED", 409,
				"sticky proof refresh was canceled")
		}
		if !isEphemeralBinding(b) {
			retired, err := s.store.RetireResourceOwned(ctx, b.ResourceID,
				b.Incarnation, b.SourceID, b.SourceConfigVersion, "VALIDATION_FAILED", s.Now())
			if err != nil {
				return domain.Evidence{}, domain.StoreUnavailable(err)
			}
			if retired > 0 {
				_, _ = s.store.ExpireBindingsForFingerprints(ctx,
					[]string{b.Fingerprint}, s.Now())
				_ = s.store.InvalidateEvidence(ctx, []string{b.Fingerprint})
				_ = s.health.RecordScoped(ctx, b.ResourceID, contextKey,
					ev.Profile, "FAIL", b.Incarnation)
				s.recordSourceOutcome(ctx, b.SourceID, b.SourceConfigVersion, "FAIL")
			}
		} else if fail.Attributed() {
			if !isEphemeralBinding(b) {
				_ = s.health.RecordScoped(ctx, b.ResourceID, contextKey,
					ev.Profile, "FAIL", b.Incarnation)
				key := sqlite.EvidenceKey{
					ResourceID: b.ResourceID, Fingerprint: b.Fingerprint,
					SourceID: b.SourceID, SourceConfigVersion: b.SourceConfigVersion,
					Incarnation: b.Incarnation,
				}
				_ = s.store.PutEvidenceProduction(ctx, key,
					domain.EvidenceScopeHash(*req.Validation), ev)
			}
			s.recordSourceOutcome(ctx, b.SourceID, b.SourceConfigVersion, "FAIL")
		}
		return domain.Evidence{}, domain.E("STICKY_BINDING_UNAVAILABLE", 409,
			"sticky connection failed revalidation: %s", fail.Kind)
	}

	if !isEphemeralBinding(b) {
		key := sqlite.EvidenceKey{
			ResourceID: b.ResourceID, Fingerprint: b.Fingerprint,
			SourceID: b.SourceID, SourceConfigVersion: b.SourceConfigVersion,
			Incarnation: b.Incarnation,
		}
		if err := s.store.PutEvidenceProduction(ctx, key,
			domain.EvidenceScopeHash(*req.Validation), ev); err != nil {
			if errors.Is(err, sqlite.ErrStale) {
				return domain.Evidence{}, domain.E(stickyProofRefreshedCode, 409,
					"sticky proof was refreshed during revalidation")
			}
			if errors.Is(err, sqlite.ErrNotEligible) {
				return domain.Evidence{}, domain.E("STICKY_BINDING_UNAVAILABLE", 409,
					"sticky proof became ineligible during revalidation")
			}
			return domain.Evidence{}, domain.StoreUnavailable(err)
		}
		obs := recordObservation(ctx, s.store, s.geo, b.ResourceID, ev, s.Now())
		if len(req.Filters.Countries) > 0 && !countryMatches(req.Filters.Countries, obs) {
			return domain.Evidence{}, domain.E("STICKY_BINDING_UNAVAILABLE", 409,
				"sticky country does not match the fresh proof")
		}
		_ = s.health.RecordScoped(ctx, b.ResourceID, contextKey, ev.Profile,
			"PASS", b.Incarnation)
	} else if len(req.Filters.Countries) > 0 {
		obs := s.observationForEvidence(ev, s.Now())
		if !countryMatches(req.Filters.Countries, obs) {
			return domain.Evidence{}, domain.E("STICKY_BINDING_UNAVAILABLE", 409,
				"sticky country does not match the fresh private proof")
		}
	}
	s.recordSourceOutcome(ctx, b.SourceID, b.SourceConfigVersion, "PASS")
	return ev, nil
}

func (s *AcquireService) observationForEvidence(ev domain.Evidence,
	now time.Time) *domain.Observation {
	if ev.ObservedExitIP == "" {
		return nil
	}
	obs := &domain.Observation{ExitIP: ev.ObservedExitIP, ObservedAt: now.UTC()}
	if s.geo != nil {
		if ip, err := netip.ParseAddr(ev.ObservedExitIP); err == nil {
			if loc, ok := s.geo.Lookup(ip); ok {
				obs.CountryCode = loc.CountryCode
				obs.Country = loc.Country
				obs.Region = loc.Region
				obs.City = loc.City
			}
		}
	}
	return obs
}

func (s *AcquireService) stickyCountryMatches(b domain.StickyBinding,
	filters []string) bool {
	if b.Validation.Outcome != "PASS" || b.Validation.CompletedAt.IsZero() ||
		b.Validation.ObservedExitIP == "" || s.geo == nil {
		return false
	}
	ip, err := netip.ParseAddr(b.Validation.ObservedExitIP)
	if err != nil {
		return false
	}
	loc, ok := s.geo.Lookup(ip)
	if !ok {
		return false
	}
	return countryMatches(filters, &domain.Observation{
		ExitIP: b.Validation.ObservedExitIP, CountryCode: loc.CountryCode,
	})
}

// isEphemeralBinding distinguishes a request-local API/static snapshot from
// an inventoried custom resource. Ephemeral bindings carry a batch incarnation
// but intentionally have no proxy_resources row.
func isEphemeralBinding(b domain.StickyBinding) bool {
	return b.ResourceID == "" || strings.HasPrefix(b.Incarnation, "ondemand_") ||
		strings.HasPrefix(b.Incarnation, "static_")
}

func stickyDeliveryResourceID(b domain.StickyBinding) string {
	if b.ResourceID != "" {
		return b.ResourceID
	}
	return b.Fingerprint
}

// stickyResourceLive is the read-side precheck; the lease insert is guarded
// again in deliverStickyReuseTx so a concurrent retire cannot race it.
func (s *AcquireService) stickyResourceLive(ctx context.Context,
	b domain.StickyBinding) (bool, error) {
	return s.store.StickyResourceLive(ctx, b.SourceID, b.ResourceID, b.Incarnation,
		b.SourceConfigVersion, s.Now())
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func transportAllowed(values []domain.Transport, want domain.Transport) bool {
	if len(values) == 0 {
		return true
	}
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
