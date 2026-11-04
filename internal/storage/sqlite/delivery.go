package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"proxypool/internal/domain"
)

var ErrBindingClaimConflict = errors.New("sticky binding claim no longer owned")

// DeliveryCandidate is one already-validated connection prepared for final
// dispatch. Reservations are request-local; no CD or lease is written until
// the final selected set is known.
type DeliveryCandidate struct {
	LeaseID             string
	ReservationID       string
	ResourceID          string
	OfferID             string
	ContextKey          string
	SourceID            string
	SourceConfigVersion int64
	Incarnation         string
	Ephemeral           bool
	Fingerprint         string
	ConnJSON            string
	ValidationJSON      string
	DeadlineText        string
	// BindingDeadlineText is the logical sticky-session deadline, separate
	// from the shorter lease deadline. The final transaction tightens only
	// its provider/retention bound, never its configured binding TTL.
	BindingDeadlineText string
	MaxPoolLeases       int
	Kind                string
	// ExpectedEpoch is the scoped failure epoch captured before the probe.
	ExpectedEpoch int64
	// ExpectedEvidence is the exact validation proof prepared by acquire.
	ExpectedEvidence domain.Evidence
	// ReusesExistingLease means the candidate was already committed by a
	// previous delivery (only possible on a rebind path); final delivery must
	// not insert a duplicate lease.
	ReusesExistingLease bool
	// MinUsable is rechecked in the commit transaction against the final
	// lease deadline after expiry/retention constraints are applied.
	MinUsable time.Duration
	// DeliverySafety bounds provider expiry in the final transaction.
	DeliverySafety time.Duration
}

type stickyReuseGuard struct {
	sourceVersion  int64
	fingerprint    string
	incarnation    string
	connection     string
	validationJSON string
	state          string
	version        int64
	deadline       time.Time
	resourceID     string
	sourceID       string
	scopeHash      string
}

// ResourceObservation returns the last successful signed exit observation
// for an inventoried resource. Ephemeral bindings have no row.
func (s *Store) ResourceObservation(ctx context.Context,
	resourceID string) (*domain.Observation, error) {
	if resourceID == "" {
		return nil, nil
	}
	var obs domain.Observation
	var observedAt string
	err := s.db.QueryRowContext(ctx, `
		SELECT exit_ip, country_code, country, region, city, isp, observed_at
		FROM resource_observations WHERE resource_id = ?`,
		resourceID).Scan(&obs.ExitIP, &obs.CountryCode, &obs.Country,
		&obs.Region, &obs.City, &obs.ISP, &observedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	obs.ObservedAt, err = time.Parse(time.RFC3339Nano, observedAt)
	if err != nil {
		return nil, err
	}
	return &obs, nil
}

func currentStickyReuseDeadline(now time.Time, leaseDeadline time.Time, minUsable time.Duration,
	bindingDeadline time.Time, providerExp, retention *time.Time,
	safety time.Duration) (time.Time, error) {
	if !bindingDeadline.After(now) {
		return time.Time{}, ErrNotEligible
	}
	deadline := leaseDeadline
	if bindingDeadline.Before(deadline) {
		deadline = bindingDeadline
	}
	if providerExp != nil {
		if !providerExp.After(now) {
			return time.Time{}, ErrNotEligible
		}
		if tightened := providerExp.Add(-safety); tightened.Before(deadline) {
			deadline = tightened
		}
	}
	if retention != nil {
		if !retention.After(now) {
			return time.Time{}, ErrNotEligible
		}
		if retention.Before(deadline) {
			deadline = *retention
		}
	}
	if !deadline.After(now) || (minUsable > 0 && deadline.Sub(now) < minUsable) {
		return time.Time{}, ErrNotEligible
	}
	return deadline, nil
}

func stickyBindingInventory(incarnation string) bool {
	return incarnation != "" &&
		!strings.HasPrefix(incarnation, "ondemand_") &&
		!strings.HasPrefix(incarnation, "static_")
}

func (c DeliveryCandidate) Conn() domain.Connection {
	var conn domain.Connection
	_ = json.Unmarshal([]byte(c.ConnJSON), &conn)
	return conn
}

// StickyBindingCommit carries a rebind's binding changes. The claim created
// before candidate work is owned by Owner/ExpectedVersion and is updated in
// the same transaction as the lease and CD.
type StickyBindingCommit struct {
	Binding         domain.StickyBinding
	ExpectedVersion int64
	Owner           string
}

// FinalDelivery returns the chosen candidates and fingerprint-keyed skip
// reasons. Eligibility skips are partial results, not storage failures.
type FinalDelivery struct {
	Items      []DeliveryCandidate
	Skipped    map[string]error
	Dispatched bool
}

// CommitFinalDelivery filters still-eligible candidates in one short
// transaction and commits only that surviving set. An eligibility/capacity
// change drops just that candidate; any genuine database error rolls back
// the whole write set and is surfaced to the caller.
func (s *Store) CommitFinalDelivery(ctx context.Context, a CommitArgs,
	candidates []DeliveryCandidate, sticky *StickyBindingCommit) (FinalDelivery, error) {
	out := FinalDelivery{Items: []DeliveryCandidate{}, Skipped: map[string]error{}}
	err := s.tx(ctx, func(t *sql.Tx) error {
		// The final CD window and capacity checks start from the actual
		// commit time, not the request's pre-probe clock snapshot.
		a.Now = time.Now().UTC()
		out.Items = []DeliveryCandidate{}
		out.Skipped = map[string]error{}
		selected := map[string]bool{}
		if sticky != nil {
			if err := validateStickyClaimTx(t, ctx, *sticky, a.Now); err != nil {
				return err
			}
		}
		for _, input := range candidates {
			if input.Fingerprint != "" && selected[input.Fingerprint] {
				out.Skipped[input.Fingerprint] = ErrNotEligible
				continue
			}
			c, err := validateFinalCandidateTx(t, ctx, a, input)
			if err != nil {
				if isEligibilitySkip(err) {
					out.Skipped[input.Fingerprint] = err
					continue
				}
				return err
			}
			// Fingerprint uniqueness is checked before insertion and the CD
			// row is upserted in this same transaction, so two selected
			// candidates can never open independent windows for one identity.
			if c.Fingerprint != "" {
				selected[c.Fingerprint] = true
			}
			if sticky != nil && len(out.Items) == 0 && c.BindingDeadlineText != "" {
				bindingDeadline, err := time.Parse(time.RFC3339Nano, c.BindingDeadlineText)
				if err != nil {
					return ErrNotEligible
				}
				if !bindingDeadline.After(a.Now) {
					return ErrNotEligible
				}
				sticky.Binding.Deadline = bindingDeadline
			}
			out.Items = append(out.Items, c)
		}

		committed := make([]DeliveryCandidate, 0, len(out.Items))
		for i := range out.Items {
			c := &out.Items[i]
			if c.ReusesExistingLease {
				committed = append(committed, *c)
				continue
			}
			a.LeaseID = c.LeaseID
			a.Fingerprint = c.Fingerprint
			a.SourceID = c.SourceID
			a.ExpectedSourceConfigVersion = c.SourceConfigVersion
			a.Ephemeral = c.Ephemeral
			a.MaxPoolLeases = c.MaxPoolLeases
			if err := insertDeliveryLeaseTx(t, ctx, a, c); err != nil {
				return err
			}
			if c.ReservationID != "" {
				res, err := t.ExecContext(ctx, `
					UPDATE reservations SET state = 'COMMITTED'
					WHERE reservation_id = ? AND owner = ? AND state = 'RESERVED'`,
					c.ReservationID, a.Owner)
				if err != nil {
					return err
				}
				if n, err := res.RowsAffected(); err != nil {
					return err
				} else if n != 1 {
					return ErrStale
				}
			}
			if err := openDeliveryCDTx(t, ctx, a); err != nil {
				return err
			}
			// Capacity is checked after each lease insert. The pre-check above
			// is the normal partial-result filter; reaching this branch means
			// the selected plan itself overfilled, so no response may be sent.
			var active int
			if err := t.QueryRowContext(ctx, `
				SELECT COUNT(*) FROM leases
				WHERE resource_id = ? AND state = 'ACTIVE' AND deadline > ?`,
				c.ResourceID, fmtTime(a.Now)).Scan(&active); err != nil {
				return err
			}
			if active > c.MaxPoolLeases {
				// The pre-check normally filters capacity loss as a partial
				// result. Reaching this branch means this plan overfilled the
				// resource; roll back instead of keeping a skipped write.
				return ErrCapacity
			}
			committed = append(committed, *c)
		}
		// A binding may only become ACTIVE when this transaction actually
		// selected and committed a survivor. An empty candidate set must not
		// consume the claim or leave an ACTIVE snapshot with no delivery.
		if sticky != nil && len(committed) > 0 {
			if err := commitStickyBindingTx(t, ctx, *sticky, a.Now); err != nil {
				return err
			}
		}
		out.Items = committed
		out.Dispatched = true
		return nil
	})
	if err != nil {
		out.Items = nil
	}
	return out, err
}

func isEligibilitySkip(err error) bool {
	return errors.Is(err, ErrNotEligible) || errors.Is(err, ErrStale) ||
		errors.Is(err, ErrCapacity)
}

func validateStickyClaimTx(t *sql.Tx, ctx context.Context,
	s StickyBindingCommit, now time.Time) error {
	var version int64
	var owner, state, deadline string
	err := t.QueryRowContext(ctx, `
		SELECT version, owner, state, deadline FROM sticky_bindings
		WHERE project_id = ? AND session_id = ?`,
		s.Binding.ProjectID, s.Binding.SessionID).
		Scan(&version, &owner, &state, &deadline)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrBindingClaimConflict
	}
	if err != nil {
		return err
	}
	if version != s.ExpectedVersion || owner != s.Owner || state != BindingClaiming {
		return ErrBindingClaimConflict
	}
	claimDeadline, err := time.Parse(time.RFC3339Nano, deadline)
	if err != nil || !claimDeadline.After(now) {
		return ErrBindingClaimConflict
	}
	return nil
}

func insertDeliveryLeaseTx(t *sql.Tx, ctx context.Context, a CommitArgs, c *DeliveryCandidate) error {
	if c.LeaseID == "" {
		return fmt.Errorf("delivery lease id is empty")
	}
	_, err := t.ExecContext(ctx, `
		INSERT INTO leases(lease_id, tenant, request_id, resource_id, offer_id, context_key,
		  mode, fence, deadline, state, connection_json, validation_json, created_at,
		  consumer_id, key_id)
		VALUES(?,?,?,?,?,?,?,0,?, 'ACTIVE', ?, ?, ?, ?, ?)`,
		c.LeaseID, a.Tenant, a.RequestID, c.ResourceID, c.OfferID, c.ContextKey,
		a.Mode, c.DeadlineText, c.ConnJSON, c.ValidationJSON, fmtTime(a.Now),
		a.ConsumerID, a.KeyID)
	return err
}

func validateFinalCandidateTx(t *sql.Tx, ctx context.Context, a CommitArgs,
	c DeliveryCandidate) (DeliveryCandidate, error) {
	if c.Fingerprint == "" || c.SourceID == "" || c.LeaseID == "" || c.DeadlineText == "" {
		return c, fmt.Errorf("incomplete final delivery candidate: %w", ErrNotEligible)
	}
	if err := projectKeyGuardsTx(t, ctx, a.ProjectID, a.KeyID, c.SourceID); err != nil {
		return c, err
	}
	if err := sourceEligibleTx(t, ctx, c.SourceID, c.SourceConfigVersion, a.Now); err != nil {
		return c, err
	}
	if c.ReservationID != "" {
		var owner, state, deadline string
		err := t.QueryRowContext(ctx, `
			SELECT owner, state, deadline FROM reservations WHERE reservation_id = ?`,
			c.ReservationID).Scan(&owner, &state, &deadline)
		if errors.Is(err, sql.ErrNoRows) {
			return c, ErrStale
		}
		if err != nil {
			return c, err
		}
		resDeadline, perr := time.Parse(time.RFC3339Nano, deadline)
		if perr != nil || owner != a.Owner || state != "RESERVED" || !resDeadline.After(a.Now) {
			return c, ErrStale
		}
	}
	if !c.Ephemeral {
		tightened, err := liveResourceEligibleTx(t, ctx, c, a.Now, a.DeliverySafety)
		if err != nil {
			return c, err
		}
		c = tightened
	}
	if blocked, err := projectBlockedTx(t, ctx, a.ProjectID, c.Fingerprint, a.Now); err != nil {
		return c, err
	} else if blocked {
		return c, ErrNotEligible
	}
	var ev domain.Evidence
	if err := json.Unmarshal([]byte(c.ValidationJSON), &ev); err != nil {
		return c, ErrNotEligible
	}
	if ev.Outcome != "PASS" || ev.CompletedAt.IsZero() || ev.CompletedAt.After(a.Now) {
		return c, ErrNotEligible
	}
	if !c.ExpectedEvidence.CompletedAt.IsZero() &&
		!c.ExpectedEvidence.CompletedAt.Equal(ev.CompletedAt) {
		return c, ErrStale
	}
	if a.MaxProofAge > 0 && a.Now.Sub(ev.CompletedAt) > a.MaxProofAge {
		return c, ErrStale
	}
	if c.ResourceID != "" {
		var epoch int64
		err := t.QueryRowContext(ctx, `
			SELECT COALESCE(failure_epoch, 0) FROM scoped_health
			WHERE resource_id = ? AND context_key = ? AND profile = ?`,
			c.ResourceID, c.ContextKey, ev.Profile).Scan(&epoch)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return c, err
		}
		if epoch != c.ExpectedEpoch {
			return c, ErrStale
		}
	}
	deadline, err := time.Parse(time.RFC3339Nano, c.DeadlineText)
	if err != nil {
		return c, ErrNotEligible
	}
	if !deadline.After(a.Now) || deadline.Sub(a.Now) < c.MinUsable {
		return c, ErrNotEligible
	}
	if c.Ephemeral {
		if c.ContextKey == "" {
			c.ContextKey = domain.ContextKey("", domain.SessionStatic)
		}
	}
	if c.MaxPoolLeases <= 0 {
		c.MaxPoolLeases = 1
	}
	var active, pending int
	err = t.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM leases
		WHERE resource_id = ? AND state = 'ACTIVE' AND deadline > ?`,
		c.ResourceID, fmtTime(a.Now)).Scan(&active)
	if err != nil {
		return c, err
	}
	err = t.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM reservations
		WHERE resource_id = ? AND state IN ('RESERVED','VALIDATING')
		  AND deadline > ? AND reservation_id != ?`,
		c.ResourceID, fmtTime(a.Now), c.ReservationID).Scan(&pending)
	if err != nil {
		return c, err
	}
	if active+pending >= c.MaxPoolLeases {
		return c, ErrCapacity
	}
	return c, nil
}

func sourceEligibleTx(t *sql.Tx, ctx context.Context, sourceID string,
	expectedVersion int64, now time.Time) error {
	var enabled int
	var version int64
	var circuit any
	err := t.QueryRowContext(ctx, `
		SELECT s.enabled, s.config_version, sc.cooldown_until
		FROM sources s LEFT JOIN source_circuits sc ON sc.source_id = s.source_id
		WHERE s.source_id = ?`, sourceID).Scan(&enabled, &version, &circuit)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotEligible
	}
	if err != nil {
		return err
	}
	if enabled != 1 || expectedVersion <= 0 || version != expectedVersion {
		return ErrNotEligible
	}
	cool, err := scanTime(circuit)
	if err != nil {
		return err
	}
	if cool != nil && cool.After(now) {
		return ErrNotEligible
	}
	return nil
}

func liveResourceEligibleTx(t *sql.Tx, ctx context.Context,
	c DeliveryCandidate, now time.Time, safety time.Duration) (DeliveryCandidate, error) {
	var (
		disabled    int
		offerState  string
		incarnation string
		providerExp any
		retention   any
		maxLeases   int
	)
	err := t.QueryRowContext(ctx, `
		SELECT r.disabled, o.state, r.incarnation, o.provider_expires_at,
		       o.local_retention_deadline, o.max_pool_leases
		FROM proxy_resources r
		JOIN source_offers o ON o.resource_id = r.resource_id AND o.offer_id = ?
		JOIN sources s ON s.source_id = o.source_id
		WHERE r.resource_id = ? AND o.source_id = ?`,
		c.OfferID, c.ResourceID, c.SourceID).
		Scan(&disabled, &offerState, &incarnation, &providerExp, &retention, &maxLeases)
	if errors.Is(err, sql.ErrNoRows) {
		return c, ErrNotEligible
	}
	if err != nil {
		return c, err
	}
	if disabled != 0 || offerState != domain.OfferEnabled ||
		(c.Incarnation != "" && incarnation != c.Incarnation) {
		return c, ErrNotEligible
	}
	deadline, derr := time.Parse(time.RFC3339Nano, c.DeadlineText)
	if derr != nil {
		return c, ErrNotEligible
	}
	exp, err := scanTime(providerExp)
	if err != nil {
		return c, err
	} else if exp != nil && !exp.After(now) {
		return c, ErrNotEligible
	} else if exp != nil {
		if tightened := exp.Add(-safety); tightened.Before(deadline) {
			deadline = tightened
			c.DeadlineText = fmtTime(tightened)
		}
	}
	ret, err := scanTime(retention)
	if err != nil {
		return c, err
	} else if ret != nil && !ret.After(now) {
		return c, ErrNotEligible
	} else if ret != nil && ret.Before(deadline) {
		deadline = *ret
		c.DeadlineText = fmtTime(*ret)
	}
	if c.BindingDeadlineText != "" {
		bindingDeadline, err := time.Parse(time.RFC3339Nano, c.BindingDeadlineText)
		if err != nil {
			return c, ErrNotEligible
		}
		if exp != nil && exp.Add(-safety).Before(bindingDeadline) {
			bindingDeadline = exp.Add(-safety)
		}
		if ret != nil && ret.Before(bindingDeadline) {
			bindingDeadline = *ret
		}
		if !bindingDeadline.After(now) {
			return c, ErrNotEligible
		}
		c.BindingDeadlineText = fmtTime(bindingDeadline)
	}
	c.MaxPoolLeases = maxLeases
	if !deadline.After(now) {
		return c, ErrNotEligible
	}
	// Reject a newer negative/edited evidence row for the same exact
	// connection, source version, and incarnation. The evidence primary key is
	// (fingerprint, scope), so a later FAIL can legitimately replace the
	// prepared PASS while sibling probes are still finishing.
	var prepared domain.Evidence
	if err := json.Unmarshal([]byte(c.ValidationJSON), &prepared); err != nil {
		return c, ErrNotEligible
	}
	scopeHash := domain.EvidenceScopeHash(domain.ValidationScope{
		Mode: prepared.Mode, TargetHostPort: prepared.Target,
		Profile: prepared.Profile, SecurityHash: prepared.SecurityHash,
	})
	var currentOutcome, currentJSON string
	err = t.QueryRowContext(ctx, `
		SELECT outcome, evidence_json FROM validation_evidence
		WHERE connection_fingerprint = ? AND scope_hash = ?
		  AND source_id = ? AND source_config_version = ? AND incarnation = ?`,
		c.Fingerprint, scopeHash, c.SourceID, c.SourceConfigVersion, c.Incarnation).
		Scan(&currentOutcome, &currentJSON)
	if err == nil {
		var current domain.Evidence
		if json.Unmarshal([]byte(currentJSON), &current) != nil ||
			currentOutcome != "PASS" || current.Outcome != "PASS" {
			return c, ErrStale
		}
		if current.Mode != prepared.Mode ||
			current.Target != prepared.Target ||
			current.Profile != prepared.Profile ||
			current.SecurityHash != prepared.SecurityHash {
			return c, ErrStale
		}
		// A sibling probe may legitimately replace this exact PASS with a
		// newer healthy one. Keep rejecting an older/edited PASS and every
		// negative or malformed row; the shared key already pins source,
		// config version, incarnation, and scope.
		if current.CompletedAt.Before(prepared.CompletedAt) {
			return c, ErrStale
		}
		if current.CompletedAt.IsZero() || current.CompletedAt.After(now) {
			return c, ErrNotEligible
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return c, err
	}
	return c, nil
}

func commitStickyBindingTx(t *sql.Tx, ctx context.Context,
	s StickyBindingCommit, now time.Time) error {
	validationJSON := marshalJSONDefault(s.Binding.Validation)
	res, err := t.ExecContext(ctx, `
		UPDATE sticky_bindings SET
		  version = version + 1,
		  owner = ?,
		  fingerprint = ?,
		  source_id = ?,
		  source_config_version = ?,
		  resource_id = ?,
		  incarnation = ?,
		  connection_ciphertext = ?,
		  validation_json = ?,
		  deadline = ?,
		  state = 'ACTIVE',
		  updated_at = ?
		WHERE project_id = ? AND session_id = ? AND version = ?
		  AND owner = ? AND state = 'CLAIMING'`,
		s.Owner, s.Binding.Fingerprint, s.Binding.SourceID,
		s.Binding.SourceConfigVersion, s.Binding.ResourceID, s.Binding.Incarnation,
		s.Binding.ConnectionCiphertext, validationJSON, fmtTime(s.Binding.Deadline),
		fmtTime(now), s.Binding.ProjectID, s.Binding.SessionID,
		s.ExpectedVersion, s.Owner)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrBindingClaimConflict
	}
	return nil
}

// StickyResourceLive checks the exact inventory object and source version a
// session binding still points at. Ephemeral bindings have no inventory row.
func (s *Store) StickyResourceLive(ctx context.Context, sourceID, resourceID,
	incarnation string, sourceConfigVersion int64, now time.Time) (bool, error) {
	if sourceID == "" || resourceID == "" || incarnation == "" || sourceConfigVersion <= 0 {
		return false, nil
	}
	var enabled int
	var configVersion int64
	var disabled int
	var actualIncarnation string
	err := s.db.QueryRowContext(ctx, `
		SELECT s.enabled, s.config_version, r.disabled, r.incarnation
		FROM proxy_resources r
		JOIN source_offers o ON o.resource_id = r.resource_id AND o.state = 'ENABLED'
		JOIN sources s ON s.source_id = o.source_id
		WHERE s.source_id = ? AND r.resource_id = ? AND r.incarnation = ?`,
		sourceID, resourceID, incarnation).
		Scan(&enabled, &configVersion, &disabled, &actualIncarnation)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return enabled == 1 && disabled == 0 && configVersion == sourceConfigVersion &&
		actualIncarnation == incarnation, nil
}

// StickyReuseRetention returns the current enabled-offer local retention and
// provider expiry bounds for an inventoried binding. Ephemeral bindings have
// no row and return nils; their binding deadline remains the sole bound.
func (s *Store) StickyReuseRetention(ctx context.Context, sourceID, resourceID,
	incarnation string, sourceConfigVersion int64) (retention, providerExpires *time.Time, err error) {
	if sourceID == "" || resourceID == "" || incarnation == "" {
		return nil, nil, nil
	}
	var retentionValue, providerValue any
	err = s.db.QueryRowContext(ctx, `
		SELECT o.local_retention_deadline, o.provider_expires_at
		FROM proxy_resources r
		JOIN source_offers o ON o.resource_id = r.resource_id
		JOIN sources s ON s.source_id = o.source_id
		WHERE s.source_id = ? AND r.resource_id = ? AND r.incarnation = ?
		  AND s.config_version = ? AND s.enabled = 1 AND r.disabled = 0
		  AND o.state = 'ENABLED'
		ORDER BY o.created_at DESC LIMIT 1`,
		sourceID, resourceID, incarnation, sourceConfigVersion).Scan(&retentionValue, &providerValue)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	if retention, err = scanTime(retentionValue); err != nil {
		return nil, nil, err
	}
	if providerExpires, err = scanTime(providerValue); err != nil {
		return nil, nil, err
	}
	return retention, providerExpires, nil
}

// CommitStickyReuse atomically rechecks the current project/key/source,
// object incarnation, binding version, proof freshness, and channel circuit
// before inserting a direct lease. A normal reuse does not change the
// binding; Revalidated replaces only its proof and never extends its TTL.
func (s *Store) CommitStickyReuse(ctx context.Context, a StickyReuseArgs) (time.Time, error) {
	var final time.Time
	err := s.tx(ctx, func(t *sql.Tx) error {
		// The caller may have taken its eligibility snapshot before waiting.
		// Commit guards must use the transaction's fresh clock.
		a.Now = time.Now().UTC()
		var guard stickyReuseGuard
		var deadlineText string
		err := t.QueryRowContext(ctx, `
			SELECT version, state, fingerprint, source_id, source_config_version,
			       resource_id, incarnation, connection_ciphertext, validation_json, deadline
			FROM sticky_bindings
			WHERE project_id = ? AND session_id = ?`,
			a.ProjectID, a.SessionID).
			Scan(&guard.version, &guard.state, &guard.fingerprint, &guard.sourceID,
				&guard.sourceVersion, &guard.resourceID, &guard.incarnation,
				&guard.connection, &guard.validationJSON,
				&deadlineText)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotEligible
		}
		if err != nil {
			return err
		}
		guard.deadline, err = time.Parse(time.RFC3339Nano, deadlineText)
		if err != nil {
			return ErrNotEligible
		}
		if guard.state != BindingActive || guard.version != a.ExpectedVersion ||
			guard.fingerprint != a.Fingerprint || guard.sourceID != a.SourceID ||
			guard.sourceVersion != a.SourceConfigVersion ||
			guard.resourceID != a.ResourceID || guard.incarnation != a.Incarnation ||
			guard.connection != a.BindingCiphertext {
			return ErrNotEligible
		}
		if err := projectKeyGuardsTx(t, ctx, a.ProjectID, a.KeyID, guard.sourceID); err != nil {
			return err
		}
		if err := sourceEligibleTx(t, ctx, guard.sourceID, guard.sourceVersion, a.Now); err != nil {
			return err
		}
		var providerExp, retention *time.Time
		if stickyBindingInventory(guard.incarnation) {
			var disabled int
			var offerState string
			var actualIncarnation string
			var providerExpValue, retentionValue any
			err = t.QueryRowContext(ctx, `
				SELECT r.disabled, r.incarnation, o.state,
				       o.provider_expires_at, o.local_retention_deadline
				FROM proxy_resources r
				JOIN source_offers o ON o.resource_id = r.resource_id
				WHERE r.resource_id = ? AND o.source_id = ? AND o.state = 'ENABLED'
				ORDER BY o.created_at DESC LIMIT 1`,
				guard.resourceID, guard.sourceID).
				Scan(&disabled, &actualIncarnation, &offerState,
					&providerExpValue, &retentionValue)
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotEligible
			}
			if err != nil {
				return err
			}
			if disabled != 0 || offerState != domain.OfferEnabled ||
				actualIncarnation != guard.incarnation {
				return ErrNotEligible
			}
			if providerExp, err = scanTime(providerExpValue); err != nil {
				return err
			}
			if retention, err = scanTime(retentionValue); err != nil {
				return err
			}
		}
		var curOutcome, curJSON string
		declaredScope := domain.EvidenceScopeHash(domain.ValidationScope{
			Mode: a.Proof.Mode, TargetHostPort: a.Proof.Target,
			Profile: a.Proof.Profile, SecurityHash: a.Proof.SecurityHash,
		})
		if a.Proof.Mode != "" && declaredScope != a.ScopeHash {
			return ErrNotEligible
		}
		if guard.scopeHash = a.ScopeHash; guard.scopeHash != "" {
			err = t.QueryRowContext(ctx, `
				SELECT outcome, evidence_json FROM validation_evidence
				WHERE connection_fingerprint = ? AND scope_hash = ?
				  AND source_id = ? AND source_config_version = ? AND incarnation = ?`,
				guard.fingerprint, guard.scopeHash, guard.sourceID,
				guard.sourceVersion, guard.incarnation).
				Scan(&curOutcome, &curJSON)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					// Ephemeral bindings may carry their private proof only in
					// the encrypted binding and must never consult public
					// validation_evidence to authorize delivery.
					if stickyBindingInventory(guard.incarnation) {
						return ErrNotEligible
					}
				} else {
					return err
				}
			}
		}
		if curJSON != "" {
			var current domain.Evidence
			if json.Unmarshal([]byte(curJSON), &current) != nil ||
				curOutcome != "PASS" ||
				current.CompletedAt.IsZero() ||
				current.CompletedAt.After(a.Now) ||
				current.CompletedAt.Before(a.Proof.CompletedAt) ||
				(a.MaxProofAge > 0 && a.Now.Sub(current.CompletedAt) > a.MaxProofAge) {
				return ErrNotEligible
			}
		} else if a.Proof.Outcome != "PASS" || a.Proof.CompletedAt.IsZero() ||
			a.Proof.CompletedAt.After(a.Now) ||
			(a.MaxProofAge > 0 && a.Now.Sub(a.Proof.CompletedAt) > a.MaxProofAge) {
			return ErrNotEligible
		}
		leaseValidationJSON := guard.validationJSON
		if a.Revalidated {
			if a.Proof.Outcome != "PASS" || a.Proof.CompletedAt.IsZero() ||
				a.Proof.CompletedAt.After(a.Now) ||
				(a.MaxProofAge > 0 && a.Now.Sub(a.Proof.CompletedAt) > a.MaxProofAge) {
				return ErrNotEligible
			}
			var previous domain.Evidence
			if guard.validationJSON != "" && guard.validationJSON != "{}" {
				if json.Unmarshal([]byte(guard.validationJSON), &previous) != nil {
					return ErrNotEligible
				}
				if previous.CompletedAt.After(a.Proof.CompletedAt) {
					return ErrStale
				}
			}
			leaseValidationJSON = marshalJSONDefault(a.Proof)
			res, err := t.ExecContext(ctx, `
				UPDATE sticky_bindings SET validation_json = ?, updated_at = ?
				WHERE project_id = ? AND session_id = ? AND version = ?
				  AND state = 'ACTIVE'`,
				leaseValidationJSON, fmtTime(a.Now), a.ProjectID, a.SessionID,
				a.ExpectedVersion)
			if err != nil {
				return err
			}
			if n, err := res.RowsAffected(); err != nil {
				return err
			} else if n != 1 {
				return ErrStale
			}
		}
		if a.ValidateCountry {
			if a.CountryExitIP == "" || a.Proof.ObservedExitIP != a.CountryExitIP {
				return ErrNotEligible
			}
			if stickyBindingInventory(guard.incarnation) {
				var exitIP, countryCode, observedAt string
				err = t.QueryRowContext(ctx, `
					SELECT exit_ip, country_code, observed_at
					FROM resource_observations
					WHERE resource_id = ? AND exit_ip = ?`,
					guard.resourceID, a.CountryExitIP).Scan(&exitIP, &countryCode, &observedAt)
				if errors.Is(err, sql.ErrNoRows) {
					return ErrNotEligible
				}
				if err != nil {
					return err
				}
				matches := false
				for _, want := range a.CountryCodes {
					if strings.EqualFold(want, countryCode) {
						matches = true
						break
					}
				}
				if !matches {
					return ErrNotEligible
				}
			}
		}
		final, err = currentStickyReuseDeadline(a.Now, a.LeaseDeadline, a.MinUsable,
			guard.deadline, providerExp, retention, a.DeliverySafety)
		if err != nil {
			return err
		}
		_, err = t.ExecContext(ctx, `
			INSERT INTO leases(lease_id, tenant, request_id, resource_id, offer_id,
			  context_key, mode, fence, deadline, state, connection_json,
			  validation_json, created_at, consumer_id, key_id)
			VALUES(?,?,?,?,?,'sticky|'||?,?,0,?, 'ACTIVE', ?, ?, ?, ?, ?)`,
			a.LeaseID, "default", a.RequestID, stickyLeaseResourceID(a.ResourceID, guard.fingerprint),
			"", a.SessionID,
			a.Mode, fmtTime(final), a.ConnJSON, leaseValidationJSON,
			fmtTime(a.Now), a.ConsumerID, a.KeyID)
		return err
	})
	if err != nil {
		return time.Time{}, err
	}
	return final, nil
}

func stickyLeaseResourceID(resourceID, fingerprint string) string {
	if resourceID != "" {
		return resourceID
	}
	return fingerprint
}

type StickyReuseArgs struct {
	ProjectID           string
	SessionID           string
	ExpectedVersion     int64
	KeyID               string
	ConsumerID          string
	Fingerprint         string
	SourceID            string
	SourceConfigVersion int64
	ResourceID          string
	Incarnation         string
	ScopeHash           string
	Proof               domain.Evidence
	MaxProofAge         time.Duration
	RequestID           string
	LeaseID             string
	Mode                string
	LeaseDeadline       time.Time
	ConnJSON            string
	Now                 time.Time
	MinUsable           time.Duration
	BindingCiphertext   string
	DeliverySafety      time.Duration
	ValidateCountry     bool
	CountryExitIP       string
	CountryCodes        []string
	// Revalidated means Proof came from a fresh probe of this same binding.
	// Only the persisted proof is refreshed; the binding deadline and CD are
	// left untouched.
	Revalidated bool
}

// SourceCircuitOpen reports whether a source is explicitly cooling. The
// sticky reuse exception does not bypass channel-level circuit state.
func (s *Store) SourceCircuitOpen(ctx context.Context, sourceID string,
	now time.Time) (bool, error) {
	var until any
	err := s.db.QueryRowContext(ctx, `
		SELECT cooldown_until FROM source_circuits WHERE source_id = ?`,
		sourceID).Scan(&until)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	cool, err := scanTime(until)
	if err != nil {
		return false, err
	}
	return cool != nil && cool.After(now), nil
}
