package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"proxypool/internal/domain"
)

var ErrCapacity = errors.New("allocation capacity exhausted")
var ErrNotEligible = errors.New("resource no longer eligible")
var ErrStale = errors.New("stale allocation attempt")

type Reserved struct {
	ReservationID       string
	Fence               int64
	FailureEpoch        int64
	SourceConfigVersion int64
	Candidate           domain.Candidate
	ContextKey          string
}

type ReserveArgs struct {
	Owner               string
	Profile             string
	Now                 time.Time
	ReservationDeadline time.Time
	ConsumerID          string
	Cooldown            time.Duration
	// ProjectID enables the fixed project-CD recheck inside the reservation
	// transaction; IdentitySecret derives the connection fingerprint for it.
	// Unlike Cooldown, the project-CD recheck ignores the current request's
	// cd parameter: an open window blocks regardless.
	ProjectID      string
	IdentitySecret string
	// KeyID records the authenticating api key so lease ownership stays
	// key-scoped even though the CD subject is the project.
	KeyID string
}

// consumerBlockedTx reports whether the CD subject already holds this exact
// connection in flight. The fixed delivery CD lives in project_proxy_cd and
// is checked separately, so the frozen legacy table is no longer consulted.
func consumerBlockedTx(t *sql.Tx, a ReserveArgs, resourceID string, maxLeases int) (bool, error) {
	if a.ConsumerID == "" {
		return false, nil
	}
	var held int
	if err := t.QueryRowContext(context.Background(), `
		SELECT
		  (SELECT COUNT(*) FROM reservations
		   WHERE consumer_id = ? AND resource_id = ?
		     AND state IN ('RESERVED','VALIDATING') AND deadline > ?)
		+ (SELECT COUNT(*) FROM leases
		   WHERE consumer_id = ? AND resource_id = ?
		     AND state = 'ACTIVE' AND deadline > ?)`,
		a.ConsumerID, resourceID, fmtTime(a.Now),
		a.ConsumerID, resourceID, fmtTime(a.Now)).Scan(&held); err != nil {
		return false, err
	}
	if maxLeases <= 0 {
		maxLeases = 1
	}
	// With more than one capacity slot, reservations are protected by the
	// project's fixed CD and the resource capacity check, not by an old
	// consumer-exclusive rule. cd=0 in-flight work must not become a
	// permanent project window.
	if maxLeases > 1 && a.Cooldown <= 0 {
		return false, nil
	}
	return held >= maxLeases, nil
}

// ReserveSlot atomically claims the exclusive allocation slot for a candidate.
// Eligibility is rechecked inside the same immediate transaction.
func (s *Store) ReserveSlot(ctx context.Context, cand domain.Candidate, a ReserveArgs) (Reserved, error) {
	if cand.Ephemeral {
		return s.reserveEphemeral(ctx, cand, a)
	}
	var out Reserved
	err := s.tx(ctx, func(t *sql.Tx) error {
		out = Reserved{}
		var (
			disabled    int
			fence       int64
			offerState  string
			providerExp any
			retention   any
			maxLeases   int
			credVersion string
			session     string
			port        int
			srcVersion  int64
		)
		err := t.QueryRowContext(ctx, `
			SELECT r.disabled, r.fence, r.transport, r.host, r.port, r.credential_version,
			       r.session_policy, r.resource_kind, COALESCE(cr.username, ''), COALESCE(cr.password, ''),
			       o.offer_id, o.state, o.provider_expires_at, o.local_retention_deadline, o.max_pool_leases,
			       src.config_version
			FROM proxy_resources r
			LEFT JOIN proxy_credentials cr ON cr.credential_version = r.credential_version
			JOIN source_offers o ON o.resource_id = r.resource_id AND o.offer_id = ?
			JOIN sources src ON src.source_id = o.source_id AND src.enabled = 1
			LEFT JOIN source_circuits sc ON sc.source_id = o.source_id
			WHERE r.resource_id = ? AND o.state = 'ENABLED'
			  AND (o.provider_expires_at IS NULL OR o.provider_expires_at > ?)
			  AND (o.local_retention_deadline IS NULL OR o.local_retention_deadline > ?)
			  AND (sc.cooldown_until IS NULL OR sc.cooldown_until <= ?)`,
			cand.Offer.ID, cand.Resource.ID,
			fmtTime(a.Now), fmtTime(a.Now), fmtTime(a.Now)).
			Scan(&disabled, &fence, &cand.Resource.Conn.Transport, &cand.Resource.Conn.Host,
				&port, &credVersion, &session, &cand.Resource.Kind,
				&cand.Resource.Conn.Username, &cand.Resource.Conn.Password,
				&cand.Offer.ID, &offerState, &providerExp, &retention, &maxLeases, &srcVersion)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotEligible
		}
		if err != nil {
			return err
		}
		cand.Resource.Conn.Port = port
		cand.Resource.CredentialVersion = credVersion
		cand.Resource.Session = domain.SessionMode(session)
		contextKey := domain.ContextKey(credVersion, cand.Resource.Session)
		if disabled != 0 || offerState != domain.OfferEnabled {
			return ErrNotEligible
		}
		if err := projectKeyGuardsTx(t, ctx, a.ProjectID, a.KeyID, cand.Offer.SourceID); err != nil {
			return err
		}
		if exp, err := scanTime(providerExp); err != nil {
			return err
		} else if exp != nil && !exp.After(a.Now) {
			return ErrNotEligible
		}
		if ret, err := scanTime(retention); err != nil {
			return err
		} else if ret != nil && !ret.After(a.Now) {
			return ErrNotEligible
		}
		// Recheck the scoped-health cooldown inside the reservation
		// transaction: cached candidates may predate a newer failure.
		var healthCooldown any
		err = t.QueryRowContext(ctx, `
				SELECT cooldown_until FROM scoped_health
				WHERE resource_id = ? AND context_key = ? AND profile = ?`,
			cand.Resource.ID, contextKey, a.Profile).Scan(&healthCooldown)
		if err == nil && healthCooldown != nil {
			cool, serr := scanTime(healthCooldown)
			if serr != nil {
				return serr
			}
			if cool != nil && cool.After(a.Now) {
				return ErrNotEligible
			}
		} else if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var active int
		err = t.QueryRowContext(ctx, `
			SELECT
			  (SELECT COUNT(*) FROM leases WHERE resource_id = ? AND state = 'ACTIVE' AND deadline > ?)
			+ (SELECT COUNT(*) FROM reservations WHERE resource_id = ?
			   AND state IN ('RESERVED','VALIDATING') AND deadline > ?)`,
			cand.Resource.ID, fmtTime(a.Now), cand.Resource.ID, fmtTime(a.Now)).Scan(&active)
		if err != nil {
			return err
		}
		if active >= maxLeases {
			return ErrCapacity
		}
		if blocked, err := consumerBlockedTx(t, a, cand.Resource.ID, maxLeases); err != nil {
			return err
		} else if blocked {
			return ErrNotEligible
		}
		if a.ProjectID != "" {
			fp := domain.ConnectionFingerprint(a.IdentitySecret, cand.Resource.Conn.Transport,
				cand.Resource.Conn.Host, cand.Resource.Conn.Port,
				cand.Resource.Conn.Username, cand.Resource.Conn.Password)
			if blocked, err := projectBlockedTx(t, ctx, a.ProjectID, fp, a.Now); err != nil {
				return err
			} else if blocked {
				return ErrNotEligible
			}
		}
		var epoch int64
		err = t.QueryRowContext(ctx, `
			SELECT COALESCE(failure_epoch, 0) FROM scoped_health
			WHERE resource_id = ? AND context_key = ? AND profile = ?`,
			cand.Resource.ID, contextKey, a.Profile).Scan(&epoch)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		newFence := fence + 1
		res, err := t.ExecContext(ctx,
			"UPDATE proxy_resources SET fence = ? WHERE resource_id = ? AND fence = ?",
			newFence, cand.Resource.ID, fence)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return ErrStale
		}
		reservationID := domain.NewID("res_")
		_, err = t.ExecContext(ctx, `
			INSERT INTO reservations(reservation_id, resource_id, offer_id, context_key, owner, fence, deadline, state, created_at, consumer_id, source_config_version, key_id)
			VALUES(?,?,?,?,?,?,?, 'RESERVED', ?, ?, ?, ?)`,
			reservationID, cand.Resource.ID, cand.Offer.ID, contextKey, a.Owner,
			newFence, fmtTime(a.ReservationDeadline), fmtTime(a.Now), a.ConsumerID, srcVersion, a.KeyID)
		if err != nil {
			return err
		}
		out = Reserved{
			ReservationID:       reservationID,
			Fence:               newFence,
			FailureEpoch:        epoch,
			SourceConfigVersion: srcVersion,
			Candidate:           cand,
			ContextKey:          contextKey,
		}
		return nil
	})
	if err != nil {
		return Reserved{}, err
	}
	return out, nil
}

// reserveEphemeral claims a slot for a request-local on-demand candidate.
// There are no inventory rows: eligibility keys on the deterministic
// connection fingerprint, the source's live state, and the consumer CD.
func (s *Store) reserveEphemeral(ctx context.Context, cand domain.Candidate, a ReserveArgs) (Reserved, error) {
	var out Reserved
	err := s.tx(ctx, func(t *sql.Tx) error {
		out = Reserved{}
		maxLeases := cand.Offer.MaxPoolLeases
		if maxLeases <= 0 {
			maxLeases = 1
		}
		var srcEnabled int
		var srcVersion int64
		var circuitCool any
		err := t.QueryRowContext(ctx, `
			SELECT src.enabled, src.config_version, sc.cooldown_until
			FROM sources src
			LEFT JOIN source_circuits sc ON sc.source_id = src.source_id
			WHERE src.source_id = ?`, cand.Offer.SourceID).
			Scan(&srcEnabled, &srcVersion, &circuitCool)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotEligible
		}
		if err != nil {
			return err
		}
		if srcEnabled != 1 {
			return ErrNotEligible
		}
		if err := projectKeyGuardsTx(t, ctx, a.ProjectID, a.KeyID, cand.Offer.SourceID); err != nil {
			return err
		}
		if circuitCool != nil {
			cool, serr := scanTime(circuitCool)
			if serr != nil {
				return serr
			}
			if cool != nil && cool.After(a.Now) {
				return ErrNotEligible
			}
		}
		if blocked, err := consumerBlockedTx(t, a, cand.Resource.ID, maxLeases); err != nil {
			return err
		} else if blocked {
			return ErrNotEligible
		}
		if a.ProjectID != "" {
			fp := domain.ConnectionFingerprint(a.IdentitySecret, cand.Resource.Conn.Transport,
				cand.Resource.Conn.Host, cand.Resource.Conn.Port,
				cand.Resource.Conn.Username, cand.Resource.Conn.Password)
			if blocked, err := projectBlockedTx(t, ctx, a.ProjectID, fp, a.Now); err != nil {
				return err
			} else if blocked {
				return ErrNotEligible
			}
		}
		contextKey := domain.ContextKey(cand.Resource.CredentialVersion, cand.Resource.Session)
		var active int
		err = t.QueryRowContext(ctx, `
			SELECT
			  (SELECT COUNT(*) FROM leases WHERE resource_id = ? AND state = 'ACTIVE' AND deadline > ?)
			+ (SELECT COUNT(*) FROM reservations WHERE resource_id = ?
			   AND state IN ('RESERVED','VALIDATING') AND deadline > ?)`,
			cand.Resource.ID, fmtTime(a.Now), cand.Resource.ID, fmtTime(a.Now)).Scan(&active)
		if err != nil {
			return err
		}
		if active >= maxLeases {
			return ErrCapacity
		}
		reservationID := domain.NewID("res_")
		if _, err := t.ExecContext(ctx, `
			INSERT INTO reservations(reservation_id, resource_id, offer_id, context_key, owner, fence, deadline, state, created_at, consumer_id, source_config_version, key_id)
			VALUES(?,?,?,?,?,0,?, 'RESERVED', ?, ?, ?, ?)`,
			reservationID, cand.Resource.ID, cand.Offer.ID, contextKey, a.Owner,
			fmtTime(a.ReservationDeadline), fmtTime(a.Now), a.ConsumerID, srcVersion, a.KeyID); err != nil {
			return err
		}
		out = Reserved{
			ReservationID:       reservationID,
			Fence:               0,
			SourceConfigVersion: srcVersion,
			Candidate:           cand,
			ContextKey:          contextKey,
		}
		return nil
	})
	if err != nil {
		return Reserved{}, err
	}
	return out, nil
}

func (s *Store) ReleaseReservation(ctx context.Context, reservationID string, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE reservations SET state = 'RELEASED'
		WHERE reservation_id = ? AND state = 'RESERVED'`, reservationID)
	if err != nil {
		return mapErr(err)
	}
	return nil
}

// ReleaseOwnedReservations releases only reservations still owned by the
// request. The bound keeps post-cancel cleanup from touching a newer owner.
func (s *Store) ReleaseOwnedReservations(ctx context.Context, owner string,
	reservationIDs []string) error {
	if owner == "" || len(reservationIDs) == 0 {
		return nil
	}
	return s.tx(ctx, func(t *sql.Tx) error {
		for _, reservationID := range reservationIDs {
			if reservationID == "" {
				continue
			}
			if _, err := t.ExecContext(ctx, `
				UPDATE reservations SET state = 'RELEASED'
				WHERE reservation_id = ? AND owner = ? AND state = 'RESERVED'`,
				reservationID, owner); err != nil {
				return err
			}
		}
		return nil
	})
}

type CommitArgs struct {
	ReservationID  string
	Owner          string
	LeaseID        string
	RequestID      string
	Tenant         string
	Mode           string
	Deadline       time.Time
	ConnJSON       string
	ValidationJSON string
	ExpectedFence  int64
	ExpectedEpoch  int64
	Profile        string
	ConsumerID     string
	// ProjectID/KeyID/Fingerprint recheck project state, credential state,
	// and the fixed CD window at commit time; SourceID and
	// ExpectedSourceConfigVersion catch source edits between reserve and
	// commit (the reservation row carries the version it reserved under).
	ProjectID                   string
	KeyID                       string
	Fingerprint                 string
	SourceID                    string
	ExpectedSourceConfigVersion int64
	// Ephemeral commits key on the reservation's connection fingerprint; no
	// inventory lookup, capacity recheck uses MaxPoolLeases.
	Ephemeral bool
	// DeliveryCooldown opens the project's fixed CD window inside the same
	// transaction as the lease insert: delivery and CD are atomic, and only
	// committed deliveries start cooldown.
	DeliveryCooldown time.Duration
	MaxPoolLeases    int
	Now              time.Time
	// IdentitySecret is required only by final dispatch, which recomputes the
	// canonical connection fingerprint inside the commit transaction.
	IdentitySecret string
	// DeliverySafety bounds supplier-expiry safety during the final commit.
	DeliverySafety time.Duration
	// MaxProofAge rejects a prepared validation proof that aged while a slow
	// sibling candidate was still probing.
	MaxProofAge time.Duration
	// Items carries the request's selected response set; final delivery uses
	// the same reservation rows to avoid a second insert path.
	Items []DeliveryCandidate
}

// commitProjectGuards rechecks, inside the commit transaction, that the
// project and the authenticating key are still enabled, the source is inside
// the project allowlist, and the fingerprint is not inside a live CD window.
func commitProjectGuards(t *sql.Tx, ctx context.Context, a CommitArgs, sourceID string) error {
	if err := projectKeyGuardsTx(t, ctx, a.ProjectID, a.KeyID, sourceID); err != nil {
		return err
	}
	if a.ProjectID != "" && a.Fingerprint != "" {
		if blocked, err := projectBlockedTx(t, ctx, a.ProjectID, a.Fingerprint, a.Now); err != nil {
			return err
		} else if blocked {
			return ErrNotEligible
		}
	}
	return nil
}

// projectKeyGuardsTx rechecks inside one transaction that the project is
// still enabled, the source is still inside the project allowlist (nil
// allowlist = unrestricted, empty = nothing), and the authenticating key is
// still enabled. Reserve and commit both call it, so a project disable,
// allowlist edit, or key rotation between the two phases is caught.
func projectKeyGuardsTx(t *sql.Tx, ctx context.Context, projectID, keyID, sourceID string) error {
	if projectID != "" {
		var enabled int
		err := t.QueryRowContext(ctx,
			`SELECT enabled FROM projects WHERE project_id = ?`, projectID).Scan(&enabled)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			// Implicit CD scopes (no projects row) carry no configuration to
			// enforce; the API layer fails closed before reaching the store.
		case err != nil:
			return err
		case enabled != 1:
			return ErrNotEligible
		default:
			var allowed any
			if err := t.QueryRowContext(ctx,
				`SELECT allowed_source_ids FROM projects WHERE project_id = ?`, projectID).
				Scan(&allowed); err != nil {
				return err
			}
			if allowed != nil {
				raw, ok := allowed.(string)
				if !ok {
					return fmt.Errorf("allowed_source_ids is %T", allowed)
				}
				var ids []string
				if err := json.Unmarshal([]byte(raw), &ids); err != nil {
					return err
				}
				found := false
				for _, id := range ids {
					if id == sourceID {
						found = true
						break
					}
				}
				if !found {
					return ErrNotEligible
				}
			}
		}
	}
	if keyID != "" {
		var enabled int
		err := t.QueryRowContext(ctx,
			`SELECT enabled FROM api_keys WHERE key_id = ?`, keyID).Scan(&enabled)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotEligible
		}
		if err != nil {
			return err
		}
		if enabled != 1 {
			return ErrNotEligible
		}
	}
	return nil
}

// sourceVersionStaleTx compares the source's live config version with the
// one the reservation captured; a changed source invalidates the attempt.
func sourceVersionStaleTx(t *sql.Tx, ctx context.Context, sourceID string, expected int64) (bool, error) {
	if sourceID == "" {
		return false, nil
	}
	var version int64
	err := t.QueryRowContext(ctx,
		`SELECT config_version FROM sources WHERE source_id = ?`, sourceID).Scan(&version)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return version != expected, nil
}

// CommitLease converts a reservation into an active lease. All eligibility
// guards (fence, epoch, expiry, disable state) are rechecked transactionally.
func (s *Store) CommitLease(ctx context.Context, a CommitArgs) error {
	if a.Ephemeral {
		return s.commitEphemeral(ctx, a)
	}
	return s.tx(ctx, func(t *sql.Tx) error {
		var (
			state       string
			fence       int64
			deadline    string
			resourceID  string
			offerID     string
			contextKey  string
			disabled    int
			offerState  string
			providerExp any
			maxLeases   int
			srcEnabled  int
			circuitCool any
			srcVersion  int64
			resVersion  int64
			sourceID    string
		)
		err := t.QueryRowContext(ctx, `
			SELECT rv.state, rv.fence, rv.deadline, rv.resource_id, rv.offer_id, rv.context_key,
			       r.disabled, o.state, o.provider_expires_at, o.max_pool_leases,
			       src.enabled, sc.cooldown_until, src.config_version, rv.source_config_version,
			       src.source_id
			FROM reservations rv
			JOIN proxy_resources r ON r.resource_id = rv.resource_id
			JOIN source_offers o ON o.offer_id = rv.offer_id
			JOIN sources src ON src.source_id = o.source_id
			LEFT JOIN source_circuits sc ON sc.source_id = o.source_id
			WHERE rv.reservation_id = ? AND rv.owner = ?`,
			a.ReservationID, a.Owner).
			Scan(&state, &fence, &deadline, &resourceID, &offerID, &contextKey,
				&disabled, &offerState, &providerExp, &maxLeases, &srcEnabled, &circuitCool,
				&srcVersion, &resVersion, &sourceID)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrStale
		}
		if err != nil {
			return err
		}
		if state != "RESERVED" || fence != a.ExpectedFence {
			return ErrStale
		}
		resDeadline, err := time.Parse(time.RFC3339Nano, deadline)
		if err != nil {
			return err
		}
		if !resDeadline.After(a.Now) || disabled != 0 || offerState != domain.OfferEnabled {
			return ErrNotEligible
		}
		if srcEnabled != 1 {
			return ErrNotEligible
		}
		if circuitCool != nil {
			cool, serr := scanTime(circuitCool)
			if serr != nil {
				return serr
			}
			if cool != nil && cool.After(a.Now) {
				return ErrNotEligible
			}
		}
		if exp, err := scanTime(providerExp); err != nil {
			return err
		} else if exp != nil && exp.Before(a.Deadline) {
			return ErrNotEligible
		}
		if err := commitProjectGuards(t, ctx, a, sourceID); err != nil {
			return err
		}
		if srcVersion != resVersion {
			return ErrStale
		}
		// Capacity is rechecked here; the resource-level fence comparison
		// wrongly invalidated concurrent reservations whenever
		// max_pool_leases allowed more than one.
		var active int
		err = t.QueryRowContext(ctx, `
			SELECT
			  (SELECT COUNT(*) FROM leases
			   WHERE resource_id = ? AND state = 'ACTIVE' AND deadline > ?)
			+ (SELECT COUNT(*) FROM reservations
			   WHERE resource_id = ? AND reservation_id != ?
			     AND state IN ('RESERVED','VALIDATING') AND deadline > ?)`,
			resourceID, fmtTime(a.Now), resourceID, a.ReservationID, fmtTime(a.Now)).Scan(&active)
		if err != nil {
			return err
		}
		if active >= maxLeases {
			return ErrCapacity
		}
		var epoch int64
		err = t.QueryRowContext(ctx, `
			SELECT COALESCE(failure_epoch, 0) FROM scoped_health
			WHERE resource_id = ? AND context_key = ? AND profile = ?`,
			resourceID, contextKey, a.Profile).Scan(&epoch)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if epoch != a.ExpectedEpoch {
			return ErrStale
		}
		if _, err := t.ExecContext(ctx, `
			INSERT INTO leases(lease_id, tenant, request_id, resource_id, offer_id, context_key,
			  mode, fence, deadline, state, connection_json, validation_json, created_at, consumer_id, key_id)
			VALUES(?,?,?,?,?,?,?,?,?, 'ACTIVE', ?, ?, ?, ?, ?)`,
			a.LeaseID, a.Tenant, a.RequestID, resourceID, offerID, contextKey,
			a.Mode, a.ExpectedFence, fmtTime(a.Deadline), a.ConnJSON, a.ValidationJSON,
			fmtTime(a.Now), a.ConsumerID, a.KeyID); err != nil {
			return err
		}
		if err := openDeliveryCDTx(t, ctx, a); err != nil {
			return err
		}
		res, err := t.ExecContext(ctx, `
			UPDATE reservations SET state = 'COMMITTED'
			WHERE reservation_id = ? AND state = 'RESERVED'`, a.ReservationID)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return ErrStale
		}
		return nil
	})
}

func (s *Store) commitEphemeral(ctx context.Context, a CommitArgs) error {
	return s.tx(ctx, func(t *sql.Tx) error {
		var state string
		var fence int64
		var deadline string
		var resourceID string
		var contextKey string
		var resVersion int64
		err := t.QueryRowContext(ctx, `
			SELECT state, fence, deadline, resource_id, context_key, source_config_version
			FROM reservations WHERE reservation_id = ? AND owner = ?`,
			a.ReservationID, a.Owner).
			Scan(&state, &fence, &deadline, &resourceID, &contextKey, &resVersion)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrStale
		}
		if err != nil {
			return err
		}
		if state != "RESERVED" || fence != a.ExpectedFence {
			return ErrStale
		}
		resDeadline, err := time.Parse(time.RFC3339Nano, deadline)
		if err != nil {
			return err
		}
		if !resDeadline.After(a.Now) {
			return ErrNotEligible
		}
		if stale, err := sourceVersionStaleTx(t, ctx, a.SourceID, resVersion); err != nil {
			return err
		} else if stale {
			return ErrStale
		}
		if err := commitProjectGuards(t, ctx, a, a.SourceID); err != nil {
			return err
		}
		maxLeases := a.MaxPoolLeases
		if maxLeases <= 0 {
			maxLeases = 1
		}
		var active int
		err = t.QueryRowContext(ctx, `
			SELECT
			  (SELECT COUNT(*) FROM leases
			   WHERE resource_id = ? AND state = 'ACTIVE' AND deadline > ?)
			+ (SELECT COUNT(*) FROM reservations
			   WHERE resource_id = ? AND reservation_id != ?
			     AND state IN ('RESERVED','VALIDATING') AND deadline > ?)`,
			resourceID, fmtTime(a.Now), resourceID, a.ReservationID, fmtTime(a.Now)).Scan(&active)
		if err != nil {
			return err
		}
		if active >= maxLeases {
			return ErrCapacity
		}
		if _, err := t.ExecContext(ctx, `
			INSERT INTO leases(lease_id, tenant, request_id, resource_id, offer_id, context_key,
			  mode, fence, deadline, state, connection_json, validation_json, created_at, consumer_id, key_id)
			VALUES(?,?,?,?,?,?,?,?,?, 'ACTIVE', ?, ?, ?, ?, ?)`,
			a.LeaseID, a.Tenant, a.RequestID, resourceID, "", contextKey, a.Mode,
			a.ExpectedFence, fmtTime(a.Deadline), a.ConnJSON, a.ValidationJSON,
			fmtTime(a.Now), a.ConsumerID, a.KeyID); err != nil {
			return err
		}
		if err := openDeliveryCDTx(t, ctx, a); err != nil {
			return err
		}
		res, err := t.ExecContext(ctx, `
			UPDATE reservations SET state = 'COMMITTED'
			WHERE reservation_id = ? AND state = 'RESERVED'`, a.ReservationID)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return ErrStale
		}
		return nil
	})
}

type LeaseRow struct {
	Lease          domain.Lease
	ConsumerID     string
	KeyID          string
	ConnectionJSON string
	ValidationJSON string
}

// DirectLeaseArgs inserts a lease without a reservation, used by sticky
// binding reuse where the connection identity comes from the binding.
type DirectLeaseArgs struct {
	LeaseID        string
	RequestID      string
	ConsumerID     string
	KeyID          string
	ResourceID     string
	ContextKey     string
	Mode           string
	Deadline       time.Time
	ConnJSON       string
	ValidationJSON string
	Now            time.Time
}

func (s *Store) InsertDirectLease(ctx context.Context, a DirectLeaseArgs) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO leases(lease_id, tenant, request_id, resource_id, offer_id, context_key,
		  mode, fence, deadline, state, connection_json, validation_json, created_at, consumer_id, key_id)
		VALUES(?, 'default', ?, ?, '', ?, ?, 0, ?, 'ACTIVE', ?, ?, ?, ?, ?)`,
		a.LeaseID, a.RequestID, a.ResourceID, a.ContextKey, a.Mode,
		fmtTime(a.Deadline), a.ConnJSON, a.ValidationJSON, fmtTime(a.Now),
		a.ConsumerID, a.KeyID)
	return mapErr(err)
}

func (s *Store) GetLease(ctx context.Context, id string) (LeaseRow, error) {
	var row LeaseRow
	var deadline, created string
	var released any
	err := s.db.QueryRowContext(ctx, `
		SELECT lease_id, tenant, request_id, resource_id, offer_id, context_key, mode, consumer_id, key_id,
		       deadline, state, connection_json, validation_json, created_at, released_at
		FROM leases WHERE lease_id = ?`, id).
		Scan(&row.Lease.ID, &row.Lease.Tenant, &row.Lease.RequestID, &row.Lease.ResourceID,
			&row.Lease.OfferID, &row.Lease.ContextKey, &row.Lease.Mode, &row.ConsumerID, &row.KeyID, &deadline,
			&row.Lease.State, &row.ConnectionJSON, &row.ValidationJSON, &created, &released)
	if errors.Is(err, sql.ErrNoRows) {
		return LeaseRow{}, domain.NotFound("lease")
	}
	if err != nil {
		return LeaseRow{}, mapErr(err)
	}
	if row.Lease.Deadline, err = time.Parse(time.RFC3339Nano, deadline); err != nil {
		return LeaseRow{}, err
	}
	if row.Lease.CreatedAt, err = time.Parse(time.RFC3339Nano, created); err != nil {
		return LeaseRow{}, err
	}
	if released != nil {
		rel, err := time.Parse(time.RFC3339Nano, released.(string))
		if err != nil {
			return LeaseRow{}, err
		}
		row.Lease.ReleasedAt = &rel
	}
	return row, nil
}

// SetLeaseState transitions an ACTIVE lease to the given terminal state.
func (s *Store) SetLeaseState(ctx context.Context, id, state string, now time.Time) (bool, error) {
	res, err := s.db.ExecContext(ctx, `
		UPDATE leases SET state = ?, released_at = ?
		WHERE lease_id = ? AND state = 'ACTIVE'`, state, fmtTime(now), id)
	if err != nil {
		return false, mapErr(err)
	}
	n, err := res.RowsAffected()
	return n == 1, mapErr(err)
}
