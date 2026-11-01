package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"proxypool/internal/domain"
)

var ErrCapacity = errors.New("allocation capacity exhausted")
var ErrNotEligible = errors.New("resource no longer eligible")
var ErrStale = errors.New("stale allocation attempt")

type Reserved struct {
	ReservationID string
	Fence         int64
	FailureEpoch  int64
	Candidate     domain.Candidate
	ContextKey    string
}

type ReserveArgs struct {
	Owner               string
	Profile             string
	Now                 time.Time
	ReservationDeadline time.Time
	ConsumerID          string
	Cooldown            time.Duration
}

// ReserveSlot atomically claims the exclusive allocation slot for a candidate.
// Eligibility is rechecked inside the same immediate transaction.
func (s *Store) ReserveSlot(ctx context.Context, cand domain.Candidate, a ReserveArgs) (Reserved, error) {
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
		)
		err := t.QueryRowContext(ctx, `
			SELECT r.disabled, r.fence, r.transport, r.host, r.port, r.credential_version,
			       r.session_policy, r.resource_kind, COALESCE(cr.username, ''), COALESCE(cr.password, ''),
			       o.offer_id, o.state, o.provider_expires_at, o.local_retention_deadline, o.max_pool_leases
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
				&cand.Offer.ID, &offerState, &providerExp, &retention, &maxLeases)
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
		if a.ConsumerID != "" && a.Cooldown > 0 {
			var recent int
			if err := t.QueryRowContext(ctx, `
				SELECT COUNT(*) FROM consumer_proxy_cooldowns
				WHERE consumer_id = ? AND resource_id = ? AND last_delivered_at > ?`,
				a.ConsumerID, cand.Resource.ID, fmtTime(a.Now.Add(-a.Cooldown))).Scan(&recent); err != nil {
				return err
			}
			if recent > 0 {
				return ErrNotEligible
			}
			var inFlight int
			if err := t.QueryRowContext(ctx, `
				SELECT COUNT(*) FROM reservations
				WHERE consumer_id = ? AND resource_id = ?
				  AND state IN ('RESERVED','VALIDATING') AND deadline > ?`,
				a.ConsumerID, cand.Resource.ID, fmtTime(a.Now)).Scan(&inFlight); err != nil {
				return err
			}
			if inFlight > 0 {
				return ErrNotEligible
			}
			var activeForConsumer int
			if err := t.QueryRowContext(ctx, `
				SELECT COUNT(*) FROM leases
				WHERE consumer_id = ? AND resource_id = ?
				  AND state = 'ACTIVE' AND deadline > ?`,
				a.ConsumerID, cand.Resource.ID, fmtTime(a.Now)).Scan(&activeForConsumer); err != nil {
				return err
			}
			if activeForConsumer > 0 {
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
			INSERT INTO reservations(reservation_id, resource_id, offer_id, context_key, owner, fence, deadline, state, created_at, consumer_id)
			VALUES(?,?,?,?,?,?,?, 'RESERVED', ?, ?)`,
			reservationID, cand.Resource.ID, cand.Offer.ID, contextKey, a.Owner,
			newFence, fmtTime(a.ReservationDeadline), fmtTime(a.Now), a.ConsumerID)
		if err != nil {
			return err
		}
		out = Reserved{
			ReservationID: reservationID,
			Fence:         newFence,
			FailureEpoch:  epoch,
			Candidate:     cand,
			ContextKey:    contextKey,
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
	Now            time.Time
}

// CommitLease converts a reservation into an active lease. All eligibility
// guards (fence, epoch, expiry, disable state) are rechecked transactionally.
func (s *Store) CommitLease(ctx context.Context, a CommitArgs) error {
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
		)
		err := t.QueryRowContext(ctx, `
			SELECT rv.state, rv.fence, rv.deadline, rv.resource_id, rv.offer_id, rv.context_key,
			       r.disabled, o.state, o.provider_expires_at, o.max_pool_leases,
			       src.enabled, sc.cooldown_until
			FROM reservations rv
			JOIN proxy_resources r ON r.resource_id = rv.resource_id
			JOIN source_offers o ON o.offer_id = rv.offer_id
			JOIN sources src ON src.source_id = o.source_id
			LEFT JOIN source_circuits sc ON sc.source_id = o.source_id
			WHERE rv.reservation_id = ? AND rv.owner = ?`,
			a.ReservationID, a.Owner).
			Scan(&state, &fence, &deadline, &resourceID, &offerID, &contextKey,
				&disabled, &offerState, &providerExp, &maxLeases, &srcEnabled, &circuitCool)
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
			  mode, fence, deadline, state, connection_json, validation_json, created_at, consumer_id)
			VALUES(?,?,?,?,?,?,?,?,?, 'ACTIVE', ?, ?, ?, ?)`,
			a.LeaseID, a.Tenant, a.RequestID, resourceID, offerID, contextKey,
			a.Mode, a.ExpectedFence, fmtTime(a.Deadline), a.ConnJSON, a.ValidationJSON,
			fmtTime(a.Now), a.ConsumerID); err != nil {
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
	ConnectionJSON string
	ValidationJSON string
}

func (s *Store) GetLease(ctx context.Context, id string) (LeaseRow, error) {
	var row LeaseRow
	var deadline, created string
	var released any
	err := s.db.QueryRowContext(ctx, `
		SELECT lease_id, tenant, request_id, resource_id, offer_id, context_key, mode, consumer_id,
		       deadline, state, connection_json, validation_json, created_at, released_at
		FROM leases WHERE lease_id = ?`, id).
		Scan(&row.Lease.ID, &row.Lease.Tenant, &row.Lease.RequestID, &row.Lease.ResourceID,
			&row.Lease.OfferID, &row.Lease.ContextKey, &row.Lease.Mode, &row.ConsumerID, &deadline,
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
