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
			WHERE r.resource_id = ?`,
			cand.Offer.ID, cand.Resource.ID).
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
		contextKey := domain.ContextKey(credVersion, cand.Resource.Session)
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
			INSERT INTO reservations(reservation_id, resource_id, offer_id, context_key, owner, fence, deadline, state, created_at)
			VALUES(?,?,?,?,?,?,?, 'RESERVED', ?)`,
			reservationID, cand.Resource.ID, cand.Offer.ID, contextKey, a.Owner,
			newFence, fmtTime(a.ReservationDeadline), fmtTime(a.Now))
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
			rFence      int64
			offerState  string
			providerExp any
		)
		err := t.QueryRowContext(ctx, `
			SELECT rv.state, rv.fence, rv.deadline, rv.resource_id, rv.offer_id, rv.context_key,
			       r.disabled, r.fence, o.state, o.provider_expires_at
			FROM reservations rv
			JOIN proxy_resources r ON r.resource_id = rv.resource_id
			JOIN source_offers o ON o.offer_id = rv.offer_id
			WHERE rv.reservation_id = ? AND rv.owner = ?`,
			a.ReservationID, a.Owner).
			Scan(&state, &fence, &deadline, &resourceID, &offerID, &contextKey,
				&disabled, &rFence, &offerState, &providerExp)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrStale
		}
		if err != nil {
			return err
		}
		if state != "RESERVED" || fence != a.ExpectedFence || rFence != a.ExpectedFence {
			return ErrStale
		}
		resDeadline, err := time.Parse(time.RFC3339Nano, deadline)
		if err != nil {
			return err
		}
		if !resDeadline.After(a.Now) || disabled != 0 || offerState != domain.OfferEnabled {
			return ErrNotEligible
		}
		if exp, err := scanTime(providerExp); err != nil {
			return err
		} else if exp != nil && exp.Before(a.Deadline) {
			return ErrNotEligible
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
			  mode, fence, deadline, state, connection_json, validation_json, created_at)
			VALUES(?,?,?,?,?,?,?,?,?, 'ACTIVE', ?, ?, ?)`,
			a.LeaseID, a.Tenant, a.RequestID, resourceID, offerID, contextKey,
			a.Mode, a.ExpectedFence, fmtTime(a.Deadline), a.ConnJSON, a.ValidationJSON,
			fmtTime(a.Now)); err != nil {
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
	ConnectionJSON string
	ValidationJSON string
}

func (s *Store) GetLease(ctx context.Context, id string) (LeaseRow, error) {
	var row LeaseRow
	var deadline, created string
	var released any
	err := s.db.QueryRowContext(ctx, `
		SELECT lease_id, tenant, request_id, resource_id, offer_id, context_key, mode,
		       deadline, state, connection_json, validation_json, created_at, released_at
		FROM leases WHERE lease_id = ?`, id).
		Scan(&row.Lease.ID, &row.Lease.Tenant, &row.Lease.RequestID, &row.Lease.ResourceID,
			&row.Lease.OfferID, &row.Lease.ContextKey, &row.Lease.Mode, &deadline,
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
