package sqlite

import (
	"context"
	"database/sql"
	"time"

	"proxypool/internal/domain"
)

func (s *Store) RecordAttempt(ctx context.Context, ev domain.Evidence, resourceID, contextKey, detail string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO validation_attempts(attempt_id, request_id, resource_id, context_key, profile,
		  outcome, started_at, completed_at, latency_ms, detail_json)
		VALUES(?,?,?,?,?,?,?,?,?,?)`,
		ev.AttemptID, ev.RequestID, resourceID, contextKey, ev.Profile,
		ev.Outcome, fmtTime(ev.StartedAt), fmtTime(ev.CompletedAt), ev.LatencyMS, detail)
	return mapErr(err)
}

type HealthUpdate struct {
	ResourceID    string
	ContextKey    string
	Profile       string
	Outcome       string // PASS or FAIL
	CooldownUntil *time.Time
	Now           time.Time
}

type HealthRow struct {
	ConsecutiveFailures int
	CooldownUntil       *time.Time
	LastOutcome         string
	LastPassAt          *time.Time
	FailureEpoch        int64
}

func (s *Store) CurrentHealth(ctx context.Context, resourceID, contextKey, profile string) (HealthRow, error) {
	var row HealthRow
	var cooldown, lastPass any
	err := s.db.QueryRowContext(ctx, `
		SELECT consecutive_failures, cooldown_until, last_outcome, last_pass_at, failure_epoch
		FROM scoped_health WHERE resource_id = ? AND context_key = ? AND profile = ?`,
		resourceID, contextKey, profile).
		Scan(&row.ConsecutiveFailures, &cooldown, &row.LastOutcome, &lastPass, &row.FailureEpoch)
	if err == sql.ErrNoRows {
		return HealthRow{LastOutcome: "UNKNOWN"}, nil
	}
	if err != nil {
		return row, mapErr(err)
	}
	if row.CooldownUntil, err = scanTime(cooldown); err != nil {
		return row, err
	}
	if row.LastPassAt, err = scanTime(lastPass); err != nil {
		return row, err
	}
	return row, nil
}

func (s *Store) UpdateHealth(ctx context.Context, u HealthUpdate) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO scoped_health(resource_id, context_key, profile, consecutive_failures,
		  cooldown_until, last_outcome, last_pass_at, failure_epoch, updated_at)
		VALUES(?,?,?,
		  CASE WHEN ? = 'PASS' THEN 0 ELSE 1 END,
		  ?, ?, CASE WHEN ? = 'PASS' THEN ? ELSE NULL END,
		  CASE WHEN ? = 'PASS' THEN 0 ELSE 1 END, ?)
		ON CONFLICT(resource_id, context_key, profile) DO UPDATE SET
		  consecutive_failures = CASE WHEN excluded.last_outcome = 'PASS'
		    THEN 0 ELSE scoped_health.consecutive_failures + 1 END,
		  cooldown_until = excluded.cooldown_until,
		  last_outcome = excluded.last_outcome,
		  last_pass_at = CASE WHEN excluded.last_outcome = 'PASS'
		    THEN excluded.last_pass_at ELSE scoped_health.last_pass_at END,
		  failure_epoch = CASE WHEN excluded.last_outcome = 'PASS'
		    THEN scoped_health.failure_epoch ELSE scoped_health.failure_epoch + 1 END,
		  updated_at = excluded.updated_at`,
		u.ResourceID, u.ContextKey, u.Profile, u.Outcome, nullTime(u.CooldownUntil),
		u.Outcome, u.Outcome, fmtTime(u.Now), u.Outcome, fmtTime(u.Now))
	return mapErr(err)
}

func (s *Store) RecordFeedback(ctx context.Context, leaseID, outcome, stage, note string, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO lease_feedback(feedback_id, lease_id, outcome, stage, note, created_at)
		VALUES(?,?,?,?,?,?)`,
		domain.NewID("fb_"), leaseID, outcome, stage, note, fmtTime(now))
	return mapErr(err)
}

type SweepStats struct {
	ExpiredLeases       int64 `json:"expired_leases"`
	ExpiredReservations int64 `json:"expired_reservations"`
	PrunedAttempts      int64 `json:"pruned_attempts"`
	PrunedFeedback      int64 `json:"pruned_feedback"`
	PrunedCooldowns     int64 `json:"pruned_cooldowns"`
}

func (s *Store) SweepExpired(ctx context.Context, now time.Time, attemptRetention time.Duration) (SweepStats, error) {
	var stats SweepStats
	nowStr := fmtTime(now)
	res, err := s.db.ExecContext(ctx, `
		UPDATE leases SET state = 'EXPIRED', released_at = ?
		WHERE state = 'ACTIVE' AND deadline <= ?`, nowStr, nowStr)
	if err != nil {
		return stats, mapErr(err)
	}
	stats.ExpiredLeases, _ = res.RowsAffected()
	res, err = s.db.ExecContext(ctx, `
		UPDATE reservations SET state = 'EXPIRED'
		WHERE state IN ('RESERVED','VALIDATING') AND deadline <= ?`, nowStr)
	if err != nil {
		return stats, mapErr(err)
	}
	stats.ExpiredReservations, _ = res.RowsAffected()
	cutoff := fmtTime(now.Add(-attemptRetention))
	res, err = s.db.ExecContext(ctx,
		"DELETE FROM validation_attempts WHERE completed_at < ?", cutoff)
	if err != nil {
		return stats, mapErr(err)
	}
	stats.PrunedAttempts, _ = res.RowsAffected()
	res, err = s.db.ExecContext(ctx,
		"DELETE FROM lease_feedback WHERE created_at < ?", cutoff)
	if err != nil {
		return stats, mapErr(err)
	}
	stats.PrunedFeedback, _ = res.RowsAffected()
	res, err = s.db.ExecContext(ctx,
		"DELETE FROM consumer_proxy_cooldowns WHERE last_delivered_at < ?",
		fmtTime(now.Add(-30*24*time.Hour)))
	if err != nil {
		return stats, mapErr(err)
	}
	stats.PrunedCooldowns, _ = res.RowsAffected()
	_, _ = s.db.ExecContext(ctx, "PRAGMA wal_checkpoint(PASSIVE)")
	return stats, nil
}

type Stats struct {
	Sources            int64 `json:"sources"`
	Resources          int64 `json:"resources"`
	EnabledOffers      int64 `json:"enabled_offers"`
	ActiveLeases       int64 `json:"active_leases"`
	ActiveReservations int64 `json:"active_reservations"`
	HealthPass         int64 `json:"health_pass"`
	HealthFail         int64 `json:"health_fail"`
}

func (s *Store) Stats(ctx context.Context) (Stats, error) {
	var st Stats
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sources").Scan(&st.Sources)
	if err != nil {
		return st, mapErr(err)
	}
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM proxy_resources").Scan(&st.Resources); err != nil {
		return st, mapErr(err)
	}
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM source_offers WHERE state = 'ENABLED'").Scan(&st.EnabledOffers); err != nil {
		return st, mapErr(err)
	}
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM leases WHERE state = 'ACTIVE'").Scan(&st.ActiveLeases); err != nil {
		return st, mapErr(err)
	}
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM reservations WHERE state IN ('RESERVED','VALIDATING')").Scan(&st.ActiveReservations); err != nil {
		return st, mapErr(err)
	}
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM scoped_health h
		JOIN proxy_resources r ON r.resource_id = h.resource_id
		WHERE h.last_outcome = 'PASS'`).Scan(&st.HealthPass); err != nil {
		return st, mapErr(err)
	}
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM scoped_health h
		JOIN proxy_resources r ON r.resource_id = h.resource_id
		WHERE h.last_outcome = 'FAIL'`).Scan(&st.HealthFail); err != nil {
		return st, mapErr(err)
	}
	return st, nil
}
