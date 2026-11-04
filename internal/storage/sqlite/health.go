package sqlite

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"proxypool/internal/domain"
)

func (s *Store) RecordAttempt(ctx context.Context, ev domain.Evidence, resourceID, contextKey, detail string) error {
	// Existence guard: a probe that started before its candidate was deleted
	// must not leave attempt rows behind for a removed resource.
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO validation_attempts(attempt_id, request_id, resource_id, context_key, profile,
		  outcome, started_at, completed_at, latency_ms, detail_json)
		SELECT ?,?,?,?,?,?,?,?,?,?
		WHERE EXISTS (SELECT 1 FROM proxy_resources r WHERE r.resource_id = ?)`,
		ev.AttemptID, ev.RequestID, resourceID, contextKey, ev.Profile,
		ev.Outcome, fmtTime(ev.StartedAt), fmtTime(ev.CompletedAt), ev.LatencyMS, detail,
		resourceID)
	return mapErr(err)
}

// RecordAttemptScoped is the incarnation-guarded form: the write only lands
// while the resource row still carries the incarnation the probe selected.
// A delete + re-import gives the same connection a new incarnation, so a slow
// old probe can never write into the recreated object.
func (s *Store) RecordAttemptScoped(ctx context.Context, ev domain.Evidence,
	resourceID, contextKey, detail, incarnation string) error {
	return s.RecordAttemptAttributed(ctx, ev, resourceID, contextKey, detail,
		incarnation, "", 0)
}

// RecordAttemptAttributed writes the attempt and a durable source-version
// attribution in one statement pair. The attribution outlives the offer so
// daily statistics do not lose a failure when AUTH deletes the resource.
func (s *Store) RecordAttemptAttributed(ctx context.Context, ev domain.Evidence,
	resourceID, contextKey, detail, incarnation, sourceID string,
	sourceConfigVersion int64) error {
	if incarnation == "" || strings.HasPrefix(incarnation, "ondemand_") ||
		strings.HasPrefix(incarnation, "static_") {
		if incarnation != "" && sourceID != "" {
			// Ephemeral candidates (API/static) intentionally have no
			// proxy_resources row. Retain only source attribution; never
			// create connection inventory or a bad-proxy list.
			_, err := s.db.ExecContext(ctx, `
				INSERT OR IGNORE INTO validation_attempt_sources(attempt_id, source_id,
				  source_config_version, completed_at, outcome)
				VALUES(?,?,?,?,?)`,
				ev.AttemptID, sourceID, sourceConfigVersion, fmtTime(ev.CompletedAt),
				ev.Outcome)
			if err != nil {
				return mapErr(err)
			}
			return s.RecordSourceStats(ctx, sourceID,
				boolInt(ev.Outcome == "PASS"), boolInt(ev.Outcome == "FAIL"),
				ev.CompletedAt)
		}
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO validation_attempts(attempt_id, request_id, resource_id, context_key, profile,
			  outcome, started_at, completed_at, latency_ms, detail_json)
			SELECT ?,?,?,?,?,?,?,?,?,?
			WHERE EXISTS (SELECT 1 FROM proxy_resources r WHERE r.resource_id = ?)`,
			ev.AttemptID, ev.RequestID, resourceID, contextKey, ev.Profile,
			ev.Outcome, fmtTime(ev.StartedAt), fmtTime(ev.CompletedAt), ev.LatencyMS, detail,
			resourceID)
		if err != nil || sourceID == "" {
			return mapErr(err)
		}
		if err := s.RecordSourceStats(ctx, sourceID, boolInt(ev.Outcome == "PASS"),
			boolInt(ev.Outcome == "FAIL"), ev.CompletedAt); err != nil {
			return err
		}
		_, err = s.db.ExecContext(ctx, `
			INSERT OR IGNORE INTO validation_attempt_sources(attempt_id, source_id,
			  source_config_version, completed_at, outcome)
			SELECT ?,?,?,?,? WHERE EXISTS (
			  SELECT 1 FROM validation_attempts WHERE attempt_id = ?)`,
			ev.AttemptID, sourceID, sourceConfigVersion, fmtTime(ev.CompletedAt),
			ev.Outcome, ev.AttemptID)
		return mapErr(err)
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO validation_attempts(attempt_id, request_id, resource_id, context_key, profile,
		  outcome, started_at, completed_at, latency_ms, detail_json)
		SELECT ?,?,?,?,?,?,?,?,?,?
		WHERE EXISTS (SELECT 1 FROM proxy_resources r
		              WHERE r.resource_id = ? AND r.incarnation = ?)`,
		ev.AttemptID, ev.RequestID, resourceID, contextKey, ev.Profile,
		ev.Outcome, fmtTime(ev.StartedAt), fmtTime(ev.CompletedAt), ev.LatencyMS, detail,
		resourceID, incarnation)
	if err != nil || sourceID == "" {
		return mapErr(err)
	}
	if err := s.RecordSourceStats(ctx, sourceID, boolInt(ev.Outcome == "PASS"),
		boolInt(ev.Outcome == "FAIL"), ev.CompletedAt); err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT OR IGNORE INTO validation_attempt_sources(attempt_id, source_id,
		  source_config_version, completed_at, outcome)
		SELECT ?,?,?,?,? WHERE EXISTS (
		  SELECT 1 FROM validation_attempts WHERE attempt_id = ?)`,
		ev.AttemptID, sourceID, sourceConfigVersion, fmtTime(ev.CompletedAt),
		ev.Outcome, ev.AttemptID)
	return mapErr(err)
}

type HealthUpdate struct {
	ResourceID    string
	ContextKey    string
	Profile       string
	Outcome       string // PASS or FAIL
	CooldownUntil *time.Time
	// Incarnation optionally pins the write to the resource row the probe
	// actually saw; empty falls back to the existence-only guard.
	Incarnation string
	Now         time.Time
}

type HealthRow struct {
	ConsecutiveFailures int
	CooldownUntil       *time.Time
	LastOutcome         string
	LastPassAt          *time.Time
	FailureEpoch        int64
}

func (s *Store) CurrentHealth(ctx context.Context, resourceID, contextKey, profile string) (HealthRow, error) {
	return s.CurrentHealthGuarded(ctx, resourceID, contextKey, profile, "")
}

// CurrentHealthGuarded returns health only for the resource incarnation that
// the caller observed. No row means the object was deleted or re-imported.
func (s *Store) CurrentHealthGuarded(ctx context.Context, resourceID, contextKey,
	profile, incarnation string) (HealthRow, error) {
	guard, guardArgs := resourceGuard(resourceID, incarnation)
	args := []any{resourceID, contextKey, profile}
	args = append(args, guardArgs...)
	var row HealthRow
	var cooldown, lastPass any
	err := s.db.QueryRowContext(ctx, `
		SELECT consecutive_failures, cooldown_until, last_outcome, last_pass_at, failure_epoch
		FROM scoped_health WHERE resource_id = ? AND context_key = ? AND profile = ?
		  AND `+guard, args...).
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
	if u.ResourceID == "" {
		return nil
	}
	guard, guardArgs := resourceGuard(u.ResourceID, u.Incarnation)
	nowStr := fmtTime(u.Now)
	updateArgs := []any{u.Outcome, nullTime(u.CooldownUntil), u.Outcome,
		u.Outcome, nowStr, u.Outcome, nowStr,
		u.ResourceID, u.ContextKey, u.Profile}
	updateArgs = append(updateArgs, guardArgs...)
	res, err := s.db.ExecContext(ctx, `
		UPDATE scoped_health SET
		  consecutive_failures = CASE WHEN ? = 'PASS' THEN 0 ELSE consecutive_failures + 1 END,
		  cooldown_until = ?,
		  last_outcome = ?,
		  last_pass_at = CASE WHEN ? = 'PASS' THEN ? ELSE last_pass_at END,
			  failure_epoch = CASE WHEN ? = 'PASS' THEN failure_epoch ELSE failure_epoch + 1 END,
		  updated_at = ?
		WHERE resource_id = ? AND context_key = ? AND profile = ? AND `+guard,
		updateArgs...)
	if err != nil {
		return mapErr(err)
	}
	if n, rerr := res.RowsAffected(); rerr == nil && n > 0 {
		return nil
	}
	// Fresh row: the guard is re-evaluated at insert time, so a resource
	// deleted (or re-imported under a new incarnation) between the two
	// statements can never be revived.
	args := []any{u.ResourceID, u.ContextKey, u.Profile,
		u.Outcome, nullTime(u.CooldownUntil),
		u.Outcome, u.Outcome, nowStr, u.Outcome, nowStr}
	args = append(args, guardArgs...)
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO scoped_health(resource_id, context_key, profile, consecutive_failures,
		  cooldown_until, last_outcome, last_pass_at, failure_epoch, updated_at)
		SELECT ?,?,?,
		  CASE WHEN ? = 'PASS' THEN 0 ELSE 1 END,
		  ?, ?, CASE WHEN ? = 'PASS' THEN ? ELSE NULL END,
		  CASE WHEN ? = 'PASS' THEN 0 ELSE 1 END, ?
		WHERE `+guard, args...)
	return mapErr(err)
}

// resourceGuard renders the survival predicate for guarded writes.
func resourceGuard(resourceID, incarnation string) (string, []any) {
	if incarnation == "" {
		return `EXISTS (SELECT 1 FROM proxy_resources r WHERE r.resource_id = ?)`,
			[]any{resourceID}
	}
	return `EXISTS (SELECT 1 FROM proxy_resources r
	              WHERE r.resource_id = ? AND r.incarnation = ?)`,
		[]any{resourceID, incarnation}
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
