package sqlite

import (
	"context"
	"database/sql"
	"time"
)

// openDeliveryCDTx starts or extends a fixed project CD window. Callers run
// it inside the same transaction that inserts the delivered lease, so a
// failed lease/binding write rolls the CD back instead of leaving a phantom
// window that later cleanup could mistake for another request's window.
func openDeliveryCDTx(t *sql.Tx, ctx context.Context, a CommitArgs) error {
	if a.ProjectID == "" || a.DeliveryCooldown <= 0 || a.Fingerprint == "" {
		return nil
	}
	blockedUntil := fmtTime(a.Now.Add(a.DeliveryCooldown))
	_, err := t.ExecContext(ctx, `
		INSERT INTO project_proxy_cd(project_id, connection_fingerprint, issued_at, blocked_until)
		VALUES(?,?,?,?)
		ON CONFLICT(project_id, connection_fingerprint) DO UPDATE SET
		  blocked_until = MAX(blocked_until, excluded.blocked_until),
		  issued_at = CASE
		    WHEN excluded.blocked_until > blocked_until THEN excluded.issued_at
		    ELSE issued_at END`,
		a.ProjectID, a.Fingerprint, fmtTime(a.Now), blockedUntil)
	return err
}

// RecordProjectDeliveries opens the fixed project cooldown window for the
// delivered connection fingerprints. Existing windows are merged with MAX so
// a shorter new cooldown can never shorten a longer live block, and the
// window is deliberately independent of inventory rows and api keys.
func (s *Store) RecordProjectDeliveries(ctx context.Context, projectID string,
	cooldown time.Duration, now time.Time, fingerprints []string) error {
	if projectID == "" || cooldown <= 0 || len(fingerprints) == 0 {
		return nil
	}
	blockedUntil := fmtTime(now.Add(cooldown))
	return s.tx(ctx, func(t *sql.Tx) error {
		for _, fp := range fingerprints {
			if fp == "" {
				continue
			}
			if _, err := t.ExecContext(ctx, `
				INSERT INTO project_proxy_cd(project_id, connection_fingerprint, issued_at, blocked_until)
				VALUES(?,?,?,?)
				ON CONFLICT(project_id, connection_fingerprint) DO UPDATE SET
				  blocked_until = MAX(blocked_until, excluded.blocked_until),
				  issued_at = CASE
				    WHEN excluded.blocked_until > blocked_until THEN excluded.issued_at
				    ELSE issued_at END`,
				projectID, fp, fmtTime(now), blockedUntil); err != nil {
				return err
			}
		}
		return nil
	})
}

// ProjectCooldownBlocked reports which fingerprints are inside a live fixed
// cooldown window for the project. Reading the window does not depend on the
// current request's cd parameter.
func (s *Store) ProjectCooldownBlocked(ctx context.Context, projectID string,
	now time.Time, fingerprints []string) (map[string]time.Time, error) {
	blocked := map[string]time.Time{}
	if projectID == "" || len(fingerprints) == 0 {
		return blocked, nil
	}
	args := append([]any{projectID, fmtTime(now), projectWideCooldownFingerprint},
		toAny(fingerprints)...)
	rows, err := s.db.QueryContext(ctx, `
		SELECT connection_fingerprint, blocked_until FROM project_proxy_cd
		WHERE project_id = ? AND blocked_until > ?
		  AND connection_fingerprint IN (`+placeholders(len(fingerprints)+1)+`)`,
		args...)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	for rows.Next() {
		var fp string
		var until any
		if err := rows.Scan(&fp, &until); err != nil {
			return nil, mapErr(err)
		}
		untilTime, err := scanTime(until)
		if err != nil {
			return nil, err
		}
		if untilTime == nil {
			continue
		}
		if fp == projectWideCooldownFingerprint {
			// A project-wide hold blocks every queried fingerprint.
			for _, f := range fingerprints {
				if prev, ok := blocked[f]; !ok || untilTime.After(prev) {
					blocked[f] = *untilTime
				}
			}
			continue
		}
		if prev, ok := blocked[fp]; !ok || untilTime.After(prev) {
			blocked[fp] = *untilTime
		}
	}
	return blocked, mapErr(rows.Err())
}

func toAny(values []string) []any {
	out := make([]any, len(values))
	for i, v := range values {
		out[i] = v
	}
	return out
}

// projectBlockedTx is the reservation-transaction form of the same check.
func projectBlockedTx(t *sql.Tx, ctx context.Context, projectID, fingerprint string, now time.Time) (bool, error) {
	if projectID == "" || fingerprint == "" {
		return false, nil
	}
	rows, err := t.QueryContext(ctx, `
		SELECT connection_fingerprint, blocked_until FROM project_proxy_cd
		WHERE project_id = ? AND connection_fingerprint IN (?, ?)`,
		projectID, projectWideCooldownFingerprint, fingerprint)
	if err != nil {
		return false, mapErr(err)
	}
	defer rows.Close()
	for rows.Next() {
		var until any
		if err := rows.Scan(new(string), &until); err != nil {
			return false, err
		}
		untilTime, err := scanTime(until)
		if err != nil {
			return false, err
		}
		if untilTime != nil && untilTime.After(now) {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, mapErr(err)
	}
	return false, nil
}

// ClearExpiredProjectCooldowns removes windows that are fully in the past.
func (s *Store) ClearExpiredProjectCooldowns(ctx context.Context, now time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM project_proxy_cd WHERE blocked_until <= ?`, fmtTime(now))
	if err != nil {
		return 0, mapErr(err)
	}
	n, err := res.RowsAffected()
	return n, mapErr(err)
}
