package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"proxypool/internal/domain"
)

const (
	// PoolMaintenanceMinBackoff prevents a permanently empty project from
	// turning inventory maintenance into a tight prewarm loop.
	PoolMaintenanceMinBackoff = 30 * time.Second
	// PoolMaintenanceScanLimit bounds one scheduler scan while leaving room
	// for projects beyond the current page to become due before the next one.
	PoolMaintenanceScanLimit = 32
)

const poolMaintenanceSchema = `
CREATE TABLE IF NOT EXISTS pool_maintenance_state (
  project_id TEXT PRIMARY KEY REFERENCES projects(project_id) ON DELETE CASCADE,
  config_version INTEGER NOT NULL DEFAULT 0,
  next_run_at TEXT NOT NULL,
  last_checked_at TEXT,
  last_job_id TEXT NOT NULL DEFAULT '',
  last_error TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pool_maintenance_due
  ON pool_maintenance_state(next_run_at, project_id);
CREATE TABLE IF NOT EXISTS pool_maintenance_jobs (
  job_id TEXT PRIMARY KEY REFERENCES jobs(job_id) ON DELETE CASCADE,
  project_id TEXT NOT NULL,
  config_version INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pool_maintenance_jobs_project
  ON pool_maintenance_jobs(project_id, created_at);
`

// ensurePoolMaintenanceSchemaTx installs the scheduler state without bumping
// the durable schema version. The project column is added defensively so a
// database created by an older release becomes usable in this same pass.
func ensurePoolMaintenanceSchemaTx(ctx context.Context, t *sql.Tx) error {
	var hasProjects int
	if err := t.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM sqlite_master
		WHERE type = 'table' AND name = 'projects'`).Scan(&hasProjects); err != nil {
		return err
	}
	if hasProjects > 0 {
		var hasTarget int
		if err := t.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM pragma_table_info('projects')
			WHERE name = 'auto_prewarm_target'`).Scan(&hasTarget); err != nil {
			return err
		}
		if hasTarget == 0 {
			if _, err := t.ExecContext(ctx, `
				ALTER TABLE projects
				ADD COLUMN auto_prewarm_target INTEGER NOT NULL DEFAULT 0`); err != nil {
				return err
			}
		}
	}
	_, err := t.ExecContext(ctx, poolMaintenanceSchema)
	return err
}

// EnsurePoolMaintenanceSchema is exposed for fixtures and callers that need
// the additive scheduler tables outside the main migration path.
func (s *Store) EnsurePoolMaintenanceSchema(ctx context.Context) error {
	return s.tx(ctx, func(t *sql.Tx) error {
		return ensurePoolMaintenanceSchemaTx(ctx, t)
	})
}

// PoolMaintenanceProject is one project due for a bounded inventory check.
type PoolMaintenanceProject struct {
	ProjectID     string
	ConfigVersion int64
	Target        int
	NextRunAt     *time.Time
}

// ListPoolMaintenanceDue returns enabled projects with a positive target whose
// durable deadline has elapsed. NULL state sorts first, and ties are broken by
// project ID so a full page cannot starve a stable suffix indefinitely.
func (s *Store) ListPoolMaintenanceDue(ctx context.Context, now time.Time,
	limit int) ([]PoolMaintenanceProject, error) {
	if limit <= 0 || limit > PoolMaintenanceScanLimit {
		limit = PoolMaintenanceScanLimit
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT p.project_id, p.config_version, p.auto_prewarm_target, st.next_run_at
		FROM projects p
		LEFT JOIN pool_maintenance_state st ON st.project_id = p.project_id
		WHERE p.enabled = 1 AND p.auto_prewarm_target > 0
		  AND (st.next_run_at IS NULL OR st.next_run_at <= ?)
		  AND NOT EXISTS (
		    SELECT 1 FROM jobs j
		    WHERE j.project_id = p.project_id AND j.kind = ? AND j.state IN (?,?)
		  )
		ORDER BY CASE WHEN st.next_run_at IS NULL THEN '' ELSE st.next_run_at END,
		         p.project_id
		LIMIT ?`, fmtTime(now), "prewarm", JobPending, JobRunning, limit)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	out := make([]PoolMaintenanceProject, 0, limit)
	for rows.Next() {
		var p PoolMaintenanceProject
		var next any
		if err := rows.Scan(&p.ProjectID, &p.ConfigVersion, &p.Target, &next); err != nil {
			return nil, mapErr(err)
		}
		if p.NextRunAt, err = scanTime(next); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, mapErr(rows.Err())
}

func normalizedPoolMaintenanceBackoff(backoff time.Duration) time.Duration {
	if backoff < PoolMaintenanceMinBackoff {
		return PoolMaintenanceMinBackoff
	}
	return backoff
}

// ClaimPoolMaintenance atomically reserves the next scheduling window after
// rechecking the project identity and active prewarm state. A successful claim
// does not mean a job was created; callers must still call StartJob and can
// release the audit slot with FinishPoolMaintenance if creation fails.
func (s *Store) ClaimPoolMaintenance(ctx context.Context, projectID string,
	expectedVersion int64, now time.Time, backoff time.Duration) (bool, error) {
	backoff = normalizedPoolMaintenanceBackoff(backoff)
	claimed := false
	err := s.tx(ctx, func(t *sql.Tx) error {
		var enabled int
		var target int
		var version int64
		err := t.QueryRowContext(ctx, `
			SELECT enabled, auto_prewarm_target, config_version
			FROM projects WHERE project_id = ?`, projectID).
			Scan(&enabled, &target, &version)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if enabled != 1 || target <= 0 || version != expectedVersion {
			return nil
		}
		var active int
		if err := t.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM jobs
			WHERE project_id = ? AND kind = ? AND state IN (?,?)`,
			projectID, "prewarm", JobPending, JobRunning).Scan(&active); err != nil {
			return err
		}
		if active > 0 {
			return nil
		}
		var next any
		err = t.QueryRowContext(ctx, `
			SELECT next_run_at FROM pool_maintenance_state
			WHERE project_id = ?`, projectID).Scan(&next)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if next != nil {
			due, err := scanTime(next)
			if err != nil {
				return err
			}
			if due.After(now) {
				return nil
			}
		}
		nextRun := now.Add(backoff)
		_, err = t.ExecContext(ctx, `
			INSERT INTO pool_maintenance_state(project_id, config_version, next_run_at,
			  last_checked_at, last_job_id, last_error, updated_at)
			VALUES(?,?,?,?, '', '', ?)
			ON CONFLICT(project_id) DO UPDATE SET
			  config_version = excluded.config_version,
			  next_run_at = excluded.next_run_at,
			  last_checked_at = excluded.last_checked_at,
			  last_error = '',
			  updated_at = excluded.updated_at`,
			projectID, version, fmtTime(nextRun), fmtTime(now), fmtTime(now))
		if err != nil {
			return err
		}
		claimed = true
		return nil
	})
	return claimed, mapErr(err)
}

// DeferPoolMaintenance records a completed check (for example, target already
// met) after proving the project version has not changed. A changed project is
// intentionally left due for the next scan.
func (s *Store) DeferPoolMaintenance(ctx context.Context, projectID string,
	expectedVersion int64, now time.Time, backoff time.Duration) (bool, error) {
	backoff = normalizedPoolMaintenanceBackoff(backoff)
	deferred := false
	err := s.tx(ctx, func(t *sql.Tx) error {
		var enabled int
		var target int
		var version int64
		err := t.QueryRowContext(ctx, `
			SELECT enabled, auto_prewarm_target, config_version
			FROM projects WHERE project_id = ?`, projectID).
			Scan(&enabled, &target, &version)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if enabled != 1 || target <= 0 || version != expectedVersion {
			return nil
		}
		nextRun := now.Add(backoff)
		_, err = t.ExecContext(ctx, `
			INSERT INTO pool_maintenance_state(project_id, config_version, next_run_at,
			  last_checked_at, last_job_id, last_error, updated_at)
			VALUES(?,?,?,?, '', '', ?)
			ON CONFLICT(project_id) DO UPDATE SET
			  config_version = excluded.config_version,
			  next_run_at = excluded.next_run_at,
			  last_checked_at = excluded.last_checked_at,
			  last_error = '',
			  updated_at = excluded.updated_at`,
			projectID, version, fmtTime(nextRun), fmtTime(now), fmtTime(now))
		if err != nil {
			return err
		}
		deferred = true
		return nil
	})
	return deferred, mapErr(err)
}

// RecordPoolMaintenanceJob links an automatically-created prewarm job to the
// scheduler. Keeping this mapping separate leaves prewarm params untouched and
// ensures target-zero cleanup can never cancel a manual job.
func (s *Store) RecordPoolMaintenanceJob(ctx context.Context, projectID, jobID string,
	configVersion int64, now time.Time) error {
	return s.tx(ctx, func(t *sql.Tx) error {
		var n int
		if err := t.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM jobs
			WHERE job_id = ? AND project_id = ? AND kind = ?`,
			jobID, projectID, "prewarm").Scan(&n); err != nil {
			return err
		}
		if n == 0 {
			return domain.NotFound("prewarm job")
		}
		if _, err := t.ExecContext(ctx, `
			INSERT INTO pool_maintenance_jobs(job_id, project_id, config_version, created_at)
			VALUES(?,?,?,?)
			ON CONFLICT(job_id) DO NOTHING`,
			jobID, projectID, configVersion, fmtTime(now)); err != nil {
			return err
		}
		_, err := t.ExecContext(ctx, `
			UPDATE pool_maintenance_state
			SET last_job_id = ?, last_error = '', updated_at = ?
			WHERE project_id = ?`, jobID, fmtTime(now), projectID)
		return err
	})
}

// FinishPoolMaintenance records an unsuccessful start without discarding the
// durable backoff that ClaimPoolMaintenance already reserved.
func (s *Store) FinishPoolMaintenance(ctx context.Context, projectID, jobID,
	lastError string, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE pool_maintenance_state
		SET last_job_id = CASE WHEN ? <> '' THEN ? ELSE last_job_id END,
		    last_error = ?, updated_at = ?
		WHERE project_id = ?`,
		jobID, jobID, lastError, fmtTime(now), projectID)
	return mapErr(err)
}

func (s *Store) AccountFetchAvailable(ctx context.Context, credentialKey string,
	now time.Time) (bool, error) {
	var deadline any
	err := s.db.QueryRowContext(ctx, `
		SELECT next_fetch_at FROM account_fetch_limits
		WHERE credential_key = ?`, credentialKey).Scan(&deadline)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, mapErr(err)
	}
	at, err := scanTime(deadline)
	if err != nil {
		return false, err
	}
	return at == nil || !at.After(now), nil
}

// PoolMaintenanceJobsToStop returns active prewarm jobs created automatically
// under project configuration that has since changed. Every project edit bumps
// config_version, covering disable/target reachability changes as well as
// allowlist and validation changes. Manual prewarm jobs have no
// pool_maintenance_jobs row and are never returned.
func (s *Store) PoolMaintenanceJobsToStop(ctx context.Context, limit int) ([]string, error) {
	if limit <= 0 || limit > PoolMaintenanceScanLimit {
		limit = PoolMaintenanceScanLimit
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT j.job_id
		FROM pool_maintenance_jobs pmj
		JOIN jobs j ON j.job_id = pmj.job_id
		JOIN projects p ON p.project_id = pmj.project_id
		WHERE j.kind = ? AND j.state IN (?,?)
		  AND (p.enabled = 0 OR p.auto_prewarm_target <= 0
		       OR p.config_version != pmj.config_version)
		ORDER BY j.created_at, j.job_id
		LIMIT ?`, "prewarm", JobPending, JobRunning, limit)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	out := make([]string, 0, limit)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, mapErr(err)
		}
		out = append(out, id)
	}
	return out, mapErr(rows.Err())
}
