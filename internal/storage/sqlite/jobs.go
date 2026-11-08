package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"proxypool/internal/domain"
)

// Job lifecycle states. A job reaches exactly one final state; CANCELLED and
// INTERRUPTED can only be produced by CancelJob and RecoverInterruptedJobs.
const (
	JobPending      = "PENDING"
	JobRunning      = "RUNNING"
	JobCompleted    = "COMPLETED"
	JobPartial      = "PARTIAL"
	JobFailed       = "FAILED"
	JobCancelled    = "CANCELLED"
	JobInterrupted  = "INTERRUPTED"
	JobStopCanceled = "cancelled"
)

var jobFinalStates = map[string]bool{
	JobCompleted:   true,
	JobPartial:     true,
	JobFailed:      true,
	JobCancelled:   true,
	JobInterrupted: true,
}

func encodeJobParams(params string) string {
	if params == "" {
		return "{}"
	}
	return params
}

// CreateJob persists a queued job. Ready starts at 0 because it is a current
// qualification count recomputed by the runner, never a carried-over sum.
func (s *Store) CreateJob(ctx context.Context, j domain.Job, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO jobs(job_id, project_id, kind, state, params_json, target,
		  ready, probed, failed, stop_reason, last_error, created_at, updated_at)
		VALUES(?,?,?,?,?,?,0,0,0,'','',?,?)`,
		j.ID, j.ProjectID, j.Kind, JobPending, encodeJobParams(j.ParamsJSON),
		j.Target, fmtTime(now), fmtTime(now))
	return mapErr(err)
}

// CreateJobWithLimits checks and inserts the job in one transaction. Checking
// separately lets concurrent StartJob calls pass the same cap and then create
// an unbounded backlog together. The insert still fails if the row is lost
// between check and commit.
func (s *Store) CreateJobWithLimits(ctx context.Context, j domain.Job, now time.Time,
	perProject, global int) error {
	return s.tx(ctx, func(t *sql.Tx) error {
		var n int
		if err := t.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM jobs
			WHERE project_id = ? AND state IN (?,?)`,
			j.ProjectID, JobPending, JobRunning).Scan(&n); err != nil {
			return err
		}
		if n >= perProject {
			return domain.E(jobLimitCode(j.Kind), 409,
				"project %q already has %d active jobs", j.ProjectID, n)
		}
		if err := t.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM jobs WHERE state IN (?,?)`,
			JobPending, JobRunning).Scan(&n); err != nil {
			return err
		}
		if n >= global {
			return domain.E(jobLimitCode(j.Kind), 409,
				"deployment already has %d active jobs", n)
		}
		_, err := t.ExecContext(ctx, `
			INSERT INTO jobs(job_id, project_id, kind, state, params_json, target,
			  ready, probed, failed, stop_reason, last_error, created_at, updated_at)
			VALUES(?,?,?,?,?,?,0,0,0,'','',?,?)`,
			j.ID, j.ProjectID, j.Kind, JobPending, encodeJobParams(j.ParamsJSON),
			j.Target, fmtTime(now), fmtTime(now))
		return err
	})
}

func jobLimitCode(kind string) string {
	if kind == "source_refresh" {
		return "SOURCE_FETCH_JOB_LIMIT"
	}
	return "PREWARM_JOB_LIMIT"
}

const jobColumns = `
	job_id, project_id, kind, state, params_json, target, ready, probed, failed,
	stop_reason, last_error, created_at, updated_at, started_at, finished_at,
	EXISTS(SELECT 1 FROM pool_maintenance_jobs pmj WHERE pmj.job_id = jobs.job_id)`

func scanJob(row interface{ Scan(...any) error }) (domain.Job, error) {
	var j domain.Job
	var params string
	var created, updated string
	var started, finished any
	err := row.Scan(&j.ID, &j.ProjectID, &j.Kind, &j.State, &params, &j.Target,
		&j.Ready, &j.Probed, &j.Failed, &j.StopReason, &j.LastError,
		&created, &updated, &started, &finished, &j.Automatic)
	if err != nil {
		return domain.Job{}, err
	}
	j.ParamsJSON = params
	if j.CreatedAt, err = time.Parse(time.RFC3339Nano, created); err != nil {
		return domain.Job{}, err
	}
	if j.UpdatedAt, err = time.Parse(time.RFC3339Nano, updated); err != nil {
		return domain.Job{}, err
	}
	if j.StartedAt, err = scanTime(started); err != nil {
		return domain.Job{}, err
	}
	if j.FinishedAt, err = scanTime(finished); err != nil {
		return domain.Job{}, err
	}
	return j, nil
}

func (s *Store) GetJob(ctx context.Context, id string) (domain.Job, error) {
	j, err := scanJob(s.db.QueryRowContext(ctx,
		`SELECT `+jobColumns+` FROM jobs WHERE job_id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Job{}, domain.NotFound("job")
	}
	return j, mapErr(err)
}

// GetProjectJob returns the job only when the project owns it; an empty
// projectID (deployment admin) sees every job. Ownership mismatches are
// indistinguishable from missing jobs so other projects cannot enumerate IDs.
func (s *Store) GetProjectJob(ctx context.Context, projectID, id string) (domain.Job, error) {
	j, err := s.GetJob(ctx, id)
	if err != nil {
		return domain.Job{}, err
	}
	if projectID != "" && j.ProjectID != projectID {
		return domain.Job{}, domain.NotFound("job")
	}
	return j, nil
}

// StartJobRun claims a queued job for execution; idempotent for the job the
// runner already started.
func (s *Store) StartJobRun(ctx context.Context, id string, now time.Time) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE jobs SET state = ?, started_at = COALESCE(started_at, ?), updated_at = ?
		WHERE job_id = ? AND state = ?`,
		JobRunning, fmtTime(now), fmtTime(now), id, JobPending)
	if err != nil {
		return mapErr(err)
	}
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		return domain.NotFound("pending job")
	}
	return nil
}

// SetJobProgress stores the recomputed current ready count (an absolute
// value, never a delta) and bumps the attempt counters.
func (s *Store) SetJobProgress(ctx context.Context, id string, ready, probedDelta,
	failedDelta int, lastError string, now time.Time) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE jobs SET
		  ready = ?,
		  target = MAX(target, ?),
		  probed = probed + ?,
		  failed = failed + ?,
		  last_error = CASE WHEN ? != '' THEN ? ELSE last_error END,
		  updated_at = ?
		WHERE job_id = ? AND state = ?`,
		ready, ready, probedDelta, failedDelta, lastError, lastError, fmtTime(now), id, JobRunning)
	if err != nil {
		return mapErr(err)
	}
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		return domain.NotFound("running job")
	}
	return nil
}

// FinishJob moves a queued or running job into exactly one final state.
// Cancelling callers win the race: once the job row is final, later runner
// updates fail with NotFound and must not overwrite the terminal state.
func (s *Store) FinishJob(ctx context.Context, id, state, stopReason, lastError string,
	now time.Time) error {
	if !jobFinalStates[state] {
		return errors.New("finish state must be final")
	}
	res, err := s.db.ExecContext(ctx, `
		UPDATE jobs SET state = ?, stop_reason = ?, last_error = ?, finished_at = ?, updated_at = ?
		WHERE job_id = ? AND state IN (?,?)`,
		state, stopReason, lastError, fmtTime(now), fmtTime(now), id, JobPending, JobRunning)
	if err != nil {
		return mapErr(err)
	}
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		return domain.NotFound("running job")
	}
	return nil
}

// CancelJob cancels a queued or running job the project owns and reports
// whether the row changed. The runner observes the state change and stops.
func (s *Store) CancelJob(ctx context.Context, projectID, id string, now time.Time) (bool, error) {
	query := `
		UPDATE jobs SET state = ?, stop_reason = ?, finished_at = ?, updated_at = ?
		WHERE job_id = ? AND state IN (?,?)`
	args := []any{JobCancelled, JobStopCanceled, fmtTime(now), fmtTime(now),
		id, JobPending, JobRunning}
	if projectID != "" {
		query += ` AND project_id = ?`
		args = append(args, projectID)
	}
	res, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return false, mapErr(err)
	}
	n, err := res.RowsAffected()
	return n > 0, mapErr(err)
}

// CountActiveJobs reports queued plus running jobs of one kind across all
// projects; CountActiveJobsByProject applies the same count per owner.
func (s *Store) CountActiveJobs(ctx context.Context, kind string) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM jobs WHERE kind = ? AND state IN (?,?)`,
		kind, JobPending, JobRunning).Scan(&n)
	return n, mapErr(err)
}

func (s *Store) CountActiveJobsByProject(ctx context.Context, kind, projectID string) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM jobs WHERE kind = ? AND project_id = ? AND state IN (?,?)`,
		kind, projectID, JobPending, JobRunning).Scan(&n)
	return n, mapErr(err)
}

// RecoverInterruptedJobs marks queued and running jobs as INTERRUPTED after a
// service restart: the previous process's in-memory runner is gone, and its
// accumulated ready count cannot be trusted, so the job must be retried
// explicitly instead of silently resuming.
func (s *Store) RecoverInterruptedJobs(ctx context.Context, reason string, now time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx, `
		UPDATE jobs SET state = ?, stop_reason = ?, finished_at = ?, updated_at = ?
		WHERE state IN (?,?)`,
		JobInterrupted, reason, fmtTime(now), fmtTime(now), JobPending, JobRunning)
	if err != nil {
		return 0, mapErr(err)
	}
	n, err := res.RowsAffected()
	return n, mapErr(err)
}

func (s *Store) ListProjectJobs(ctx context.Context, projectID string, limit int) ([]domain.Job, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+jobColumns+` FROM jobs WHERE project_id = ? ORDER BY created_at DESC, job_id DESC LIMIT ?`,
		projectID, limit)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []domain.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, mapErr(err)
		}
		out = append(out, j)
	}
	return out, mapErr(rows.Err())
}

// ListStaleRunningJobs returns RUNNING jobs of a kind whose row has not been
// updated since staleBefore. Live runners persist progress every round, so a
// silent row has lost its runner (crashed process, failed finalize) even when
// the service is still up.
func (s *Store) ListStaleRunningJobs(ctx context.Context, kind string,
	staleBefore time.Time, limit int) ([]domain.Job, error) {
	if limit <= 0 {
		limit = 32
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+jobColumns+` FROM jobs WHERE kind = ? AND state = ? AND updated_at < ?
		 ORDER BY created_at LIMIT ?`,
		kind, JobRunning, fmtTime(staleBefore), limit)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []domain.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, mapErr(err)
		}
		out = append(out, j)
	}
	return out, mapErr(rows.Err())
}

// PrewarmQuery selects one bounded page of custom-source inventory candidates
// for background validation accounting. Evidence reuse is intentionally not
// SQL: connection fingerprints are HMAC values the store cannot compute, so
// the caller matches validation_evidence in Go through RecentEvidence and
// filters fixed project CD the same way.
type PrewarmQuery struct {
	ProjectID    string
	Profile      string
	Now          time.Time
	Transports   []domain.Transport
	Kinds        []domain.ResourceKind
	SourceIDs    []string
	Countries    []string
	ExcludeByIDs []string
	Limit        int
	// IncludeCooling keeps candidates whose source circuit is OPEN, so the
	// runner can distinguish "cooling" from "exhausted" in its stop reason.
	IncludeCooling bool
	// ScopeUnrestricted distinguishes nil project authorization from an
	// explicitly empty allowlist. ScopeEmpty authorizes no candidates.
	ScopeUnrestricted bool
	ScopeEmpty        bool
}

func (s *Store) SelectPrewarmCandidates(ctx context.Context, q PrewarmQuery) ([]domain.Candidate, error) {
	if q.ScopeEmpty {
		return nil, nil
	}
	if q.Limit <= 0 {
		q.Limit = 64
	}
	args := []any{q.Profile, fmtTime(q.Now), fmtTime(q.Now), fmtTime(q.Now)}
	query := `
		SELECT o.offer_id, o.source_id, o.resource_id, o.source_record_id, o.source_generation,
		       o.provider_expires_at, o.local_retention_deadline, o.expiry_basis, o.claims_json,
		       o.max_pool_leases, o.capacity_group, o.state,
		       r.transport, r.host, r.port, r.credential_version, r.session_policy, r.resource_kind,
		       COALESCE(cr.username, ''), COALESCE(cr.password, ''),
		       COALESCE(h.last_outcome, 'UNKNOWN'), h.last_pass_at, COALESCE(h.consecutive_failures, 0),
		       src.config_version, r.incarnation,
		       ro.exit_ip, ro.country_code, ro.country, ro.region, ro.city, ro.isp, ro.observed_at
		FROM source_offers o
		JOIN proxy_resources r ON r.resource_id = o.resource_id AND r.disabled = 0
		JOIN sources src ON src.source_id = o.source_id AND src.enabled = 1
		  AND src.adapter_type = 'custom'
		LEFT JOIN proxy_credentials cr ON cr.credential_version = r.credential_version
		LEFT JOIN source_circuits sc ON sc.source_id = o.source_id
		LEFT JOIN resource_observations ro ON ro.resource_id = r.resource_id
		LEFT JOIN scoped_health h
		  ON h.resource_id = r.resource_id
		 AND h.context_key = r.credential_version || '|' || r.session_policy
		 AND h.profile = ?
		WHERE o.state = 'ENABLED'
		  AND (o.provider_expires_at IS NULL OR o.provider_expires_at > ?)
		  AND (o.local_retention_deadline IS NULL OR o.local_retention_deadline > ?)
		  AND (h.cooldown_until IS NULL OR h.cooldown_until <= ?)`
	if !q.IncludeCooling {
		query += `
		  AND (sc.cooldown_until IS NULL OR sc.cooldown_until <= ?)`
		args = append(args, fmtTime(q.Now))
	}
	if q.ProjectID != "" {
		query += sourceProjectAccessClause
		args = append(args, q.ProjectID)
	}
	query += `
		  AND NOT EXISTS (
		        SELECT 1 FROM leases l
		        WHERE l.resource_id = r.resource_id AND l.state = 'ACTIVE')`
	if len(q.Transports) > 0 {
		query += " AND r.transport IN (" + placeholders(len(q.Transports)) + ")"
		for _, v := range q.Transports {
			args = append(args, string(v))
		}
	}
	if len(q.Kinds) > 0 {
		query += " AND r.resource_kind IN (" + placeholders(len(q.Kinds)) + ")"
		for _, v := range q.Kinds {
			args = append(args, string(v))
		}
	}
	if !q.ScopeUnrestricted && len(q.SourceIDs) == 0 {
		return nil, nil
	}
	if !q.ScopeUnrestricted {
		query += " AND o.source_id IN (" + placeholders(len(q.SourceIDs)) + ")"
		for _, v := range q.SourceIDs {
			args = append(args, v)
		}
	}
	if len(q.Countries) > 0 {
		query += " AND (ro.country_code IN (" + placeholders(len(q.Countries)) + ") OR ro.resource_id IS NULL)"
		for _, v := range q.Countries {
			args = append(args, v)
		}
	}
	if len(q.ExcludeByIDs) > 0 {
		query += " AND r.resource_id NOT IN (" + placeholders(len(q.ExcludeByIDs)) + ")"
		for _, v := range q.ExcludeByIDs {
			args = append(args, v)
		}
	}
	query += `
		ORDER BY (h.last_pass_at IS NULL) DESC, h.last_pass_at ASC, random()
		LIMIT ?`
	args = append(args, q.Limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []domain.Candidate
	for rows.Next() {
		var c domain.Candidate
		var providerExp, retention, lastPass any
		var claimsJSON string
		var port int
		var obsIP, obsCountryCode, obsCountry, obsRegion, obsCity, obsISP, obsAt sql.NullString
		if err := rows.Scan(
			&c.Offer.ID, &c.Offer.SourceID, &c.Offer.ResourceID, &c.Offer.SourceRecordID,
			&c.Offer.SourceGeneration, &providerExp, &retention, &c.Offer.ExpiryBasis,
			&claimsJSON, &c.Offer.MaxPoolLeases, &c.Offer.CapacityGroup, &c.Offer.State,
			&c.Resource.Conn.Transport, &c.Resource.Conn.Host, &port,
			&c.Resource.CredentialVersion, &c.Resource.Session, &c.Resource.Kind,
			&c.Resource.Conn.Username, &c.Resource.Conn.Password,
			&c.LastOutcome, &lastPass, &c.ConsecutiveFailures,
			&c.SourceConfigVersion,
			&c.Incarnation,
			&obsIP, &obsCountryCode, &obsCountry, &obsRegion, &obsCity, &obsISP, &obsAt,
		); err != nil {
			return nil, mapErr(err)
		}
		c.Resource.Conn.Port = port
		c.Resource.ID = c.Offer.ResourceID
		if c.Offer.ProviderExpiresAt, err = scanTime(providerExp); err != nil {
			return nil, err
		}
		if c.Offer.LocalRetentionDeadline, err = scanTime(retention); err != nil {
			return nil, err
		}
		if c.LastPassAt, err = scanTime(lastPass); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(claimsJSON), &c.Offer.Claims); err != nil {
			return nil, errors.New("decode claims: " + err.Error())
		}
		if obsIP.Valid && obsIP.String != "" {
			obs := &domain.Observation{
				ExitIP:      obsIP.String,
				CountryCode: obsCountryCode.String,
				Country:     obsCountry.String,
				Region:      obsRegion.String,
				City:        obsCity.String,
				ISP:         obsISP.String,
			}
			if obsAt.Valid {
				if t, err := time.Parse(time.RFC3339Nano, obsAt.String); err == nil {
					obs.ObservedAt = t
				}
			}
			c.Observation = obs
		}
		out = append(out, c)
	}
	return out, mapErr(rows.Err())
}

// ListJobs lists recent jobs; an empty projectID lists every project's jobs
// for the admin console.
func (s *Store) ListJobs(ctx context.Context, projectID string, limit int) ([]domain.Job, error) {
	if limit <= 0 {
		limit = 50
	}
	query := `SELECT ` + jobColumns + ` FROM jobs`
	args := []any{}
	if projectID != "" {
		query += ` WHERE project_id = ?`
		args = append(args, projectID)
	}
	query += ` ORDER BY created_at DESC, job_id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []domain.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, mapErr(err)
		}
		out = append(out, j)
	}
	return out, mapErr(rows.Err())
}
