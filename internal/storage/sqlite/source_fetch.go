package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"proxypool/internal/domain"
)

const sourceFetchSchema = `
CREATE TABLE IF NOT EXISTS source_fetch_state (
 source_id TEXT PRIMARY KEY REFERENCES sources(source_id) ON DELETE CASCADE,
 config_version INTEGER NOT NULL,
 last_started_at TEXT,
 last_success_at TEXT,
 next_fetch_at TEXT,
 next_refresh_at TEXT,
 consecutive_failures INTEGER NOT NULL DEFAULT 0,
 last_error_code TEXT NOT NULL DEFAULT '',
 fetched INTEGER NOT NULL DEFAULT 0,
 rejected INTEGER NOT NULL DEFAULT 0,
 retained INTEGER NOT NULL DEFAULT 0,
 duration_ms INTEGER NOT NULL DEFAULT 0,
 updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_source_fetch_due ON source_fetch_state(next_fetch_at);
`

type SourceFetchState struct {
	SourceID            string     `json:"source_id"`
	ConfigVersion       int64      `json:"config_version"`
	LastStartedAt       *time.Time `json:"last_started_at"`
	LastSuccessAt       *time.Time `json:"last_success_at"`
	NextFetchAt         *time.Time `json:"next_fetch_at"`
	NextRefreshAt       *time.Time `json:"next_refresh_at"`
	ConsecutiveFailures int        `json:"consecutive_failures"`
	LastErrorCode       string     `json:"last_error_code,omitempty"`
	Fetched             int        `json:"fetched"`
	Rejected            int        `json:"rejected"`
	Retained            int        `json:"retained"`
	DurationMS          int64      `json:"duration_ms"`
}

func (s *Store) SourceFetchState(ctx context.Context, sourceID string) (SourceFetchState, error) {
	out := SourceFetchState{SourceID: sourceID}
	var started, success, next, scheduled any
	err := s.db.QueryRowContext(ctx, `SELECT config_version,last_started_at,last_success_at,next_fetch_at,next_refresh_at,
 consecutive_failures,last_error_code,fetched,rejected,retained,duration_ms
	 FROM source_fetch_state WHERE source_id=?`, sourceID).Scan(&out.ConfigVersion, &started, &success, &next, &scheduled,
		&out.ConsecutiveFailures, &out.LastErrorCode, &out.Fetched, &out.Rejected, &out.Retained, &out.DurationMS)
	if errors.Is(err, sql.ErrNoRows) {
		return out, nil
	}
	if err != nil {
		return out, mapErr(err)
	}
	if out.LastStartedAt, err = scanTime(started); err != nil {
		return out, err
	}
	if out.LastSuccessAt, err = scanTime(success); err != nil {
		return out, err
	}
	out.NextFetchAt, err = scanTime(next)
	if err != nil {
		return out, err
	}
	out.NextRefreshAt, err = scanTime(scheduled)
	return out, err
}

// ClaimSourceFetch preserves real upstream rate windows over restarts and
// edits. A config edit invalidates candidates, not a vendor request that was
// already made under that source. Network I/O happens after this transaction.
func (s *Store) ClaimSourceFetch(ctx context.Context, sourceID string, version int64, now time.Time, interval time.Duration) error {
	return s.tx(ctx, func(t *sql.Tx) error {
		var enabled int
		var currentVersion int64
		if err := t.QueryRowContext(ctx, `SELECT enabled,config_version FROM sources WHERE source_id=?`, sourceID).Scan(&enabled, &currentVersion); err != nil {
			return err
		}
		if enabled != 1 || currentVersion != version {
			return ErrStale
		}
		var next any
		err := t.QueryRowContext(ctx, `SELECT next_fetch_at FROM source_fetch_state WHERE source_id=?`, sourceID).Scan(&next)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if next != nil {
			deadline, err := scanTime(next)
			if err != nil {
				return err
			}
			if deadline.After(now) {
				derr := domain.E("SOURCE_RATE_LIMITED", 429, "source %q cannot fetch again before %s", sourceID, deadline.UTC().Format(time.RFC3339))
				derr.RetryAfter = deadline.Sub(now)
				return derr
			}
		}
		_, err = t.ExecContext(ctx, `INSERT INTO source_fetch_state(source_id,config_version,last_started_at,next_fetch_at,updated_at)
 VALUES(?,?,?,?,?) ON CONFLICT(source_id) DO UPDATE SET
	consecutive_failures=CASE WHEN config_version!=excluded.config_version THEN 0 ELSE consecutive_failures END,
 config_version=excluded.config_version,last_started_at=excluded.last_started_at,next_fetch_at=excluded.next_fetch_at,updated_at=excluded.updated_at`,
			sourceID, version, fmtTime(now), fmtTime(now.Add(interval)), fmtTime(now))
		return err
	})
}

type SourceFetchOutcome struct {
	SourceID                    string
	ConfigVersion               int64
	StartedAt                   time.Time
	CompletedAt                 time.Time
	NextFetchAt                 time.Time
	NextRefreshAt               time.Time
	ErrorCode                   string
	Fetched, Rejected, Retained int
	DurationMS                  int64
}

// FinishSourceFetch rejects delayed writers by both source version and
// fetch start timestamp. A failed/stale result cannot overwrite a new run.
func (s *Store) FinishSourceFetch(ctx context.Context, o SourceFetchOutcome) error {
	var success any
	if o.ErrorCode == "" {
		success = fmtTime(o.CompletedAt)
	}
	res, err := s.db.ExecContext(ctx, `UPDATE source_fetch_state SET
 last_success_at=COALESCE(?,last_success_at),next_fetch_at=?,next_refresh_at=?,
 consecutive_failures=CASE WHEN ?='' THEN 0 ELSE consecutive_failures+1 END,
 last_error_code=?,fetched=?,rejected=?,retained=?,duration_ms=?,updated_at=?
 WHERE source_id=? AND config_version=? AND last_started_at=?
 AND EXISTS(SELECT 1 FROM sources WHERE source_id=? AND enabled=1 AND config_version=?)`,
		success, fmtTime(o.NextFetchAt), nullRefreshTime(o.NextRefreshAt), o.ErrorCode, o.ErrorCode, o.Fetched, o.Rejected, o.Retained, o.DurationMS, fmtTime(o.CompletedAt),
		o.SourceID, o.ConfigVersion, fmtTime(o.StartedAt), o.SourceID, o.ConfigVersion)
	if err != nil {
		return mapErr(err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrStale
	}
	return nil
}

// ListIntervalSources returns only list adapters with explicit scheduling.
// Earliest deadlines sort first so large catalogues are not starved by LIMIT.
func (s *Store) ListIntervalSources(ctx context.Context, limit int) ([]domain.Source, error) {
	if limit < 1 || limit > 64 {
		limit = 32
	}
	rows, err := s.db.QueryContext(ctx, `SELECT s.source_id,s.tenant,s.adapter_type,s.parser_type,s.enabled,s.config_version,s.config_json
 FROM sources s LEFT JOIN source_fetch_state f ON f.source_id=s.source_id
 WHERE s.enabled=1 AND s.adapter_type IN ('site_fetch','static_url','account_list','provider_api')
 AND json_extract(s.config_json,'$.refresh.mode')='interval'
	ORDER BY MAX(COALESCE(f.next_fetch_at,''),CASE WHEN f.config_version=s.config_version THEN COALESCE(f.next_refresh_at,'') ELSE '' END),s.source_id LIMIT ?`, limit)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	out := []domain.Source{}
	for rows.Next() {
		var src domain.Source
		if err := rows.Scan(&src.ID, &src.Tenant, &src.AdapterType, &src.ParserType, &src.Enabled, &src.ConfigVersion, &src.ConfigJSON); err != nil {
			return nil, err
		}
		out = append(out, src)
	}
	return out, rows.Err()
}

func nullRefreshTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return fmtTime(t)
}

func ensureSourceFetchSchemaTx(ctx context.Context, t *sql.Tx) error {
	if _, err := t.ExecContext(ctx, sourceFetchSchema); err != nil {
		return err
	}
	var n int
	if err := t.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('source_fetch_state') WHERE name='next_refresh_at'`).Scan(&n); err != nil {
		return err
	}
	if n == 0 {
		_, err := t.ExecContext(ctx, `ALTER TABLE source_fetch_state ADD COLUMN next_refresh_at TEXT`)
		return err
	}
	return nil
}
