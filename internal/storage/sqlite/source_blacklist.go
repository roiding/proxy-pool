package sqlite

import (
	"context"
	"database/sql"
	"time"

	"proxypool/internal/domain"
)

const DefaultNetworkReuseCooldown = 30 * time.Minute

func ensureSourceBlacklistSchemaTx(ctx context.Context, t *sql.Tx) error {
	_, err := t.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS source_proxy_blacklist (
	  source_id TEXT NOT NULL REFERENCES sources(source_id) ON DELETE CASCADE,
	  resource_id TEXT NOT NULL,
	  unavailable_until TEXT NOT NULL,
	  reason TEXT NOT NULL,
	  updated_at TEXT NOT NULL,
	  PRIMARY KEY(source_id, resource_id)
	);
	CREATE INDEX IF NOT EXISTS idx_source_blacklist_until ON source_proxy_blacklist(unavailable_until);`)
	if err != nil {
		return err
	}
	var hasProjects int
	if err := t.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='projects'`).Scan(&hasProjects); err != nil {
		return err
	}
	if hasProjects == 0 {
		return nil
	}
	var exists int
	if err := t.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('projects')
	  WHERE name = 'network_reuse_cooldown_ms'`).Scan(&exists); err != nil {
		return err
	}
	if exists == 0 {
		_, err = t.ExecContext(ctx, `ALTER TABLE projects ADD COLUMN network_reuse_cooldown_ms INTEGER NOT NULL DEFAULT 1800000`)
	}
	return err
}

func sourceProxyBlockedTx(ctx context.Context, t *sql.Tx, sourceID, fp string, now time.Time) (bool, error) {
	var n int
	err := t.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM source_proxy_blacklist
	  WHERE source_id=? AND resource_id=? AND unavailable_until>?)`, sourceID, fp, fmtTime(now)).Scan(&n)
	return n != 0, err
}

func (s *Store) SourceProxyBlocked(ctx context.Context, sourceID, fp string, now time.Time) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM source_proxy_blacklist
	  WHERE source_id=? AND resource_id=? AND unavailable_until>?)`, sourceID, fp, fmtTime(now)).Scan(&n)
	return n != 0, mapErr(err)
}

// projectNetworkCooldownTx reads the current downstream setting at commit.
// Missing/implicit consumers use the 30-minute default. Existing blacklist
// windows never shrink when a project subsequently lowers its setting.
func projectNetworkCooldownTx(ctx context.Context, t *sql.Tx, projectID string) (time.Duration, error) {
	var ms int64
	err := t.QueryRowContext(ctx, `SELECT network_reuse_cooldown_ms FROM projects WHERE project_id=?`, projectID).Scan(&ms)
	if err == sql.ErrNoRows {
		return DefaultNetworkReuseCooldown, nil
	}
	if err != nil {
		return 0, err
	}
	if ms <= 0 {
		return DefaultNetworkReuseCooldown, nil
	}
	return time.Duration(ms) * time.Millisecond, nil
}

func markSourceProxyTx(ctx context.Context, t *sql.Tx, sourceID, fp, projectID, reason string, now time.Time) error {
	d, err := projectNetworkCooldownTx(ctx, t, projectID)
	if err != nil {
		return err
	}
	_, err = t.ExecContext(ctx, `INSERT INTO source_proxy_blacklist
	  (source_id,resource_id,unavailable_until,reason,updated_at) VALUES(?,?,?,?,?)
	  ON CONFLICT(source_id,resource_id) DO UPDATE SET
	  unavailable_until=MAX(source_proxy_blacklist.unavailable_until,excluded.unavailable_until),
	  reason=excluded.reason, updated_at=excluded.updated_at`, sourceID, fp, fmtTime(now.Add(d)), reason, fmtTime(now))
	return err
}

func (s *Store) MarkSourceProxyUnavailable(ctx context.Context, c domain.Candidate, fp, projectID, reason string, now time.Time) error {
	if c.Resource.ID != "" {
		fp = c.Resource.ID
	}
	return s.tx(ctx, func(t *sql.Tx) error {
		var live int
		if err := t.QueryRowContext(ctx, `SELECT COUNT(*) FROM sources WHERE source_id=?
		  AND enabled=1 AND config_version=?`, c.Offer.SourceID, c.SourceConfigVersion).Scan(&live); err != nil {
			return err
		}
		if live != 1 {
			return ErrStale
		}
		return markSourceProxyTx(ctx, t, c.Offer.SourceID, fp, projectID, reason, now)
	})
}

func (s *Store) SourceBlacklist(ctx context.Context, id string, now time.Time) (map[string]bool, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT resource_id FROM source_proxy_blacklist
	  WHERE source_id=? AND unavailable_until>?`, id, fmtTime(now))
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var fp string
		if err := rows.Scan(&fp); err != nil {
			return nil, err
		}
		out[fp] = true
	}
	return out, rows.Err()
}

// BootstrapSourceBlacklist imports recent old-version failures and actual
// delivered leases once. Resource IDs are stable HMAC connection identities,
// so historical raw proxy credentials need not be recovered or persisted.
func (s *Store) BootstrapSourceBlacklist(ctx context.Context, now time.Time) error {
	return s.tx(ctx, func(t *sql.Tx) error {
		if _, err := t.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS source_blacklist_migrations (name TEXT PRIMARY KEY)`); err != nil {
			return err
		}
		var done int
		if err := t.QueryRowContext(ctx, `SELECT COUNT(*) FROM source_blacklist_migrations WHERE name='legacy_once'`).Scan(&done); err != nil {
			return err
		}
		if done != 0 {
			return nil
		}
		cutoff := fmtTime(now.Add(-DefaultNetworkReuseCooldown))
		rows, err := t.QueryContext(ctx, `SELECT a.source_id,v.resource_id,a.completed_at,'validation_failed'
		 FROM validation_attempt_sources a JOIN validation_attempts v ON v.attempt_id=a.attempt_id
		 JOIN sources s ON s.source_id=a.source_id AND s.config_version=a.source_config_version
		 WHERE a.outcome='FAIL' AND a.completed_at>? AND v.resource_id<>''
		 AND s.adapter_type IN ('site_fetch','static_url','account_list','provider_api')
		 UNION ALL
		 SELECT a.source_id,l.resource_id,l.created_at,'delivered'
		 FROM leases l JOIN validation_attempt_sources a ON a.attempt_id=json_extract(
		   CASE WHEN json_valid(l.validation_json) THEN l.validation_json ELSE '{}' END,'$.attempt_id')
		 JOIN sources s ON s.source_id=a.source_id AND s.config_version=a.source_config_version
		 WHERE l.created_at>? AND l.resource_id<>''
		 AND s.adapter_type IN ('site_fetch','static_url','account_list','provider_api') LIMIT 10001`, cutoff, cutoff)
		if err != nil {
			return err
		}
		type oldWindow struct{ source, id, at, reason string }
		var windows []oldWindow
		for rows.Next() {
			var w oldWindow
			if err := rows.Scan(&w.source, &w.id, &w.at, &w.reason); err != nil {
				rows.Close()
				return err
			}
			windows = append(windows, w)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if len(windows) > 10000 {
			return domain.E("BLACKLIST_MIGRATION_LIMIT", 503, "too many recent legacy records; migration cannot safely truncate")
		}
		for _, w := range windows {
			at, err := time.Parse(time.RFC3339Nano, w.at)
			if err != nil {
				return err
			}
			until := at.Add(DefaultNetworkReuseCooldown)
			if at.After(now) || !until.After(now) {
				continue
			}
			if _, err := t.ExecContext(ctx, `INSERT INTO source_proxy_blacklist(source_id,resource_id,unavailable_until,reason,updated_at)
			 VALUES(?,?,?,?,?) ON CONFLICT(source_id,resource_id) DO UPDATE SET
			 unavailable_until=MAX(source_proxy_blacklist.unavailable_until,excluded.unavailable_until)`, w.source, w.id, fmtTime(until), w.reason, fmtTime(now)); err != nil {
				return err
			}
		}
		_, err = t.ExecContext(ctx, `INSERT INTO source_blacklist_migrations(name) VALUES('legacy_once')`)
		return err
	})
}
