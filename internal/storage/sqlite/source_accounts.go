package sqlite

import (
	"context"
	"database/sql"
	"time"

	"proxypool/internal/domain"
)

func ensureSourceAccountsSchemaTx(ctx context.Context, t *sql.Tx) error {
	_, err := t.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS source_accounts (
 source_id TEXT PRIMARY KEY REFERENCES sources(source_id) ON DELETE CASCADE,
 secret_version INTEGER NOT NULL,
 ciphertext TEXT NOT NULL DEFAULT '',
 updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS account_fetch_limits (
 credential_key TEXT PRIMARY KEY,
 next_fetch_at TEXT NOT NULL
);`)
	return err
}

// The key is an application HMAC, never an account token. Multiple source
// instances with the SAME credential share this read-API rate floor across
// process restarts. No purchasing/quota model is inferred from this table.
func (s *Store) ClaimAccountFetch(ctx context.Context, key string, now time.Time, interval time.Duration) error {
	return s.tx(ctx, func(t *sql.Tx) error {
		if _, err := t.ExecContext(ctx, `DELETE FROM account_fetch_limits WHERE next_fetch_at<=?`, fmtTime(now)); err != nil {
			return err
		}
		var raw any
		err := t.QueryRowContext(ctx, `SELECT next_fetch_at FROM account_fetch_limits WHERE credential_key=?`, key).Scan(&raw)
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		if raw != nil {
			at, err := scanTime(raw)
			if err != nil {
				return err
			}
			if at.After(now) {
				derr := domain.E("SOURCE_RATE_LIMITED", 429, "account credential was fetched recently; retry after its shared rate window")
				derr.RetryAfter = at.Sub(now)
				return derr
			}
		}
		_, err = t.ExecContext(ctx, `INSERT INTO account_fetch_limits(credential_key,next_fetch_at) VALUES(?,?) ON CONFLICT(credential_key) DO UPDATE SET next_fetch_at=excluded.next_fetch_at`, key, fmtTime(now.Add(interval)))
		return err
	})
}

func (s *Store) ExtendAccountFetchLimit(ctx context.Context, key string, deadline time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE account_fetch_limits SET next_fetch_at=MAX(next_fetch_at,?) WHERE credential_key=?`, fmtTime(deadline), key)
	return mapErr(err)
}

// SourceAccountRecord is an internal persistence model. Do not serialize it
// into an admin response: only the application status view is public.
type SourceAccountRecord struct {
	SourceID      string
	ConfigVersion int64
	SecretVersion int64
	Ciphertext    string
	UpdatedAt     *time.Time
}

func (s *Store) SourceAccount(ctx context.Context, id string) (SourceAccountRecord, error) {
	out := SourceAccountRecord{SourceID: id}
	var updated any
	err := s.db.QueryRowContext(ctx, `SELECT s.config_version,COALESCE(a.secret_version,0),COALESCE(a.ciphertext,''),a.updated_at
 FROM sources s LEFT JOIN source_accounts a ON a.source_id=s.source_id WHERE s.source_id=?`, id).
		Scan(&out.ConfigVersion, &out.SecretVersion, &out.Ciphertext, &updated)
	if err == sql.ErrNoRows {
		return out, domain.NotFound("source")
	}
	if err != nil {
		return out, mapErr(err)
	}
	out.UpdatedAt, err = scanTime(updated)
	return out, err
}

// ReplaceSourceSecret CASes the encrypted secret and source definition in
// the same transaction. A rotation/clear invalidates every previous batch,
// proof and sticky-source version without touching project CD windows.
func (s *Store) ReplaceSourceSecret(ctx context.Context, id string, expectedConfig, expectedSecret int64, ciphertext string, now time.Time) error {
	return s.tx(ctx, func(t *sql.Tx) error {
		if err := ensureSupplySchemaTx(ctx, t); err != nil {
			return err
		}
		var adapter string
		var configVersion, secretVersion int64
		if err := t.QueryRowContext(ctx, `SELECT s.adapter_type,s.config_version,COALESCE(a.secret_version,0)
 FROM sources s LEFT JOIN source_accounts a ON a.source_id=s.source_id WHERE s.source_id=?`, id).
			Scan(&adapter, &configVersion, &secretVersion); err != nil {
			if err == sql.ErrNoRows {
				return domain.NotFound("source")
			}
			return err
		}
		if adapter != "account_list" {
			return domain.InvalidRequest("only account_list sources have account secrets")
		}
		if expectedConfig <= 0 || configVersion != expectedConfig || secretVersion != expectedSecret {
			return domain.E("SOURCE_VERSION_CONFLICT", 409, "source changed; reload its configuration version")
		}
		next, err := nextSourceVersionTx(ctx, t, id)
		if err != nil {
			return err
		}
		if _, err := t.ExecContext(ctx, `UPDATE sources SET config_version=?,updated_at=? WHERE source_id=? AND config_version=?`, next, fmtTime(now), id, expectedConfig); err != nil {
			return err
		}
		_, err = t.ExecContext(ctx, `INSERT INTO source_accounts(source_id,secret_version,ciphertext,updated_at) VALUES(?,?,?,?)
 ON CONFLICT(source_id) DO UPDATE SET secret_version=excluded.secret_version,ciphertext=excluded.ciphertext,updated_at=excluded.updated_at`, id, secretVersion+1, ciphertext, fmtTime(now))
		return err
	})
}
