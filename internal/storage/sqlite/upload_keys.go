package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"proxypool/internal/domain"
)

// ensureUploadKeySchemaTx creates the upload-key tables. It is invoked by
// every upload-key store method so an upgraded database self-heals without
// a dedicated migration version.
func ensureUploadKeySchemaTx(ctx context.Context, t *sql.Tx) error {
	_, err := t.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS upload_keys (
		  key_id TEXT PRIMARY KEY,
		  name TEXT NOT NULL UNIQUE,
		  token_hash TEXT NOT NULL UNIQUE,
		  token_prefix TEXT NOT NULL,
		  enabled INTEGER NOT NULL DEFAULT 1,
		  created_at TEXT NOT NULL,
		  updated_at TEXT NOT NULL
		);
		CREATE TABLE IF NOT EXISTS upload_key_sources (
		  key_id TEXT NOT NULL,
		  source_id TEXT NOT NULL,
		  PRIMARY KEY(key_id, source_id)
		);`)
	return err
}

// EnsureUploadKeySchema applies the upload-key schema to an open database.
func (s *Store) EnsureUploadKeySchema(ctx context.Context) error {
	return s.tx(ctx, func(t *sql.Tx) error {
		return ensureUploadKeySchemaTx(ctx, t)
	})
}

func (s *Store) CreateUploadKey(ctx context.Context, key domain.UploadKey, tokenHash string) error {
	return s.tx(ctx, func(t *sql.Tx) error {
		if err := ensureUploadKeySchemaTx(ctx, t); err != nil {
			return err
		}
		if _, err := t.ExecContext(ctx, `
			INSERT INTO upload_keys(key_id, name, token_hash, token_prefix, enabled,
			  created_at, updated_at)
			VALUES(?,?,?,?,?,?,?)`,
			key.ID, key.Name, tokenHash, key.TokenPrefix, boolInt(key.Enabled),
			fmtTime(key.CreatedAt), fmtTime(key.UpdatedAt)); err != nil {
			return err
		}
		for _, sourceID := range key.SourceIDs {
			if _, err := t.ExecContext(ctx, `
				INSERT INTO upload_key_sources(key_id, source_id) VALUES(?,?)`,
				key.ID, sourceID); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) ListUploadKeys(ctx context.Context) ([]domain.UploadKey, error) {
	if err := s.EnsureUploadKeySchema(ctx); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT key_id, name, token_prefix, enabled, created_at, updated_at
		FROM upload_keys ORDER BY name`)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []domain.UploadKey
	for rows.Next() {
		k, err := scanUploadKeyRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	if err := rows.Err(); err != nil {
		return nil, mapErr(err)
	}
	for i := range out {
		if out[i].SourceIDs, err = s.uploadKeySourceIDs(ctx, out[i].ID); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func scanUploadKeyRow(row interface{ Scan(...any) error }) (domain.UploadKey, error) {
	var k domain.UploadKey
	var enabled int
	var created, updated string
	if err := row.Scan(&k.ID, &k.Name, &k.TokenPrefix, &enabled, &created, &updated); err != nil {
		return domain.UploadKey{}, err
	}
	k.Enabled = enabled == 1
	var err error
	if k.CreatedAt, err = time.Parse(time.RFC3339Nano, created); err != nil {
		return domain.UploadKey{}, err
	}
	if k.UpdatedAt, err = time.Parse(time.RFC3339Nano, updated); err != nil {
		return domain.UploadKey{}, err
	}
	return k, nil
}

func (s *Store) uploadKeySourceIDs(ctx context.Context, keyID string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT source_id FROM upload_key_sources WHERE key_id = ? ORDER BY source_id`, keyID)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, mapErr(err)
		}
		out = append(out, id)
	}
	return out, mapErr(rows.Err())
}

// UploadKeyByHash resolves an upload credential to its record. Disabled keys
// never authenticate; the enabled filter fails closed inside the lookup.
func (s *Store) UploadKeyByHash(ctx context.Context, tokenHash string) (domain.UploadKey, error) {
	if err := s.EnsureUploadKeySchema(ctx); err != nil {
		return domain.UploadKey{}, err
	}
	row := s.db.QueryRowContext(ctx, `
		SELECT key_id, name, token_prefix, enabled, created_at, updated_at
		FROM upload_keys WHERE token_hash = ? AND enabled = 1`, tokenHash)
	k, err := scanUploadKeyRow(row)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.UploadKey{}, domain.NotFound("upload key")
	}
	if err != nil {
		return domain.UploadKey{}, mapErr(err)
	}
	if k.SourceIDs, err = s.uploadKeySourceIDs(ctx, k.ID); err != nil {
		return domain.UploadKey{}, err
	}
	return k, nil
}

func (s *Store) UpdateUploadKey(ctx context.Context, id string, name *string,
	enabled *bool, now time.Time) error {
	if err := s.EnsureUploadKeySchema(ctx); err != nil {
		return err
	}
	var enabledArg any
	if enabled != nil {
		enabledArg = boolInt(*enabled)
	}
	res, err := s.db.ExecContext(ctx, `
		UPDATE upload_keys SET
		  name = COALESCE(?, name),
		  enabled = COALESCE(?, enabled),
		  updated_at = ?
		WHERE key_id = ?`,
		name, enabledArg, fmtTime(now), id)
	if err != nil {
		return mapErr(err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return mapErr(err)
	}
	if n == 0 {
		return domain.NotFound("upload key")
	}
	return nil
}

func (s *Store) DeleteUploadKey(ctx context.Context, id string) error {
	return s.tx(ctx, func(t *sql.Tx) error {
		if err := ensureUploadKeySchemaTx(ctx, t); err != nil {
			return err
		}
		if _, err := t.ExecContext(ctx,
			`DELETE FROM upload_key_sources WHERE key_id = ?`, id); err != nil {
			return err
		}
		res, err := t.ExecContext(ctx, `DELETE FROM upload_keys WHERE key_id = ?`, id)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return domain.NotFound("upload key")
		}
		return nil
	})
}

// UploadKeyActiveForSource re-checks, as late as possible before an apply,
// that the key still exists, is still enabled, and is still bound to the
// source. Authentication snapshots are request-start facts; this check is
// the fail-closed revalidation for the authorization that actually applies
// the batch.
func (s *Store) UploadKeyActiveForSource(ctx context.Context, keyID, sourceID string) (bool, error) {
	if err := s.EnsureUploadKeySchema(ctx); err != nil {
		return false, err
	}
	var active int
	err := s.db.QueryRowContext(ctx, `
		SELECT EXISTS(
		  SELECT 1 FROM upload_keys k
		  JOIN upload_key_sources s ON s.key_id = k.key_id
		  WHERE k.key_id = ? AND k.enabled = 1 AND s.source_id = ?)`,
		keyID, sourceID).Scan(&active)
	if err != nil {
		return false, mapErr(err)
	}
	return active == 1, nil
}
