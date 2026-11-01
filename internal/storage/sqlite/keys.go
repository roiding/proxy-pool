package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"proxypool/internal/domain"
)

func (s *Store) CreateAPIKey(ctx context.Context, key domain.APIKey, tokenHash string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO api_keys(key_id, name, token_hash, token_prefix, enabled,
		  default_cooldown_ms, created_at, updated_at)
		VALUES(?,?,?,?,?,?,?,?)`,
		key.ID, key.Name, tokenHash, key.TokenPrefix, boolInt(key.Enabled),
		key.DefaultCooldownMS, fmtTime(key.CreatedAt), fmtTime(key.UpdatedAt))
	return mapErr(err)
}

func (s *Store) ListAPIKeys(ctx context.Context) ([]domain.APIKey, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT key_id, name, token_prefix, enabled, default_cooldown_ms, created_at, updated_at
		FROM api_keys ORDER BY name`)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []domain.APIKey
	for rows.Next() {
		var k domain.APIKey
		var enabled int
		var created, updated string
		if err := rows.Scan(&k.ID, &k.Name, &k.TokenPrefix, &enabled,
			&k.DefaultCooldownMS, &created, &updated); err != nil {
			return nil, mapErr(err)
		}
		k.Enabled = enabled == 1
		if k.CreatedAt, err = time.Parse(time.RFC3339Nano, created); err != nil {
			return nil, err
		}
		if k.UpdatedAt, err = time.Parse(time.RFC3339Nano, updated); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, mapErr(rows.Err())
}

func (s *Store) APIKeyByHash(ctx context.Context, tokenHash string) (domain.APIKey, error) {
	var k domain.APIKey
	var enabled int
	var created, updated string
	err := s.db.QueryRowContext(ctx, `
		SELECT key_id, name, token_prefix, enabled, default_cooldown_ms, created_at, updated_at
		FROM api_keys WHERE token_hash = ? AND enabled = 1`, tokenHash).
		Scan(&k.ID, &k.Name, &k.TokenPrefix, &enabled, &k.DefaultCooldownMS, &created, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.APIKey{}, domain.NotFound("api key")
	}
	if err != nil {
		return domain.APIKey{}, mapErr(err)
	}
	k.Enabled = enabled == 1
	if k.CreatedAt, err = time.Parse(time.RFC3339Nano, created); err != nil {
		return domain.APIKey{}, err
	}
	if k.UpdatedAt, err = time.Parse(time.RFC3339Nano, updated); err != nil {
		return domain.APIKey{}, err
	}
	return k, nil
}

func (s *Store) UpdateAPIKey(ctx context.Context, id string, name *string,
	enabled *bool, defaultCooldownMS *int64, now time.Time) error {
	var enabledArg any
	if enabled != nil {
		enabledArg = boolInt(*enabled)
	}
	res, err := s.db.ExecContext(ctx, `
		UPDATE api_keys SET
		  name = COALESCE(?, name),
		  enabled = COALESCE(?, enabled),
		  default_cooldown_ms = COALESCE(?, default_cooldown_ms),
		  updated_at = ?
		WHERE key_id = ?`,
		name, enabledArg, defaultCooldownMS, fmtTime(now), id)
	if err != nil {
		return mapErr(err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return mapErr(err)
	}
	if n == 0 {
		return domain.NotFound("api key")
	}
	return nil
}

func (s *Store) DeleteAPIKey(ctx context.Context, id string) error {
	if _, err := s.db.ExecContext(ctx,
		`DELETE FROM consumer_proxy_cooldowns WHERE consumer_id = ?`, id); err != nil {
		return mapErr(err)
	}
	res, err := s.db.ExecContext(ctx, `DELETE FROM api_keys WHERE key_id = ?`, id)
	if err != nil {
		return mapErr(err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return mapErr(err)
	}
	if n == 0 {
		return domain.NotFound("api key")
	}
	return nil
}

// RecordConsumerDeliveries starts the per-key cooldown for proxies that were
// actually returned. It is called after the acquire result is final, so a
// failed request does not burn the consumer's cooldown window.
func (s *Store) RecordConsumerDeliveries(ctx context.Context, consumerID string,
	resourceIDs []string, now time.Time) error {
	if consumerID == "" || len(resourceIDs) == 0 {
		return nil
	}
	return s.tx(ctx, func(t *sql.Tx) error {
		for _, resourceID := range resourceIDs {
			if _, err := t.ExecContext(ctx, `
				INSERT INTO consumer_proxy_cooldowns(consumer_id, resource_id, last_delivered_at)
				VALUES(?,?,?)
				ON CONFLICT(consumer_id, resource_id) DO UPDATE SET
				  last_delivered_at = excluded.last_delivered_at`,
				consumerID, resourceID, fmtTime(now)); err != nil {
				return err
			}
		}
		return nil
	})
}
