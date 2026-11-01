package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
	"proxypool/internal/domain"
)

const driverName = "proxypool-sqlite"

var registerOnce sync.Once

func Open(path string, cacheKiB int) (*sql.DB, error) {
	registerOnce.Do(func() {
		sql.Register(driverName, &sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				pragmas := []string{
					"PRAGMA journal_mode = WAL",
					"PRAGMA foreign_keys = ON",
					"PRAGMA synchronous = FULL",
					fmt.Sprintf("PRAGMA cache_size = -%d", cacheKiB),
					"PRAGMA mmap_size = 0",
					"PRAGMA temp_store = FILE",
				}
				for _, p := range pragmas {
					if _, err := conn.Exec(p, nil); err != nil {
						return err
					}
				}
				return nil
			},
		})
	})
	dsn := fmt.Sprintf("file:%s?_txlock=immediate&_busy_timeout=1000", path)
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	var mode string
	if err := db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil {
		db.Close()
		return nil, err
	}
	if !strings.EqualFold(mode, "wal") {
		db.Close()
		return nil, fmt.Errorf("sqlite journal_mode is %q, want wal", mode)
	}
	return db, nil
}

type Store struct {
	db *sql.DB
}

func New(db *sql.DB) *Store { return &Store{db: db} }

func (s *Store) Close() error { return s.db.Close() }

// fmtTime uses a fixed-width fraction so lexicographic TEXT comparison in
// SQLite matches chronological order; RFC3339Nano trims trailing zeros and
// breaks ordering across the same second.
func fmtTime(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000000000Z07:00")
}

func nullTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return fmtTime(*t)
}

func scanTime(value any) (*time.Time, error) {
	if value == nil {
		return nil, nil
	}
	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("time column is %T, want string", value)
	}
	t, err := time.Parse(time.RFC3339Nano, str)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func mapErr(err error) error {
	var sqliteErr sqlite3.Error
	if errors.As(err, &sqliteErr) && sqliteErr.Code == sqlite3.ErrBusy {
		return fmt.Errorf("sqlite busy: %w", err)
	}
	return err
}

func marshalJSONDefault(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func (s *Store) tx(ctx context.Context, fn func(*sql.Tx) error) error {
	t, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return mapErr(err)
	}
	if err := fn(t); err != nil {
		_ = t.Rollback()
		return mapErr(err)
	}
	return mapErr(t.Commit())
}

const schemaV1 = `
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sources (
  source_id TEXT PRIMARY KEY,
  tenant TEXT NOT NULL DEFAULT 'default',
  adapter_type TEXT NOT NULL,
  parser_type TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  config_version INTEGER NOT NULL DEFAULT 1,
  config_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS proxy_credentials (
  credential_version TEXT PRIMARY KEY,
  username TEXT NOT NULL DEFAULT '',
  password TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS proxy_resources (
  resource_id TEXT PRIMARY KEY,
  tenant TEXT NOT NULL,
  transport TEXT NOT NULL,
  host TEXT NOT NULL,
  port INTEGER NOT NULL,
  credential_version TEXT NOT NULL DEFAULT '',
  session_policy TEXT NOT NULL DEFAULT 'static',
  resource_kind TEXT NOT NULL DEFAULT 'static_endpoint',
  disabled INTEGER NOT NULL DEFAULT 0,
  fence INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(tenant, transport, host, port, credential_version, session_policy)
);
CREATE TABLE IF NOT EXISTS source_offers (
  offer_id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL REFERENCES sources(source_id),
  resource_id TEXT NOT NULL REFERENCES proxy_resources(resource_id),
  source_record_id TEXT NOT NULL,
  source_generation TEXT NOT NULL,
  provider_expires_at TEXT,
  local_retention_deadline TEXT,
  expiry_basis TEXT NOT NULL DEFAULT 'unknown',
  claims_json TEXT NOT NULL DEFAULT '{}',
  max_pool_leases INTEGER NOT NULL DEFAULT 1,
  capacity_group TEXT NOT NULL DEFAULT '',
  state TEXT NOT NULL DEFAULT 'ENABLED',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(source_id, source_record_id, source_generation)
);
CREATE INDEX IF NOT EXISTS idx_offers_resource ON source_offers(resource_id);
CREATE INDEX IF NOT EXISTS idx_offers_state ON source_offers(state, provider_expires_at, local_retention_deadline);
CREATE TABLE IF NOT EXISTS scoped_health (
  resource_id TEXT NOT NULL,
  context_key TEXT NOT NULL,
  profile TEXT NOT NULL,
  consecutive_failures INTEGER NOT NULL DEFAULT 0,
  cooldown_until TEXT,
  last_outcome TEXT NOT NULL DEFAULT 'UNKNOWN',
  last_pass_at TEXT,
  failure_epoch INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (resource_id, context_key, profile)
);
CREATE TABLE IF NOT EXISTS reservations (
  reservation_id TEXT PRIMARY KEY,
  resource_id TEXT NOT NULL REFERENCES proxy_resources(resource_id),
  offer_id TEXT NOT NULL,
  context_key TEXT NOT NULL,
  owner TEXT NOT NULL,
  fence INTEGER NOT NULL,
  deadline TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'RESERVED',
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_res_resource ON reservations(resource_id, state, deadline);
CREATE TABLE IF NOT EXISTS leases (
  lease_id TEXT PRIMARY KEY,
  tenant TEXT NOT NULL,
  request_id TEXT NOT NULL,
  resource_id TEXT NOT NULL REFERENCES proxy_resources(resource_id),
  offer_id TEXT NOT NULL,
  context_key TEXT NOT NULL,
  mode TEXT NOT NULL DEFAULT 'exclusive',
  fence INTEGER NOT NULL,
  deadline TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'ACTIVE',
  connection_json TEXT NOT NULL,
  validation_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  released_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_leases_resource ON leases(resource_id, state, deadline);
CREATE TABLE IF NOT EXISTS validation_attempts (
  attempt_id TEXT PRIMARY KEY,
  request_id TEXT NOT NULL,
  resource_id TEXT NOT NULL,
  context_key TEXT NOT NULL,
  profile TEXT NOT NULL,
  outcome TEXT NOT NULL,
  started_at TEXT NOT NULL,
  completed_at TEXT NOT NULL,
  latency_ms INTEGER,
  detail_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_attempts_completed ON validation_attempts(completed_at);
CREATE TABLE IF NOT EXISTS lease_feedback (
  feedback_id TEXT PRIMARY KEY,
  lease_id TEXT NOT NULL,
  outcome TEXT NOT NULL,
  stage TEXT NOT NULL DEFAULT '',
  note TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS eviction_tombstones (
  tombstone_id TEXT PRIMARY KEY,
  resource_id TEXT NOT NULL,
  source_id TEXT NOT NULL,
  source_record_id TEXT NOT NULL DEFAULT '',
  source_generation TEXT NOT NULL DEFAULT '',
  reason TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(resource_id, source_id, source_record_id, source_generation, reason)
);
`

const schemaV2 = `
CREATE TABLE IF NOT EXISTS source_circuits (
  source_id TEXT PRIMARY KEY REFERENCES sources(source_id) ON DELETE CASCADE,
  state TEXT NOT NULL DEFAULT 'CLOSED',
  consecutive_failures INTEGER NOT NULL DEFAULT 0,
  open_count INTEGER NOT NULL DEFAULT 0,
  cooldown_until TEXT,
  last_outcome TEXT NOT NULL DEFAULT 'UNKNOWN',
  last_success_at TEXT,
  last_failure_at TEXT,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS resource_observations (
  resource_id TEXT PRIMARY KEY REFERENCES proxy_resources(resource_id) ON DELETE CASCADE,
  exit_ip TEXT NOT NULL,
  country_code TEXT NOT NULL DEFAULT '',
  country TEXT NOT NULL DEFAULT '',
  region TEXT NOT NULL DEFAULT '',
  city TEXT NOT NULL DEFAULT '',
  isp TEXT NOT NULL DEFAULT '',
  observed_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_observations_country ON resource_observations(country_code);
`

const schemaV3 = `
CREATE TABLE IF NOT EXISTS api_keys (
  key_id TEXT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  token_hash TEXT NOT NULL UNIQUE,
  token_prefix TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  default_cooldown_ms INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS consumer_proxy_cooldowns (
  consumer_id TEXT NOT NULL,
  resource_id TEXT NOT NULL,
  last_delivered_at TEXT NOT NULL,
  PRIMARY KEY (consumer_id, resource_id)
);
CREATE INDEX IF NOT EXISTS idx_consumer_cooldown_time
  ON consumer_proxy_cooldowns(consumer_id, last_delivered_at);
`

const schemaV4 = `
ALTER TABLE reservations ADD COLUMN consumer_id TEXT NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS idx_res_consumer
  ON reservations(consumer_id, resource_id, state, deadline);
`

const schemaV5 = `
ALTER TABLE leases ADD COLUMN consumer_id TEXT NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS idx_leases_consumer
  ON leases(consumer_id, resource_id, state, deadline);
`

func (s *Store) Migrate(ctx context.Context) error {
	var version int
	if err := s.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return mapErr(err)
	}
	switch {
	case version > 5:
		return fmt.Errorf("schema version %d is newer than supported version 5", version)
	case version == 0:
		err := s.tx(ctx, func(t *sql.Tx) error {
			if _, err := t.ExecContext(ctx, schemaV1); err != nil {
				return err
			}
			_, err := t.ExecContext(ctx, "PRAGMA user_version = 1")
			return err
		})
		if err != nil {
			return err
		}
		version = 1
	}
	if version == 1 {
		if err := s.tx(ctx, func(t *sql.Tx) error {
			if _, err := t.ExecContext(ctx, schemaV2); err != nil {
				return err
			}
			_, err := t.ExecContext(ctx, "PRAGMA user_version = 2")
			return err
		}); err != nil {
			return err
		}
		version = 2
	}
	if version == 2 {
		if err := s.tx(ctx, func(t *sql.Tx) error {
			if _, err := t.ExecContext(ctx, schemaV3); err != nil {
				return err
			}
			_, err := t.ExecContext(ctx, "PRAGMA user_version = 3")
			return err
		}); err != nil {
			return err
		}
		version = 3
	}
	if version == 3 {
		if err := s.tx(ctx, func(t *sql.Tx) error {
			if _, err := t.ExecContext(ctx, schemaV4); err != nil {
				return err
			}
			_, err := t.ExecContext(ctx, "PRAGMA user_version = 4")
			return err
		}); err != nil {
			return err
		}
		version = 4
	}
	if version == 4 {
		return s.tx(ctx, func(t *sql.Tx) error {
			if _, err := t.ExecContext(ctx, schemaV5); err != nil {
				return err
			}
			_, err := t.ExecContext(ctx, "PRAGMA user_version = 5")
			return err
		})
	}
	return nil
}

func (s *Store) UpsertSource(ctx context.Context, src domain.Source, now time.Time) error {
	return s.tx(ctx, func(t *sql.Tx) error {
		var version int64
		err := t.QueryRowContext(ctx,
			"SELECT config_version FROM sources WHERE source_id = ?", src.ID).Scan(&version)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			version = 0
		case err != nil:
			return err
		}
		version++
		_, err = t.ExecContext(ctx, `
			INSERT INTO sources(source_id, tenant, adapter_type, parser_type, enabled, config_version, config_json, created_at, updated_at)
			VALUES(?,?,?,?,?,?,?,?,?)
			ON CONFLICT(source_id) DO UPDATE SET
			  adapter_type = excluded.adapter_type,
			  parser_type = excluded.parser_type,
			  enabled = excluded.enabled,
			  config_version = excluded.config_version,
			  config_json = excluded.config_json,
			  updated_at = excluded.updated_at`,
			src.ID, src.Tenant, src.AdapterType, src.ParserType, boolInt(src.Enabled),
			version, src.ConfigJSON, fmtTime(now), fmtTime(now))
		return err
	})
}

func (s *Store) GetSource(ctx context.Context, id string) (domain.Source, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT source_id, tenant, adapter_type, parser_type, enabled, config_version, config_json
		FROM sources WHERE source_id = ?`, id)
	var src domain.Source
	var enabled int
	err := row.Scan(&src.ID, &src.Tenant, &src.AdapterType, &src.ParserType,
		&enabled, &src.ConfigVersion, &src.ConfigJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Source{}, domain.NotFound("source")
	}
	if err != nil {
		return domain.Source{}, mapErr(err)
	}
	src.Enabled = enabled == 1
	return src, nil
}
