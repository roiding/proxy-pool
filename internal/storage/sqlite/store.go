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

// DB exposes the raw handle for diagnostics and test fixtures; production
// code must go through typed methods.
func (s *Store) DB() *sql.DB { return s.db }

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

// schemaV6 detaches leases/reservations from the static inventory so
// on-demand API deliveries can key on the connection fingerprint without
// inventory rows, and revokes legacy provider-API offers (they are fetched
// per request now, never imported).
const schemaV6 = `
UPDATE source_offers SET state = 'REVOKED'
WHERE source_id IN (SELECT source_id FROM sources WHERE adapter_type = 'provider_api');
CREATE TABLE leases_v6 (
  lease_id TEXT PRIMARY KEY,
  tenant TEXT NOT NULL,
  request_id TEXT NOT NULL,
  resource_id TEXT NOT NULL,
  offer_id TEXT NOT NULL,
  context_key TEXT NOT NULL,
  mode TEXT NOT NULL DEFAULT 'exclusive',
  fence INTEGER NOT NULL,
  deadline TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'ACTIVE',
  connection_json TEXT NOT NULL,
  validation_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  released_at TEXT,
  consumer_id TEXT NOT NULL DEFAULT ''
);
INSERT INTO leases_v6 SELECT lease_id, tenant, request_id, resource_id, offer_id, context_key,
  mode, fence, deadline, state, connection_json, validation_json, created_at, released_at, consumer_id
FROM leases;
DROP TABLE leases;
ALTER TABLE leases_v6 RENAME TO leases;
CREATE INDEX idx_leases_resource ON leases(resource_id, state, deadline);
CREATE INDEX idx_leases_consumer ON leases(consumer_id, resource_id, state, deadline);
CREATE TABLE reservations_v6 (
  reservation_id TEXT PRIMARY KEY,
  resource_id TEXT NOT NULL,
  offer_id TEXT NOT NULL,
  context_key TEXT NOT NULL,
  owner TEXT NOT NULL,
  fence INTEGER NOT NULL,
  deadline TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'RESERVED',
  created_at TEXT NOT NULL,
  consumer_id TEXT NOT NULL DEFAULT ''
);
INSERT INTO reservations_v6 SELECT reservation_id, resource_id, offer_id, context_key,
  owner, fence, deadline, state, created_at, consumer_id
FROM reservations;
DROP TABLE reservations;
ALTER TABLE reservations_v6 RENAME TO reservations;
CREATE INDEX idx_res_resource ON reservations(resource_id, state, deadline);
CREATE INDEX idx_res_consumer ON reservations(consumer_id, resource_id, state, deadline);
`

func (s *Store) Migrate(ctx context.Context) error {
	warnings, err := s.MigrateWithOptions(ctx, MigrateOptions{})
	_ = warnings
	return err
}

// MigrateOptions carries deployment inputs the database cannot derive on its
// own. LegacyMaxCooldown is the configured maximum downstream cooldown:
// frozen v6 rows only recorded last_delivered_at, so legacy cooldowns are
// conservatively rebased to last_delivery + LegacyMaxCooldown and are never
// dropped or zeroed.
type MigrateOptions struct {
	IdentitySecret    string
	LegacyMaxCooldown time.Duration
}

// MigrateWithOptions advances the schema to the current version and returns
// non-fatal migration warnings alongside the error.
func (s *Store) MigrateWithOptions(ctx context.Context, opts MigrateOptions) ([]string, error) {
	var version int
	if err := s.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return nil, mapErr(err)
	}
	switch {
	case version > 7:
		return nil, fmt.Errorf("schema version %d is newer than supported version 7", version)
	case version == 0:
		err := s.tx(ctx, func(t *sql.Tx) error {
			if _, err := t.ExecContext(ctx, schemaV1); err != nil {
				return err
			}
			_, err := t.ExecContext(ctx, "PRAGMA user_version = 1")
			return err
		})
		if err != nil {
			return nil, err
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
			return nil, err
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
			return nil, err
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
			return nil, err
		}
		version = 4
	}
	if version == 4 {
		if err := s.tx(ctx, func(t *sql.Tx) error {
			if _, err := t.ExecContext(ctx, schemaV5); err != nil {
				return err
			}
			_, err := t.ExecContext(ctx, "PRAGMA user_version = 5")
			return err
		}); err != nil {
			return nil, err
		}
		version = 5
	}
	if version == 5 {
		err := s.tx(ctx, func(t *sql.Tx) error {
			if _, err := t.ExecContext(ctx, schemaV6); err != nil {
				return err
			}
			_, err := t.ExecContext(ctx, "PRAGMA user_version = 6")
			return err
		})
		if err != nil {
			return nil, err
		}
		version = 6
	}
	if version == 6 {
		if warnings, err := s.migrateV7(ctx, opts); err != nil {
			return nil, err
		} else if len(warnings) > 0 {
			// Ensure always runs below, including on a freshly migrated DB.
			if err := s.tx(ctx, func(t *sql.Tx) error {
				return ensureSupplySchemaTx(ctx, t)
			}); err != nil {
				return nil, err
			}
			return warnings, nil
		}
	}
	if err := s.tx(ctx, func(t *sql.Tx) error {
		return ensureSupplySchemaTx(ctx, t)
	}); err != nil {
		return nil, err
	}
	if version >= 7 {
		// Fresh v6->v7 already ran this above; an idempotent second pass keeps
		// the branch uniform and harmless.
		if err := s.ensureV7AdditiveColumns(ctx); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

const schemaV7 = `
CREATE TABLE IF NOT EXISTS projects (
  project_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  cooldown_ms INTEGER NOT NULL DEFAULT 0,
  timeout_ms INTEGER NOT NULL DEFAULT 0,
  validation_json TEXT NOT NULL DEFAULT '{}',
  allowed_source_ids TEXT,
  config_version INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS project_proxy_cd (
  project_id TEXT NOT NULL,
  connection_fingerprint TEXT NOT NULL,
  issued_at TEXT NOT NULL,
  blocked_until TEXT NOT NULL,
  PRIMARY KEY (project_id, connection_fingerprint)
);
CREATE INDEX IF NOT EXISTS idx_project_cd_blocked ON project_proxy_cd(blocked_until);
CREATE TABLE IF NOT EXISTS sticky_bindings (
  project_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  version INTEGER NOT NULL DEFAULT 1,
  owner TEXT NOT NULL DEFAULT '',
  fingerprint TEXT NOT NULL,
  source_id TEXT NOT NULL DEFAULT '',
  source_config_version INTEGER NOT NULL DEFAULT 0,
  resource_id TEXT NOT NULL DEFAULT '',
  incarnation TEXT NOT NULL DEFAULT '',
  connection_ciphertext TEXT NOT NULL DEFAULT '',
  validation_json TEXT NOT NULL DEFAULT '{}',
  deadline TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'ACTIVE',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (project_id, session_id)
);
CREATE INDEX IF NOT EXISTS idx_sticky_state_deadline ON sticky_bindings(state, deadline);
CREATE TABLE IF NOT EXISTS jobs (
  job_id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'PENDING',
  params_json TEXT NOT NULL DEFAULT '{}',
  target INTEGER NOT NULL DEFAULT 0,
  ready INTEGER NOT NULL DEFAULT 0,
  probed INTEGER NOT NULL DEFAULT 0,
  failed INTEGER NOT NULL DEFAULT 0,
  stop_reason TEXT NOT NULL DEFAULT '',
  last_error TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  started_at TEXT,
  finished_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_jobs_project ON jobs(project_id, created_at);
CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state);
CREATE TABLE IF NOT EXISTS validation_evidence (
  connection_fingerprint TEXT NOT NULL,
  scope_hash TEXT NOT NULL,
  source_id TEXT NOT NULL DEFAULT '',
  source_config_version INTEGER NOT NULL DEFAULT 0,
  incarnation TEXT NOT NULL DEFAULT '',
  evidence_json TEXT NOT NULL,
  outcome TEXT NOT NULL,
  completed_at TEXT NOT NULL,
  PRIMARY KEY (connection_fingerprint, scope_hash)
);
`

const schemaV7APIKeyProject = `
ALTER TABLE api_keys ADD COLUMN project_id TEXT NOT NULL DEFAULT '';
`

const schemaV7ReservationSourceVersion = `
ALTER TABLE reservations ADD COLUMN source_config_version INTEGER NOT NULL DEFAULT 0;
`

const schemaV7LeaseKeyID = `
ALTER TABLE leases ADD COLUMN key_id TEXT NOT NULL DEFAULT '';
`

const schemaV7ReservationKeyID = `
ALTER TABLE reservations ADD COLUMN key_id TEXT NOT NULL DEFAULT '';
`

const schemaV7StickyOwner = `
ALTER TABLE sticky_bindings ADD COLUMN owner TEXT NOT NULL DEFAULT '';
`

const schemaV7StickyObject = `
ALTER TABLE sticky_bindings ADD COLUMN resource_id TEXT NOT NULL DEFAULT '';
`

const schemaV7StickyIncarnation = `
ALTER TABLE sticky_bindings ADD COLUMN incarnation TEXT NOT NULL DEFAULT '';
`

// schemaV7ResourceIncarnation gives every fresh resource row a nonce so
// health/attempt writes from probes that started before a delete can never
// reattach (and resurrect state) after the same connection is re-imported.
const schemaV7ResourceIncarnation = `
ALTER TABLE proxy_resources ADD COLUMN incarnation TEXT NOT NULL DEFAULT '';
`

// ensureV7AdditiveColumns repairs databases stamped at v7 before later
// additive columns/tables landed. It is idempotent and never advances the
// durable schema version, so a restart can safely run it on every boot.
func (s *Store) ensureV7AdditiveColumns(ctx context.Context) error {
	return s.tx(ctx, func(t *sql.Tx) error {
		columns := []struct{ table, column, ddl string }{
			{"api_keys", "project_id", schemaV7APIKeyProject},
			{"reservations", "source_config_version", schemaV7ReservationSourceVersion},
			{"leases", "key_id", schemaV7LeaseKeyID},
			{"reservations", "key_id", schemaV7ReservationKeyID},
			{"sticky_bindings", "owner", schemaV7StickyOwner},
			{"sticky_bindings", "resource_id", schemaV7StickyObject},
			{"sticky_bindings", "incarnation", schemaV7StickyIncarnation},
			{"proxy_resources", "incarnation", schemaV7ResourceIncarnation},
		}
		for _, col := range columns {
			var count int
			if err := t.QueryRowContext(ctx,
				`SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = ?`,
				col.table, col.column).Scan(&count); err != nil {
				return err
			}
			if count == 0 {
				if _, err := t.ExecContext(ctx, col.ddl); err != nil {
					return err
				}
			}
		}
		return ensureSupplySchemaTx(ctx, t)
	})
}

// defaultLegacyMaxCooldown is the conservative fallback when the caller does
// not provide one; it matches the shipped max_downstream_cooldown_ms.
const defaultLegacyMaxCooldown = 7 * 24 * time.Hour

// projectWideCooldownFingerprint is the wildcard row that holds a whole
// project in cooldown; legacy rows that cannot be mapped onto a connection
// fingerprint are converted into it so the new selector can never silently
// ignore a window the old consumer table recorded.
const projectWideCooldownFingerprint = ""

func (s *Store) migrateV7(ctx context.Context, opts MigrateOptions) ([]string, error) {
	now := time.Now().UTC()
	var warnings []string
	err := s.tx(ctx, func(t *sql.Tx) error {
		if _, err := t.ExecContext(ctx, schemaV7); err != nil {
			return err
		}
		var hasProjectID int
		if err := t.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM pragma_table_info('api_keys') WHERE name = 'project_id'`).
			Scan(&hasProjectID); err != nil {
			return err
		}
		if hasProjectID == 0 {
			if _, err := t.ExecContext(ctx, schemaV7APIKeyProject); err != nil {
				return err
			}
		}
		var hasResVersion int
		if err := t.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM pragma_table_info('reservations')
			WHERE name = 'source_config_version'`).Scan(&hasResVersion); err != nil {
			return err
		}
		if hasResVersion == 0 {
			if _, err := t.ExecContext(ctx, schemaV7ReservationSourceVersion); err != nil {
				return err
			}
		}
		for _, col := range []struct{ table, column, ddl string }{
			{"leases", "key_id", schemaV7LeaseKeyID},
			{"reservations", "key_id", schemaV7ReservationKeyID},
			{"sticky_bindings", "owner", schemaV7StickyOwner},
			{"sticky_bindings", "resource_id", schemaV7StickyObject},
			{"sticky_bindings", "incarnation", schemaV7StickyIncarnation},
		} {
			var hasCol int
			if err := t.QueryRowContext(ctx,
				`SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = ?`, col.table, col.column).
				Scan(&hasCol); err != nil {
				return err
			}
			if hasCol == 0 {
				if _, err := t.ExecContext(ctx, col.ddl); err != nil {
					return err
				}
			}
		}
		var hasIncarnation int
		if err := t.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM pragma_table_info('proxy_resources')
			WHERE name = 'incarnation'`).Scan(&hasIncarnation); err != nil {
			return err
		}
		if hasIncarnation == 0 {
			if _, err := t.ExecContext(ctx, schemaV7ResourceIncarnation); err != nil {
				return err
			}
		}
		stamp := fmtTime(now)
		if _, err := t.ExecContext(ctx, `
			INSERT INTO projects(project_id, name, enabled, cooldown_ms, timeout_ms,
			  validation_json, allowed_source_ids, config_version, created_at, updated_at)
			VALUES(?, 'Default', 1, 0, 0, '{}', NULL, 1, ?, ?)
			ON CONFLICT(project_id) DO NOTHING`, DefaultProjectID, stamp, stamp); err != nil {
			return err
		}
		// Every legacy key becomes its own project: distinct businesses must
		// never share project CD because keys were rotated or re-created.
		rows, err := t.QueryContext(ctx,
			`SELECT key_id, name, default_cooldown_ms FROM api_keys WHERE project_id = ''`)
		if err != nil {
			return err
		}
		type legacyKey struct {
			id string
			cd int64
		}
		var legacyKeys []legacyKey
		for rows.Next() {
			var k legacyKey
			if err := rows.Scan(&k.id, new(string), &k.cd); err != nil {
				rows.Close()
				return err
			}
			legacyKeys = append(legacyKeys, k)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		for _, k := range legacyKeys {
			projectID := legacyProjectID(k.id)
			if _, err := t.ExecContext(ctx, `
				INSERT INTO projects(project_id, name, enabled, cooldown_ms, timeout_ms,
				  validation_json, allowed_source_ids, config_version, created_at, updated_at)
				VALUES(?, ?, 1, ?, 0, '{}', NULL, 1, ?, ?)
				ON CONFLICT(project_id) DO NOTHING`, projectID, k.id, k.cd, stamp, stamp); err != nil {
				return err
			}
			if _, err := t.ExecContext(ctx,
				`UPDATE api_keys SET project_id = ?, updated_at = ? WHERE key_id = ?`,
				projectID, stamp, k.id); err != nil {
				return err
			}
		}
		if len(legacyKeys) > 0 {
			warnings = append(warnings, fmt.Sprintf(
				"migrated %d legacy api key(s) into one project each; original per-key cooldown values were unknown", len(legacyKeys)))
		}
		if err := s.migrateLegacyCooldowns(ctx, t, opts, now, &warnings); err != nil {
			return err
		}
		// Supply attribution is an additive idempotent table set. It lives in
		// the same successful migration transaction so fresh and upgraded
		// databases both expose real stats without a fixture-only step.
		if err := ensureSupplySchemaTx(ctx, t); err != nil {
			return err
		}
		_, err = t.ExecContext(ctx, "PRAGMA user_version = 7")
		return err
	})
	if err != nil {
		return nil, err
	}
	return warnings, nil
}

// migrateLegacyCooldowns rebases frozen last_delivered_at rows into fixed
// project_proxy_cd windows. Rows without a matching inventory resource cannot
// be fingerprinted and are kept (and warned about) rather than dropped.
func (s *Store) migrateLegacyCooldowns(ctx context.Context, t *sql.Tx,
	opts MigrateOptions, now time.Time, warnings *[]string) error {
	var legacyRows int
	if err := t.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM consumer_proxy_cooldowns`).Scan(&legacyRows); err != nil {
		return err
	}
	if legacyRows == 0 {
		return nil
	}
	maxCooldown := opts.LegacyMaxCooldown
	if maxCooldown <= 0 {
		maxCooldown = defaultLegacyMaxCooldown
		*warnings = append(*warnings,
			"legacy cooldown migration used the built-in 7d default because no LegacyMaxCooldown was provided")
	}
	if opts.IdentitySecret == "" {
		// Fail-safe: stamping v7 without a secret would permanently hide
		// every legacy cooldown from the new selector. The deployment must
		// provide IdentitySecret (main always does).
		return fmt.Errorf("cannot migrate %d legacy cooldown row(s) without an identity secret: provide MigrateOptions.IdentitySecret", legacyRows)
	}
	rows, err := t.QueryContext(ctx, `
		SELECT c.consumer_id, c.resource_id, c.last_delivered_at,
		       COALESCE(r.transport, ''), COALESCE(r.host, ''), COALESCE(r.port, -1),
		       COALESCE(cr.username, ''), COALESCE(cr.password, '')
		FROM consumer_proxy_cooldowns c
		LEFT JOIN proxy_resources r ON r.resource_id = c.resource_id
		LEFT JOIN proxy_credentials cr ON cr.credential_version = r.credential_version`)
	if err != nil {
		return err
	}
	defer rows.Close()
	type cdRow struct {
		consumerID string
		resourceID string
		delivered  time.Time
		transport  domain.Transport
		host       string
		port       int
		username   string
		password   string
	}
	var cds []cdRow
	for rows.Next() {
		var r cdRow
		var delivered string
		if err := rows.Scan(&r.consumerID, &r.resourceID, &delivered, &r.transport,
			&r.host, &r.port, &r.username, &r.password); err != nil {
			return err
		}
		if r.delivered, err = time.Parse(time.RFC3339Nano, delivered); err != nil {
			// A corrupted timestamp must not fail the whole migration; the
			// row still forces a conservative project hold below.
			r.delivered = time.Time{}
		}
		cds = append(cds, r)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	rows.Close()
	keyProjects := map[string]string{}
	keyRows, err := t.QueryContext(ctx,
		`SELECT key_id, project_id FROM api_keys WHERE project_id != ''`)
	if err != nil {
		return err
	}
	for keyRows.Next() {
		var keyID, projectID string
		if err := keyRows.Scan(&keyID, &projectID); err != nil {
			keyRows.Close()
			return err
		}
		keyProjects[keyID] = projectID
	}
	if err := keyRows.Err(); err != nil {
		keyRows.Close()
		return err
	}
	keyRows.Close()
	unmapped := 0
	for _, r := range cds {
		projectID := keyProjects[r.consumerID]
		if projectID == "" {
			projectID = DefaultProjectID
		}
		if r.transport == "" || r.delivered.IsZero() {
			// No inventory row (or an unreadable delivery time): the
			// fingerprint cannot be recomputed, so hold the whole project
			// conservatively instead of silently dropping the window.
			unmapped++
			if _, err := holdProjectCooldownTx(ctx, t, projectID,
				r.delivered, maxCooldown, now); err != nil {
				return err
			}
			continue
		}
		fp := domain.ConnectionFingerprint(opts.IdentitySecret, r.transport,
			r.host, r.port, r.username, r.password)
		blockedUntil := r.delivered.Add(maxCooldown)
		if !blockedUntil.After(now) {
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
			projectID, fp, fmtTime(r.delivered), fmtTime(blockedUntil)); err != nil {
			return err
		}
	}
	if unmapped > 0 {
		*warnings = append(*warnings, fmt.Sprintf(
			"%d legacy cooldown row(s) had no matching inventory resource or unreadable delivery time; a conservative project-wide cooldown hold was opened for each", unmapped))
	}
	return nil
}

// holdProjectCooldownTx opens a project-wide cooldown window from the given
// delivery baseline; the wildcard row blocks every fingerprint for the
// project until it expires.
func holdProjectCooldownTx(ctx context.Context, t *sql.Tx, projectID string,
	delivered time.Time, cooldown time.Duration, now time.Time) (bool, error) {
	baseline := delivered
	if baseline.IsZero() || baseline.After(now) {
		baseline = now
	}
	blockedUntil := baseline.Add(cooldown)
	if !blockedUntil.After(now) {
		return false, nil
	}
	_, err := t.ExecContext(ctx, `
		INSERT INTO project_proxy_cd(project_id, connection_fingerprint, issued_at, blocked_until)
		VALUES(?,?,?,?)
		ON CONFLICT(project_id, connection_fingerprint) DO UPDATE SET
		  blocked_until = MAX(blocked_until, excluded.blocked_until),
		  issued_at = CASE
		    WHEN excluded.blocked_until > blocked_until THEN excluded.issued_at
		    ELSE issued_at END`,
		projectID, projectWideCooldownFingerprint, fmtTime(baseline), fmtTime(blockedUntil))
	return err == nil, err
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
