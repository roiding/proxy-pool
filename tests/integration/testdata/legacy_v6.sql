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
ALTER TABLE reservations ADD COLUMN consumer_id TEXT NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS idx_res_consumer
  ON reservations(consumer_id, resource_id, state, deadline);
ALTER TABLE leases ADD COLUMN consumer_id TEXT NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS idx_leases_consumer
  ON leases(consumer_id, resource_id, state, deadline);
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
PRAGMA user_version = 6;
