package integration

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"proxypool/internal/config"
	"proxypool/internal/domain"
	"proxypool/internal/storage/sqlite"
)

// Seeds a frozen v6 database exactly like the audit fixture and migrates it
// with the deployment inputs; legacy CD must survive as a fixed project
// window keyed by the canonical connection fingerprint.
func TestMigrateV7LegacyCooldownBecomesProjectWindow(t *testing.T) {
	proxy := startFakeProxy(t)
	cfg := config.Default()
	cfg.DatabasePath = filepath.Join(t.TempDir(), "legacy.db")
	cfg.IdentitySecret = "core-migration-secret-0123456789"
	db, err := sqlite.Open(cfg.DatabasePath, cfg.SQLiteCacheKiB)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	fixture, err := os.ReadFile("testdata/legacy_v6.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(fixture)); err != nil {
		t.Fatalf("create frozen v6 database: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	stamp := now.Format(time.RFC3339)
	resourceID := auditLegacyFingerprint(cfg.IdentitySecret, "127.0.0.1", portOf(proxy.URL))
	token := "pk_core_migration_token_not_production"
	mac := hmac.New(sha256.New, []byte(cfg.IdentitySecret))
	_, _ = mac.Write([]byte(token))
	tokenHash := hex.EncodeToString(mac.Sum(nil))
	seeds := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO sources(source_id,tenant,adapter_type,parser_type,enabled,config_version,config_json,created_at,updated_at)
		  VALUES('legacy-upload','default','custom','lines_v1',1,1,'{"default_transport":"http","max_pool_leases":2}',?,?)`,
			[]any{stamp, stamp}},
		{`INSERT INTO proxy_resources(resource_id,tenant,transport,host,port,credential_version,session_policy,resource_kind,disabled,created_at,updated_at)
		  VALUES(?,'default','http','127.0.0.1',?,'','static','static_endpoint',0,?,?)`,
			[]any{resourceID, portOf(proxy.URL), stamp, stamp}},
		{`INSERT INTO api_keys(key_id,name,token_hash,token_prefix,enabled,default_cooldown_ms,created_at,updated_at)
		  VALUES('legacy-key','legacy-business',?,'pk_core_mi',1,300000,?,?)`,
			[]any{tokenHash, stamp, stamp}},
		{`INSERT INTO consumer_proxy_cooldowns(consumer_id,resource_id,last_delivered_at) VALUES('legacy-key',?,?)`,
			[]any{resourceID, now.Add(-time.Minute).Format(time.RFC3339)}},
	}
	for _, s := range seeds {
		if _, err := db.Exec(s.sql, s.args...); err != nil {
			t.Fatalf("seed legacy database: %v", s.sql)
		}
	}

	store := sqlite.New(db)
	ctx := context.Background()
	warnings, err := store.MigrateWithOptions(ctx, sqlite.MigrateOptions{
		IdentitySecret:    cfg.IdentitySecret,
		LegacyMaxCooldown: 7 * 24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("migrate v6 -> v7: %v", err)
	}
	if len(warnings) == 0 {
		t.Fatal("legacy cooldown migration must warn that original cd values were unknown")
	}
	var version int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil || version != 7 {
		t.Fatalf("user_version=%d err=%v, want 7", version, err)
	}

	// The legacy key owns exactly one project and keeps its credential.
	var projectID string
	if err := db.QueryRowContext(ctx,
		`SELECT project_id FROM api_keys WHERE key_id = 'legacy-key'`).Scan(&projectID); err != nil ||
		projectID != "proj_legacy_legacy-key" {
		t.Fatalf("legacy key project: %q err=%v", projectID, err)
	}
	var hash, prefix string
	var enabled int
	if err := db.QueryRowContext(ctx,
		`SELECT token_hash, token_prefix, enabled FROM api_keys WHERE key_id = 'legacy-key'`).
		Scan(&hash, &prefix, &enabled); err != nil || hash != tokenHash || prefix != "pk_core_mi" || enabled != 1 {
		t.Fatalf("migration changed an existing credential: %q %q %d (%v)", hash, prefix, enabled, err)
	}
	proj, err := store.GetProject(ctx, projectID)
	if err != nil || !proj.Enabled || proj.Defaults.CooldownMS != 300000 || proj.AllowedSourceIDs != nil {
		t.Fatalf("legacy project defaults: %+v err=%v", proj, err)
	}

	// The frozen last_delivered_at became a fixed window on the canonical
	// fingerprint (transport/host/port/credentials only; no tenant/session).
	fp := domain.ConnectionFingerprint(cfg.IdentitySecret, domain.TransportHTTP,
		"127.0.0.1", portOf(proxy.URL), "", "")
	blocked, err := store.ProjectCooldownBlocked(ctx, projectID, time.Now(), []string{fp})
	if err != nil {
		t.Fatal(err)
	}
	until, ok := blocked[fp]
	if !ok {
		t.Fatalf("legacy cooldown was not mapped to the canonical fingerprint %s", fp)
	}
	want := now.Add(-time.Minute).Add(7 * 24 * time.Hour)
	if d := until.Sub(want); d < -time.Minute || d > time.Minute {
		t.Fatalf("blocked_until %s, want ~%s", until, want)
	}
	// The window must survive a second boot with the same inputs.
	if _, err := store.MigrateWithOptions(ctx, sqlite.MigrateOptions{
		IdentitySecret:    cfg.IdentitySecret,
		LegacyMaxCooldown: 7 * 24 * time.Hour,
	}); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	blocked, err = store.ProjectCooldownBlocked(ctx, projectID, time.Now(), []string{fp})
	if err != nil || len(blocked) != 1 {
		t.Fatalf("second migration disturbed the window: %+v err=%v", blocked, err)
	}
}

func TestMigrateFreshDatabaseCreatesDefaultProjectAndTables(t *testing.T) {
	cfg := config.Default()
	cfg.DatabasePath = filepath.Join(t.TempDir(), "fresh.db")
	cfg.IdentitySecret = "core-migration-secret-0123456789"
	db, err := sqlite.Open(cfg.DatabasePath, cfg.SQLiteCacheKiB)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := sqlite.New(db)
	ctx := context.Background()
	warnings, err := store.MigrateWithOptions(ctx, sqlite.MigrateOptions{IdentitySecret: cfg.IdentitySecret})
	if err != nil || len(warnings) != 0 {
		t.Fatalf("fresh migrate: warnings=%v err=%v", warnings, err)
	}
	if _, err := store.GetProject(ctx, sqlite.DefaultProjectID); err != nil {
		t.Fatalf("default project missing: %v", err)
	}
	for _, table := range []string{"projects", "project_proxy_cd", "sticky_bindings", "jobs", "validation_evidence"} {
		var n int
		if err := db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&n); err != nil || n != 1 {
			t.Fatalf("v7 table %s missing: n=%d err=%v", table, n, err)
		}
	}
	// Re-running the migration on a current database is a no-op.
	warnings2, err := store.MigrateWithOptions(ctx, sqlite.MigrateOptions{IdentitySecret: cfg.IdentitySecret})
	if err != nil || warnings2 != nil {
		t.Fatalf("idempotent migrate: warnings=%v err=%v", warnings2, err)
	}
}

// TestMigrateV7FailsSafeWithoutSecretAndHoldsUnmappedRows pins two
// migration guarantees: a missing identity secret must fail the v7 stamp
// instead of silently hiding legacy cooldowns forever, and legacy rows
// without an inventory resource become a conservative project-wide hold
// rather than being dropped or zeroed.
func TestMigrateV7FailsSafeWithoutSecretAndHoldsUnmappedRows(t *testing.T) {
	cfg := config.Default()
	cfg.DatabasePath = filepath.Join(t.TempDir(), "legacy.db")
	db, err := sqlite.Open(cfg.DatabasePath, cfg.SQLiteCacheKiB)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	fixture, err := os.ReadFile("testdata/legacy_v6.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(fixture)); err != nil {
		t.Fatalf("create frozen v6 database: %v", err)
	}
	stamp := time.Now().UTC().Format(time.RFC3339)
	seeds := []string{
		`INSERT INTO sources(source_id,tenant,adapter_type,parser_type,enabled,config_version,config_json,created_at,updated_at)
		 VALUES('legacy-upload','default','custom','lines_v1',1,1,'{"default_transport":"http","max_pool_leases":2}','` + stamp + `','` + stamp + `')`,
		`INSERT INTO api_keys(key_id,name,token_hash,token_prefix,enabled,default_cooldown_ms,created_at,updated_at)
		 VALUES('legacy-key','legacy-business','deadbeef','pk_core_mi',1,300000,'` + stamp + `','` + stamp + `')`,
		// The cooldown row points at a resource that no longer exists: the
		// fingerprint cannot be recomputed from inventory.
		`INSERT INTO consumer_proxy_cooldowns(consumer_id,resource_id,last_delivered_at)
		 VALUES('legacy-key','res-vanished','` + time.Now().UTC().Add(-time.Minute).Format(time.RFC3339) + `')`,
	}
	for _, s := range seeds {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("seed legacy database: %v", err)
		}
	}
	store := sqlite.New(db)
	ctx := context.Background()
	if _, err := store.MigrateWithOptions(ctx, sqlite.MigrateOptions{}); err == nil {
		t.Fatal("migrating legacy cooldown rows without an identity secret must fail")
	}
	var version int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil || version != 6 {
		t.Fatalf("failed migration must not stamp v7: version=%d err=%v", version, err)
	}

	warnings, err := store.MigrateWithOptions(ctx, sqlite.MigrateOptions{
		IdentitySecret:    "core-failsafe-secret-0123456789",
		LegacyMaxCooldown: 7 * 24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("migrate with secret: %v", err)
	}
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil || version != 7 {
		t.Fatalf("user_version=%d err=%v, want 7", version, err)
	}
	found := false
	for _, w := range warnings {
		if strings.Contains(w, "project-wide cooldown hold") {
			found = true
		}
	}
	if !found {
		t.Fatalf("unmapped legacy rows must be warned about: %+v", warnings)
	}
	blocked, err := store.ProjectCooldownBlocked(ctx, "proj_legacy_legacy-key",
		time.Now(), []string{"any-fingerprint"})
	if err != nil || len(blocked) != 1 {
		t.Fatalf("unmapped legacy row must hold the whole project: %+v err=%v", blocked, err)
	}
}
