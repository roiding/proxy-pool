package integration

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"proxypool/internal/application"
	"proxypool/internal/config"
	"proxypool/internal/domain"
	"proxypool/internal/storage/sqlite"
)

// The v6 fixture and fingerprint deliberately do not use the evolving schema
// or identity implementation: migration must work for an actual old database.
func auditLegacyFingerprint(secret, host string, port int) string {
	canonical := strings.Join([]string{"v1", "default", "http", host, strconv.Itoa(port), "", "static"}, "\x1f")
	mac := hmac.New(sha256.New, []byte("proxy-pool/resource-identity/v1\x00"+secret))
	_, _ = mac.Write([]byte(canonical))
	return "v1-" + hex.EncodeToString(mac.Sum(nil))
}

func TestAuditLegacyMigrationPreservesTokenInventoryAndActiveCooldown(t *testing.T) {
	j := startJudge(t)
	proxy := startFakeProxy(t)
	cfg := config.Default()
	cfg.DatabasePath = filepath.Join(t.TempDir(), "legacy.db")
	cfg.IdentitySecret = "audit-identity-secret-0123456789"
	cfg.APIToken = "audit-master-token-0123456789"
	cfg.JudgeURL = j.url()
	cfg.JudgeCAPem = j.certPEM()
	cfg.AllowPrivateAddresses = true
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
	token := "pk_audit_legacy_token_not_for_production"
	mac := hmac.New(sha256.New, []byte(cfg.IdentitySecret))
	_, _ = mac.Write([]byte(token))
	tokenHash := hex.EncodeToString(mac.Sum(nil))
	seed := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO sources(source_id,tenant,adapter_type,parser_type,enabled,config_version,config_json,created_at,updated_at)
		  VALUES('legacy-upload','default','custom','lines_v1',1,1,'{"default_transport":"http","max_pool_leases":2}',?,?)`,
			[]any{stamp, stamp}},
		{`INSERT INTO proxy_resources(resource_id,tenant,transport,host,port,credential_version,session_policy,resource_kind,disabled,created_at,updated_at)
		  VALUES(?,'default','http','127.0.0.1',?,'','static','static_endpoint',0,?,?)`,
			[]any{resourceID, portOf(proxy.URL), stamp, stamp}},
		{`INSERT INTO source_offers(offer_id,source_id,resource_id,source_record_id,source_generation,expiry_basis,claims_json,max_pool_leases,state,created_at,updated_at)
		  VALUES('legacy-offer','legacy-upload',?,'legacy-record','pool','unknown','{}',2,'ENABLED',?,?)`,
			[]any{resourceID, stamp, stamp}},
		{`INSERT INTO api_keys(key_id,name,token_hash,token_prefix,enabled,default_cooldown_ms,created_at,updated_at)
		  VALUES('legacy-key','legacy-business',?,'pk_audit_le',1,300000,?,?)`,
			[]any{tokenHash, stamp, stamp}},
		{`INSERT INTO consumer_proxy_cooldowns(consumer_id,resource_id,last_delivered_at) VALUES('legacy-key',?,?)`,
			[]any{resourceID, now.Add(-time.Minute).Format(time.RFC3339)}},
	}
	for _, q := range seed {
		if _, err := db.Exec(q.sql, q.args...); err != nil {
			t.Fatalf("seed legacy database: %v", err)
		}
	}

	for boot := 0; boot < 2; boot++ {
		t.Run(fmt.Sprintf("boot-%d", boot), func(t *testing.T) {
			e := newEnvWithConfig(t, cfg, j.certPool())
			srv := auditServer(t, e)
			status, data := auditPost(t, srv, token, "/api/v1/proxies/acquire", `{"count":1,"cd":0}`)
			var result application.AcquireResult
			if status != http.StatusOK || json.Unmarshal(data, &result) != nil || result.Delivered != 0 {
				t.Fatalf("migration or restart cleared old CD: status=%d body=%s", status, data)
			}
			stats, err := e.store.Stats(context.Background())
			if err != nil || stats.Resources != 1 || stats.EnabledOffers != 1 {
				t.Fatalf("migration lost uploaded inventory: stats=%+v err=%v", stats, err)
			}
			var hash, prefix string
			var enabled int
			if err := db.QueryRow(`SELECT token_hash, token_prefix, enabled FROM api_keys WHERE key_id='legacy-key'`).
				Scan(&hash, &prefix, &enabled); err != nil {
				t.Fatalf("migration lost the old key: %v", err)
			}
			if hash != tokenHash || prefix != "pk_audit_le" || enabled != 1 {
				t.Fatal("migration changed an existing credential")
			}
		})
	}
}

func TestAuditCurrentVersionReconcilesAdditiveSchema(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	ctx := context.Background()
	now := time.Now().UTC()
	until := now.Add(10 * time.Minute)
	if err := e.store.RecordProjectDeliveries(ctx, application.DefaultAcquireProject,
		10*time.Minute, now, []string{"audit-schema-preserved-cd"}); err != nil {
		t.Fatal(err)
	}
	// Model a v7 database written before the new additive fields existed.
	for _, column := range []string{"owner", "resource_id", "incarnation"} {
		if _, err := e.store.DB().Exec(`ALTER TABLE sticky_bindings DROP COLUMN ` + column); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.store.DB().Exec(`DROP TABLE validation_attempt_sources;
		DROP TABLE source_stat_counters; PRAGMA user_version = 7`); err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.MigrateWithOptions(ctx, sqlite.MigrateOptions{
		IdentitySecret: e.cfg.IdentitySecret, LegacyMaxCooldown: e.cfg.D(e.cfg.MaxDownstreamCooldownMS),
	}); err != nil {
		t.Fatal(err)
	}
	for _, column := range []string{"owner", "resource_id", "incarnation"} {
		var count int
		if err := e.store.DB().QueryRow(`SELECT COUNT(*) FROM pragma_table_info('sticky_bindings') WHERE name = ?`, column).Scan(&count); err != nil || count != 1 {
			t.Errorf("current-version startup did not reconcile sticky field %s: count=%d err=%v", column, count, err)
		}
	}
	for _, table := range []string{"validation_attempt_sources", "source_stat_counters"} {
		var count int
		if err := e.store.DB().QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?`, table).Scan(&count); err != nil || count != 1 {
			t.Errorf("current-version startup did not install supply table %s: count=%d err=%v", table, count, err)
		}
	}
	blocked, err := e.store.ProjectCooldownBlocked(ctx, application.DefaultAcquireProject,
		now, []string{"audit-schema-preserved-cd"})
	if err != nil || !blocked["audit-schema-preserved-cd"].Equal(until) {
		t.Fatalf("additive migration changed an existing fixed CD: windows=%+v err=%v", blocked, err)
	}
}

func TestAuditSourceVersionWatermarkSurvivesMigrationAndRestart(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	ctx := context.Background()
	now := time.Now().UTC()
	const sourceID = "audit-durable-source-version"
	source := domain.Source{
		ID: sourceID, Tenant: "default", AdapterType: "custom",
		ParserType: "lines_v1", Enabled: true, ConfigJSON: "{}",
	}
	if err := e.store.CreateSource(ctx, source, now); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		current, err := e.store.GetSource(ctx, sourceID)
		if err != nil {
			t.Fatal(err)
		}
		current.Enabled = !current.Enabled
		if err := e.store.UpdateSource(ctx, current, current.ConfigVersion, now); err != nil {
			t.Fatal(err)
		}
	}
	latest, err := e.store.GetSource(ctx, sourceID)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.store.RecordProjectDeliveries(ctx, application.DefaultAcquireProject,
		10*time.Minute, now, []string{"audit-durable-version-cd"}); err != nil {
		t.Fatal(err)
	}
	// Existing v7 deployments predate this additive table. Reconciliation
	// must seed the watermark from the latest source version, not from 1.
	if _, err := e.store.DB().Exec(`DROP TABLE source_version_watermarks; PRAGMA user_version = 7`); err != nil {
		t.Fatal(err)
	}
	opts := sqlite.MigrateOptions{
		IdentitySecret: e.cfg.IdentitySecret, LegacyMaxCooldown: e.cfg.D(e.cfg.MaxDownstreamCooldownMS),
	}
	if _, err := e.store.MigrateWithOptions(ctx, opts); err != nil {
		t.Fatal(err)
	}
	var watermark int64
	if err := e.store.DB().QueryRow(`SELECT last_version FROM source_version_watermarks WHERE source_id = ?`, sourceID).
		Scan(&watermark); err != nil || watermark < latest.ConfigVersion {
		t.Fatalf("v7 reconciliation reset the source version: watermark=%d latest=%d err=%v", watermark, latest.ConfigVersion, err)
	}
	if _, err := e.store.DeleteSource(ctx, sourceID, now); err != nil {
		t.Fatal(err)
	}
	if err := e.store.DB().Close(); err != nil {
		t.Fatal(err)
	}
	previous := latest.ConfigVersion
	for boot := 0; boot < 2; boot++ {
		db, err := sqlite.Open(e.cfg.DatabasePath, e.cfg.SQLiteCacheKiB)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		store := sqlite.New(db)
		if _, err := store.MigrateWithOptions(ctx, opts); err != nil {
			t.Fatal(err)
		}
		if err := store.CreateSource(ctx, source, now); err != nil {
			t.Fatal(err)
		}
		recreated, err := store.GetSource(ctx, sourceID)
		if err != nil || recreated.ConfigVersion <= previous {
			t.Fatalf("boot %d reused an obsolete source version: previous=%d current=%d err=%v", boot, previous, recreated.ConfigVersion, err)
		}
		previous = recreated.ConfigVersion
		blocked, err := store.ProjectCooldownBlocked(ctx, application.DefaultAcquireProject,
			now, []string{"audit-durable-version-cd"})
		if err != nil || !blocked["audit-durable-version-cd"].Equal(now.Add(10*time.Minute)) {
			t.Fatalf("source recreation or restart changed project CD: windows=%+v err=%v", blocked, err)
		}
		if _, err := store.DeleteSource(ctx, sourceID, now); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
