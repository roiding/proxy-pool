package config

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// validSecrets satisfies the credential checks so structural tests exercise
// the parameters under test instead of the always-required secrets.
func validSecrets(c *Config) {
	c.APIToken = "test-master-token-0123456789"
	c.IdentitySecret = "test-identity-secret-0123456789"
	c.JudgeURL = "http://127.0.0.1:8080/v1/judge"
}

func TestDefaultsPassValidation(t *testing.T) {
	cfg := Default()
	validSecrets(&cfg)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("defaults must validate: %v", err)
	}
	if cfg.AcquireDeadline() != 30*time.Second {
		t.Fatalf("default acquire deadline = %v, want 30s", cfg.AcquireDeadline())
	}
	if cfg.MaxAcquireDeadline() != 120*time.Second {
		t.Fatalf("default acquire ceiling = %v, want 120s", cfg.MaxAcquireDeadline())
	}
	if cfg.MaxCount != 100 {
		t.Fatalf("default MaxCount = %d, want 100", cfg.MaxCount)
	}
	if cfg.EvidenceCacheTTL() != 30*time.Second {
		t.Fatalf("default evidence cache TTL = %v, want 30s", cfg.EvidenceCacheTTL())
	}
	if cfg.MaxProofAge() < cfg.EvidenceCacheTTL() {
		t.Fatalf("proof age %v must cover cache TTL %v so cached evidence is not dropped at dispatch",
			cfg.MaxProofAge(), cfg.EvidenceCacheTTL())
	}
	if cfg.EffectiveEvidenceCacheTTL() != cfg.EvidenceCacheTTL() {
		t.Fatalf("effective TTL %v must equal configured TTL %v by default",
			cfg.EffectiveEvidenceCacheTTL(), cfg.EvidenceCacheTTL())
	}
	if cfg.StaticCacheTTL() != 5*time.Minute {
		t.Fatalf("default static cache TTL = %v, want 5m", cfg.StaticCacheTTL())
	}
	if cfg.StaticCacheMaxEntries != 256 || cfg.StaticCacheMaxBytes != 4<<20 {
		t.Fatalf("unexpected static cache bounds: %d entries, %d bytes",
			cfg.StaticCacheMaxEntries, cfg.StaticCacheMaxBytes)
	}
	if cfg.StickyBindingTTL() != 10*time.Minute {
		t.Fatalf("default sticky binding TTL = %v, want 10m", cfg.StickyBindingTTL())
	}
	if cfg.PrewarmDeadline() > cfg.PrewarmMaxDeadline() {
		t.Fatalf("prewarm deadline %v exceeds ceiling %v",
			cfg.PrewarmDeadline(), cfg.PrewarmMaxDeadline())
	}
	if cfg.JobMaxActivePerProject != 1 || cfg.JobMaxActiveGlobal != 4 {
		t.Fatalf("unexpected job concurrency defaults: %d/%d",
			cfg.JobMaxActivePerProject, cfg.JobMaxActiveGlobal)
	}
	if cfg.BarkTimeout() != 10*time.Second || cfg.CloudUsageTimeout() != 15*time.Second {
		t.Fatalf("unexpected usage timeouts: bark=%v cloud=%v",
			cfg.BarkTimeout(), cfg.CloudUsageTimeout())
	}
	if cfg.BarkMaxRetries != 3 || cfg.CloudUsageRetry != 3 {
		t.Fatalf("unexpected retry defaults: bark=%d cloud=%d",
			cfg.BarkMaxRetries, cfg.CloudUsageRetry)
	}
}

// Legacy deployments carry the old 8s request budget and 2s proof window.
// They must keep loading: the new ceiling is additive, not a reinterpretation
// of the old field, and the cache relation only applies to explicit values.
func TestLegacyEightSecondConfigLoads(t *testing.T) {
	legacy := `{
	  "listen_addr": "127.0.0.1:8080",
	  "api_token": "legacy-master-token-0123456789",
	  "identity_secret": "legacy-identity-secret-0123456789",
	  "judge_url": "http://127.0.0.1:8080/v1/judge",
	  "acquire_deadline_ms": 8000,
	  "connect_timeout_ms": 1000,
	  "probe_total_timeout_ms": 2500,
	  "max_proof_age_at_dispatch_ms": 2000,
	  "max_count_per_request": 5,
	  "prewarm_deadline_ms": 120000,
	  "unknown_future_field": true
	}`
	path := filepath.Join(t.TempDir(), "legacy.json")
	if err := os.WriteFile(path, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("legacy config must load: %v", err)
	}
	if cfg.AcquireDeadline() != 8*time.Second {
		t.Fatalf("legacy acquire deadline = %v, want 8s", cfg.AcquireDeadline())
	}
	if cfg.MaxAcquireDeadline() != 120*time.Second {
		t.Fatalf("legacy ceiling must default to 120s, got %v", cfg.MaxAcquireDeadline())
	}
	if cfg.MaxProofAge() != 2*time.Second {
		t.Fatalf("legacy proof age = %v, want 2s", cfg.MaxProofAge())
	}
	if cfg.MaxCount != 5 {
		t.Fatalf("legacy MaxCount = %d, want 5", cfg.MaxCount)
	}
	// The default 30s TTL is capped by the legacy 2s proof window, so the
	// effective reuse window stays 2s instead of silently dropping entries.
	if cfg.EffectiveEvidenceCacheTTL() != 2*time.Second {
		t.Fatalf("legacy effective cache TTL = %v, want 2s", cfg.EffectiveEvidenceCacheTTL())
	}
}

func TestLoadWithoutCredentialsFails(t *testing.T) {
	_, err := Load("")
	if err == nil || !strings.Contains(err.Error(), "api_token") {
		t.Fatalf(`Load("") without a config must still demand credentials, got %v`, err)
	}
}

func TestAcquireCeilingRelations(t *testing.T) {
	if _, err := LoadJSON(t, `{"acquire_deadline_ms":130000}`); err == nil {
		t.Fatal("default 120s ceiling must reject a 130s request budget")
	}
	cfg, err := LoadJSON(t, `{"max_acquire_deadline_ms":150000,"acquire_deadline_ms":150000}`)
	if err != nil {
		t.Fatalf("raised ceiling must accept matching budget: %v", err)
	}
	if cfg.MaxAcquireDeadline() != 150*time.Second {
		t.Fatalf("ceiling = %v, want 150s", cfg.MaxAcquireDeadline())
	}
	if _, err := LoadJSON(t, `{"max_acquire_deadline_ms":60000,"acquire_deadline_ms":90000}`); err == nil {
		t.Fatal("budget above explicit ceiling must fail")
	}
}

func TestExplicitEvidenceCacheMustFitProofWindow(t *testing.T) {
	if _, err := LoadJSON(t, `{"evidence_cache_ttl_ms":60000,"max_proof_age_at_dispatch_ms":2000}`); err == nil {
		t.Fatal("explicit cache TTL beyond the proof window must fail")
	}
	// The legacy combination stays loadable because the TTL was not set.
	if _, err := LoadJSON(t, `{"max_proof_age_at_dispatch_ms":2000}`); err != nil {
		t.Fatalf("legacy proof age without explicit cache TTL must load: %v", err)
	}
}

func TestResourceParameterRanges(t *testing.T) {
	cases := map[string]func(*Config){
		"max_count":               func(c *Config) { c.MaxCount = 0 },
		"acquire_budget_negative": func(c *Config) { c.AcquireDeadlineMS = -1 },
		"acquire_budget_over_day": func(c *Config) {
			c.MaxAcquireDeadlineMS = 24*3600*1000 + 1
			c.AcquireDeadlineMS = c.MaxAcquireDeadlineMS
		},
		"ceiling_zero":              func(c *Config) { c.MaxAcquireDeadlineMS = 0 },
		"connect_timeout_zero":      func(c *Config) { c.ConnectTimeoutMS = 0 },
		"probe_below_connect":       func(c *Config) { c.ProbeTotalTimeoutMS = c.ConnectTimeoutMS - 1 },
		"margin_eats_min_usable":    func(c *Config) { c.ExpirySafetyMarginMS = c.MinUsableMS },
		"lease_above_max":           func(c *Config) { c.LeaseTTLMS = c.MaxLeaseTTLMS + 1 },
		"parallel_zero":             func(c *Config) { c.ParallelPerRequest = 0 },
		"static_cache_zero_bytes":   func(c *Config) { c.StaticCacheMaxBytes = 0 },
		"static_cache_zero_entries": func(c *Config) { c.StaticCacheMaxEntries = 0 },
		"sticky_ttl_zero":           func(c *Config) { c.StickyBindingTTLMS = 0 },
		"prewarm_over_ceiling":      func(c *Config) { c.PrewarmDeadlineMS = c.PrewarmMaxDeadlineMS + 1 },
		"prewarm_ceiling_over_day":  func(c *Config) { c.PrewarmMaxDeadlineMS = 24*3600*1000 + 1 },
		"jobs_per_project_zero":     func(c *Config) { c.JobMaxActivePerProject = 0 },
		"jobs_global_below_project": func(c *Config) { c.JobMaxActiveGlobal = c.JobMaxActivePerProject - 1 },
		"session_ttl_zero":          func(c *Config) { c.AdminSessionTTLMS = 0 },
		"login_attempts_zero":       func(c *Config) { c.AdminLoginMaxAttempts = 0 },
		"max_sessions_zero":         func(c *Config) { c.AdminMaxSessions = 0 },
		"body_limit_zero":           func(c *Config) { c.MaxRequestBodyBytes = 0 },
		"bark_timeout_zero":         func(c *Config) { c.BarkTimeoutMS = 0 },
		"bark_retries_negative":     func(c *Config) { c.BarkMaxRetries = -1 },
		"cloud_timeout_zero":        func(c *Config) { c.CloudUsageTimeoutMS = 0 },
		"cloud_threshold_zero":      func(c *Config) { c.CloudUsageThresholdPercent = 0 },
		"cloud_threshold_high":      func(c *Config) { c.CloudUsageThresholdPercent = 101 },
		"cooldown_base_above_max":   func(c *Config) { c.CooldownBaseMS = c.CooldownMaxMS + 1 },
		"cooldown_jitter_one":       func(c *Config) { c.CooldownJitter = 1 },
		"source_threshold_zero":     func(c *Config) { c.SourceFailureThreshold = 0 },
		"source_base_above_max":     func(c *Config) { c.SourceCooldownBaseMS = c.SourceCooldownMaxMS + 1 },
		"source_max_overflow":       func(c *Config) { c.SourceCooldownBaseMS = math.MaxInt64; c.SourceCooldownMaxMS = math.MaxInt64 },
		"downstream_cd_overflow":    func(c *Config) { c.MaxDownstreamCooldownMS = math.MaxInt64 },
		"jitter_nan":                func(c *Config) { c.CooldownJitter = math.NaN() },
		"jitter_inf":                func(c *Config) { c.CooldownJitter = math.Inf(1) },
		"jitter_negative":           func(c *Config) { c.CooldownJitter = -0.1 },
		"candidate_page_zero":       func(c *Config) { c.CandidatePage = 0 },
		"probe_slots_zero":          func(c *Config) { c.ProbeSlots = 0 },
		"acquire_slots_zero":        func(c *Config) { c.MaxActiveAcquires = 0 },
		"sweep_zero":                func(c *Config) { c.SweepIntervalMS = 0 },
		"retention_below_hour":      func(c *Config) { c.AttemptRetentionMS = 59999 },
	}
	for name, mutate := range cases {
		cfg := Default()
		validSecrets(&cfg)
		mutate(&cfg)
		if err := cfg.Validate(); err == nil {
			t.Errorf("%s: expected validation error", name)
		}
	}
}

func TestSecurityAndUsageSettings(t *testing.T) {
	if _, err := LoadJSON(t, `{"trusted_proxy_cidrs":["10.0.0.0/8","bad-cidr"]}`); err == nil {
		t.Fatal("invalid trusted proxy CIDR must fail")
	}
	cfg, err := LoadJSON(t, `{"trusted_proxy_cidrs":["10.0.0.0/8","2001:db8::/32"],"public_https":true}`)
	if err != nil {
		t.Fatalf("valid CIDRs must load: %v", err)
	}
	if len(cfg.TrustedProxyCIDRs) != 2 || !cfg.PublicHTTPS {
		t.Fatalf("trusted proxy settings not retained: %v", cfg.TrustedProxyCIDRs)
	}
	if _, err := LoadJSON(t, `{"bark_url":"http://bark.example.com/secret"}`); err == nil {
		t.Fatal("plain-http external bark_url must fail")
	}
	if _, err := LoadJSON(t, `{"bark_url":"ftp://localhost/secret"}`); err == nil {
		t.Fatal("non-http bark_url scheme must fail")
	}
	if _, err := LoadJSON(t, `{"bark_url":"ftp://bark.example.com/secret"}`); err == nil {
		t.Fatal("non-http external bark_url scheme must fail")
	}
	if _, err := LoadJSON(t, `{"bark_url":"https://user:pass@bark.example.com/x"}`); err == nil {
		t.Fatal("bark_url with embedded credentials must fail")
	}
	for _, raw := range []string{
		`{"bark_url":"https://bark.example.com/device?token=query-secret"}`,
		`{"bark_url":"https://bark.example.com/device#fragment-secret"}`,
	} {
		_, err := LoadJSON(t, raw)
		if err == nil {
			t.Fatalf("secret-bearing bark_url accepted: %s", raw)
		}
		if strings.Contains(err.Error(), "query-secret") ||
			strings.Contains(err.Error(), "fragment-secret") {
			t.Fatalf("config validation echoed a secret-bearing URL: %v", err)
		}
	}
	if _, err := LoadJSON(t, `{"bark_url":"http://127.0.0.1:8080/ping"}`); err != nil {
		t.Fatalf("local plain-http bark_url must load: %v", err)
	}
	if _, err := LoadJSON(t, `{"bark_url":"https://bark.example.com/x"}`); err != nil {
		t.Fatalf("https bark_url must load: %v", err)
	}
	if _, err := LoadJSON(t, `{"cloud_usage_timezone":"Not/AZone"}`); err == nil {
		t.Fatal("invalid timezone must fail")
	}
	if _, err := LoadJSON(t, `{"cloud_usage_check_time":"9am"}`); err == nil {
		t.Fatal("invalid check time must fail")
	}
	if _, err := LoadJSON(t, `{"secret_encryption_key":"short"}`); err == nil {
		t.Fatal("short secret encryption key must fail")
	}
	cfg, err = LoadJSON(t, `{"secret_encryption_key":"0123456789abcdef0123456789abcdef"}`)
	if err != nil {
		t.Fatalf("32-char encryption key must load: %v", err)
	}
	if cfg.SecretEncryptionKey != "0123456789abcdef0123456789abcdef" {
		t.Fatal("secret encryption key not retained")
	}
}

func TestBrokenJSONFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "broken.json")
	if err := os.WriteFile(path, []byte(`{"acquire_deadline_ms":`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("malformed JSON must fail")
	}
	if _, err := LoadJSON(t, `[]`); err == nil {
		t.Fatal("non-object JSON config must fail")
	}
	if _, err := LoadJSON(t, strings.Repeat("x", 3)); err == nil {
		t.Fatal("garbage JSON config must fail")
	}
}

// LoadJSON prepends the required test credentials to a config snippet,
// writes it to a temp file and loads it, mirroring how deployments supply
// the file. Only the snippet's keys are present, so explicit-key rules in
// Load behave as they would in production.
func LoadJSON(t *testing.T, raw string) (Config, error) {
	t.Helper()
	trimmed := strings.TrimSpace(raw)
	if strings.HasPrefix(trimmed, "{") {
		base := `{"api_token":"test-master-token-0123456789",` +
			`"identity_secret":"test-identity-secret-0123456789",` +
			`"judge_url":"http://127.0.0.1:8080/v1/judge",`
		raw = base + trimmed[1:]
	}
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	return Load(path)
}
