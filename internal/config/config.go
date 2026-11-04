package config

import (
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/url"
	"os"
	"strings"
	"time"
)

// DefaultMaxAcquireDeadlineMS is the default safety ceiling for one acquire
// request budget.
const (
	DefaultMaxAcquireDeadlineMS int64 = 120000

	hourMS = int64(3600 * 1000)
	dayMS  = 24 * hourMS
)

type Config struct {
	ListenAddr            string `json:"listen_addr"`
	DatabasePath          string `json:"database_path"`
	LockPath              string `json:"lock_path"`
	APIToken              string `json:"api_token"`
	AdminPassword         string `json:"admin_password"`
	IdentitySecret        string `json:"identity_secret"`
	JudgeURL              string `json:"judge_url"`
	JudgeCAPem            string `json:"judge_ca_pem"`
	JudgeSecret           string `json:"judge_secret"`
	JudgeTrustForwarded   bool   `json:"judge_trust_forwarded"`
	AllowPrivateAddresses bool   `json:"allow_private_addresses"`

	AcquireDeadlineMS int64 `json:"acquire_deadline_ms"`
	// MaxAcquireDeadlineMS bounds one request's total wait budget; it is not
	// a per-proxy timeout.
	MaxAcquireDeadlineMS int64 `json:"max_acquire_deadline_ms"`
	ConnectTimeoutMS     int64 `json:"connect_timeout_ms"`
	ProbeTotalTimeoutMS  int64 `json:"probe_total_timeout_ms"`
	MaxProofAgeMS        int64 `json:"max_proof_age_at_dispatch_ms"`
	LeaseTTLMS           int64 `json:"default_lease_ttl_ms"`
	MaxLeaseTTLMS        int64 `json:"max_lease_ttl_ms"`
	MinUsableMS          int64 `json:"default_min_usable_ms"`
	ExpirySafetyMarginMS int64 `json:"expiry_safety_margin_ms"`

	MaxCount             int `json:"max_count_per_request"`
	ParallelPerRequest   int `json:"max_parallel_probes_per_request"`
	ProbeSlots           int `json:"deployment_probe_slots"`
	MaxActiveAcquires    int `json:"max_active_acquisitions"`
	CandidatePage        int `json:"candidate_page_size"`
	MaxCandidateAttempts int `json:"max_candidate_attempts_per_request"`

	EvidenceCacheTTLMS    int64 `json:"evidence_cache_ttl_ms"`
	StaticCacheTTLMS      int64 `json:"static_cache_ttl_ms"`
	StaticCacheMaxEntries int   `json:"static_cache_max_entries"`
	StaticCacheMaxBytes   int64 `json:"static_cache_max_bytes"`
	StickyBindingTTLMS    int64 `json:"sticky_binding_ttl_ms"`

	PrewarmMaxDeadlineMS   int64 `json:"prewarm_max_deadline_ms"`
	JobMaxActivePerProject int   `json:"job_max_active_per_project"`
	JobMaxActiveGlobal     int   `json:"job_max_active_global"`

	AdminSessionTTLMS     int64    `json:"admin_session_ttl_ms"`
	AdminLoginWindowMS    int64    `json:"admin_login_window_ms"`
	AdminLoginMaxAttempts int      `json:"admin_login_max_attempts"`
	AdminMaxSessions      int      `json:"admin_max_sessions"`
	MaxRequestBodyBytes   int64    `json:"max_request_body_bytes"`
	PublicHTTPS           bool     `json:"public_https"`
	TrustedProxyCIDRs     []string `json:"trusted_proxy_cidrs"`

	BarkURL        string `json:"bark_url"`
	BarkTimeoutMS  int64  `json:"bark_timeout_ms"`
	BarkMaxRetries int    `json:"bark_max_retries"`

	CloudUsageTimeoutMS        int64  `json:"cloud_usage_timeout_ms"`
	CloudUsageRetry            int    `json:"cloud_usage_retry"`
	CloudUsageCheckTime        string `json:"cloud_usage_check_time"`
	CloudUsageTimezone         string `json:"cloud_usage_timezone"`
	CloudUsageThresholdPercent int    `json:"cloud_usage_threshold_percent"`
	CloudUsageMonthlyTotalGB   int64  `json:"cloud_usage_monthly_total_gb"`
	HuaweiAccessKeyID          string `json:"huawei_access_key_id"`
	HuaweiSecretAccessKey      string `json:"huawei_secret_access_key"`
	HuaweiRegion               string `json:"huawei_region"`
	HuaweiResourceID           string `json:"huawei_resource_id"`
	HuaweiPackageID            string `json:"huawei_package_id"`
	// SecretEncryptionKey protects stored cloud/Bark secrets. It lives in
	// deployment config only and must never be written into the database.
	SecretEncryptionKey string `json:"secret_encryption_key"`

	SQLiteCacheKiB     int   `json:"sqlite_cache_kib"`
	SweepIntervalMS    int64 `json:"sweep_interval_ms"`
	AttemptRetentionMS int64 `json:"attempt_retention_ms"`

	NightlyValidationTime string `json:"custom_nightly_validation_time"`
	NightlyRetireFailures int    `json:"custom_nightly_retire_failures"`

	SourceFailureThreshold  int    `json:"source_failure_threshold"`
	SourceCooldownBaseMS    int64  `json:"source_cooldown_base_ms"`
	SourceCooldownMaxMS     int64  `json:"source_cooldown_max_ms"`
	PrewarmDeadlineMS       int64  `json:"prewarm_deadline_ms"`
	PrewarmMaxAttempts      int    `json:"prewarm_max_attempts"`
	GeoIPDBPath             string `json:"geoip_db_path"`
	MaxDownstreamCooldownMS int64  `json:"max_downstream_cooldown_ms"`

	CooldownBaseMS int64   `json:"first_failure_cooldown_ms"`
	CooldownMaxMS  int64   `json:"cooldown_max_ms"`
	CooldownJitter float64 `json:"cooldown_jitter_fraction"`
}

func Default() Config {
	return Config{
		ListenAddr:                 "127.0.0.1:8080",
		DatabasePath:               "proxy-pool.db",
		LockPath:                   "proxy-pool.lock",
		AcquireDeadlineMS:          30000,
		MaxAcquireDeadlineMS:       DefaultMaxAcquireDeadlineMS,
		ConnectTimeoutMS:           1000,
		ProbeTotalTimeoutMS:        2500,
		MaxProofAgeMS:              30000,
		LeaseTTLMS:                 30000,
		MaxLeaseTTLMS:              300000,
		MinUsableMS:                10000,
		ExpirySafetyMarginMS:       2000,
		MaxCount:                   100,
		ParallelPerRequest:         2,
		ProbeSlots:                 4,
		MaxActiveAcquires:          8,
		CandidatePage:              8,
		MaxCandidateAttempts:       25,
		EvidenceCacheTTLMS:         30000,
		StaticCacheTTLMS:           300000,
		StaticCacheMaxEntries:      256,
		StaticCacheMaxBytes:        4 << 20,
		StickyBindingTTLMS:         600000,
		SQLiteCacheKiB:             2048,
		SweepIntervalMS:            30000,
		AttemptRetentionMS:         24 * 3600 * 1000,
		NightlyValidationTime:      "03:00",
		NightlyRetireFailures:      2,
		SourceFailureThreshold:     3,
		SourceCooldownBaseMS:       30 * 1000,
		SourceCooldownMaxMS:        15 * 60 * 1000,
		PrewarmDeadlineMS:          120 * 1000,
		PrewarmMaxDeadlineMS:       600000,
		JobMaxActivePerProject:     1,
		JobMaxActiveGlobal:         4,
		AdminSessionTTLMS:          12 * hourMS,
		AdminLoginWindowMS:         60000,
		AdminLoginMaxAttempts:      10,
		AdminMaxSessions:           64,
		MaxRequestBodyBytes:        65536,
		BarkTimeoutMS:              10000,
		BarkMaxRetries:             3,
		CloudUsageTimeoutMS:        15000,
		CloudUsageRetry:            3,
		CloudUsageCheckTime:        "09:00",
		CloudUsageTimezone:         "Asia/Shanghai",
		CloudUsageThresholdPercent: 80,
		PrewarmMaxAttempts:         500,
		MaxDownstreamCooldownMS:    7 * 24 * 3600 * 1000,
		CooldownBaseMS:             30000,
		CooldownMaxMS:              15 * 60 * 1000,
		CooldownJitter:             0.25,
	}
}

func Load(path string) (Config, error) {
	cfg := Default()
	present := map[string]bool{}
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return Config{}, fmt.Errorf("read config: %w", err)
		}
		if err := json.Unmarshal(data, &cfg); err != nil {
			return Config{}, fmt.Errorf("parse config: %w", err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(data, &fields); err != nil {
			return Config{}, fmt.Errorf("parse config: %w", err)
		}
		for k := range fields {
			present[k] = true
		}
	}
	if err := cfg.validate(present); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	return c.validate(nil)
}

func (c Config) validate(present map[string]bool) error {
	var errs []string
	add := func(format string, args ...any) {
		errs = append(errs, fmt.Sprintf(format, args...))
	}
	if len(c.APIToken) < 16 {
		errs = append(errs, "api_token must be at least 16 characters")
	}
	if len(c.IdentitySecret) < 16 {
		errs = append(errs, "identity_secret must be at least 16 characters")
	}
	if c.AdminPassword != "" && len(c.AdminPassword) < 6 {
		errs = append(errs, "admin_password must be at least 6 characters when set")
	}
	if c.JudgeURL == "" {
		errs = append(errs, "judge_url is required")
	} else {
		u, err := url.Parse(c.JudgeURL)
		if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
			errs = append(errs, "judge_url must be an absolute http(s) URL")
		} else if u.Scheme == "http" {
			switch u.Hostname() {
			case "127.0.0.1", "::1", "localhost":
			default:
				if len(c.JudgeSecret) < 16 {
					errs = append(errs,
						"plain-http judge_url outside local testing requires judge_secret of at least 16 characters")
				}
			}
		}
		if c.JudgeSecret != "" && len(c.JudgeSecret) < 16 {
			errs = append(errs, "judge_secret must be at least 16 characters")
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("invalid config: %s", strings.Join(errs, "; "))
	}
	if c.SourceFailureThreshold < 1 {
		add("source_failure_threshold must be at least 1")
	}
	if c.SourceCooldownBaseMS <= 0 || c.SourceCooldownMaxMS < c.SourceCooldownBaseMS {
		add("source_cooldown_base_ms must be positive and <= source_cooldown_max_ms")
	}
	if c.PrewarmMaxAttempts < 1 {
		add("prewarm_max_attempts must be at least 1")
	}
	if c.MaxDownstreamCooldownMS <= 0 {
		add("max_downstream_cooldown_ms must be positive")
	}
	if len(errs) > 0 {
		return fmt.Errorf("invalid config: %s", strings.Join(errs, "; "))
	}

	checkRange := func(name string, v, min, max int64) {
		if v < min || v > max {
			add("%s must be between %d and %d", name, min, max)
		}
	}
	checkIntRange := func(name string, v, min, max int) {
		if v < min || v > max {
			add("%s must be between %d and %d", name, min, max)
		}
	}

	checkRange("acquire_deadline_ms", c.AcquireDeadlineMS, 1, dayMS)
	checkRange("max_acquire_deadline_ms", c.MaxAcquireDeadlineMS, 1, dayMS)
	if c.AcquireDeadlineMS > c.MaxAcquireDeadlineMS {
		add("acquire_deadline_ms must not exceed max_acquire_deadline_ms")
	}
	checkRange("connect_timeout_ms", c.ConnectTimeoutMS, 1, 60*1000)
	checkRange("probe_total_timeout_ms", c.ProbeTotalTimeoutMS, c.ConnectTimeoutMS, 5*60*1000)
	checkRange("max_proof_age_at_dispatch_ms", c.MaxProofAgeMS, 1, dayMS)
	checkRange("evidence_cache_ttl_ms", c.EvidenceCacheTTLMS, 0, dayMS)
	// A cache that outlives the dispatch proof window silently discards its
	// entries, so reject that combination when the TTL was set explicitly.
	if present["evidence_cache_ttl_ms"] && c.EvidenceCacheTTLMS > c.MaxProofAgeMS {
		add("evidence_cache_ttl_ms must not exceed max_proof_age_at_dispatch_ms")
	}
	checkRange("default_lease_ttl_ms", c.LeaseTTLMS, 1, dayMS)
	checkRange("max_lease_ttl_ms", c.MaxLeaseTTLMS, c.LeaseTTLMS, dayMS)
	checkRange("default_min_usable_ms", c.MinUsableMS, 1, dayMS)
	checkRange("expiry_safety_margin_ms", c.ExpirySafetyMarginMS, 0, dayMS)
	if c.ExpirySafetyMarginMS >= c.MinUsableMS {
		add("expiry_safety_margin_ms must be smaller than default_min_usable_ms")
	}
	checkIntRange("max_count_per_request", c.MaxCount, 1, 10000)
	checkIntRange("max_parallel_probes_per_request", c.ParallelPerRequest, 1, 64)
	checkIntRange("deployment_probe_slots", c.ProbeSlots, 1, 1024)
	checkIntRange("max_active_acquisitions", c.MaxActiveAcquires, 1, 1024)
	checkIntRange("candidate_page_size", c.CandidatePage, 1, 10000)
	checkIntRange("max_candidate_attempts_per_request", c.MaxCandidateAttempts, 1, 100000)

	checkRange("static_cache_ttl_ms", c.StaticCacheTTLMS, 1, dayMS)
	checkIntRange("static_cache_max_entries", c.StaticCacheMaxEntries, 1, 1000000)
	checkRange("static_cache_max_bytes", c.StaticCacheMaxBytes, 1024, 1<<30)
	checkRange("sticky_binding_ttl_ms", c.StickyBindingTTLMS, 1, dayMS)

	checkRange("prewarm_deadline_ms", c.PrewarmDeadlineMS, 1, c.PrewarmMaxDeadlineMS)
	checkRange("prewarm_max_deadline_ms", c.PrewarmMaxDeadlineMS, 1, dayMS)
	checkIntRange("job_max_active_per_project", c.JobMaxActivePerProject, 1, 100)
	checkIntRange("job_max_active_global", c.JobMaxActiveGlobal, c.JobMaxActivePerProject, 1000)

	checkRange("admin_session_ttl_ms", c.AdminSessionTTLMS, time.Minute.Milliseconds(), 7*dayMS)
	checkRange("admin_login_window_ms", c.AdminLoginWindowMS, 1000, hourMS)
	checkIntRange("admin_login_max_attempts", c.AdminLoginMaxAttempts, 1, 1000)
	checkIntRange("admin_max_sessions", c.AdminMaxSessions, 1, 100000)
	checkRange("max_request_body_bytes", c.MaxRequestBodyBytes, 1024, 1<<20)
	for _, cidr := range c.TrustedProxyCIDRs {
		if _, _, err := net.ParseCIDR(strings.TrimSpace(cidr)); err != nil {
			add("trusted_proxy_cidrs entry %q is not a valid CIDR", cidr)
		}
	}

	checkRange("bark_timeout_ms", c.BarkTimeoutMS, 1, 60*1000)
	checkIntRange("bark_max_retries", c.BarkMaxRetries, 0, 10)
	checkRange("cloud_usage_timeout_ms", c.CloudUsageTimeoutMS, 1, 120*1000)
	checkIntRange("cloud_usage_retry", c.CloudUsageRetry, 0, 10)
	checkIntRange("cloud_usage_threshold_percent", c.CloudUsageThresholdPercent, 1, 100)
	checkRange("cloud_usage_monthly_total_gb", c.CloudUsageMonthlyTotalGB, 0, 10*1024*1024)
	if c.BarkURL != "" {
		u, err := url.Parse(c.BarkURL)
		if err != nil || u.Host == "" || u.User != nil ||
			u.RawQuery != "" || u.ForceQuery || u.Fragment != "" ||
			(u.Scheme != "https" && u.Scheme != "http") {
			add("bark_url must be an absolute http(s) URL without embedded credentials, query, or fragment")
		} else if u.Scheme == "http" {
			switch u.Hostname() {
			case "127.0.0.1", "::1", "localhost":
			default:
				add("bark_url outside local testing must use https")
			}
		}
	}
	if c.CloudUsageCheckTime != "" {
		if _, err := time.Parse("15:04", c.CloudUsageCheckTime); err != nil {
			add("cloud_usage_check_time %q must be HH:MM", c.CloudUsageCheckTime)
		}
	}
	if c.CloudUsageTimezone != "" {
		if _, err := time.LoadLocation(c.CloudUsageTimezone); err != nil {
			add("cloud_usage_timezone %q is not a valid IANA timezone", c.CloudUsageTimezone)
		}
	}
	if c.SecretEncryptionKey != "" && len(c.SecretEncryptionKey) < 32 {
		add("secret_encryption_key must be at least 32 characters when set")
	}

	if c.NightlyValidationTime != "" {
		if _, err := time.Parse("15:04", c.NightlyValidationTime); err != nil {
			add("custom_nightly_validation_time %q must be HH:MM", c.NightlyValidationTime)
		}
	}
	checkIntRange("custom_nightly_retire_failures", c.NightlyRetireFailures, 0, 1000)
	checkIntRange("sqlite_cache_kib", c.SQLiteCacheKiB, 0, 1<<20)
	checkRange("sweep_interval_ms", c.SweepIntervalMS, 1000, hourMS)
	checkRange("attempt_retention_ms", c.AttemptRetentionMS, hourMS, 365*dayMS)
	checkRange("source_cooldown_max_ms", c.SourceCooldownMaxMS, 1, 365*dayMS)
	checkRange("max_downstream_cooldown_ms", c.MaxDownstreamCooldownMS, 1, 365*dayMS)
	checkRange("first_failure_cooldown_ms", c.CooldownBaseMS, 1, c.CooldownMaxMS)
	checkRange("cooldown_max_ms", c.CooldownMaxMS, 1, 365*dayMS)
	if math.IsNaN(c.CooldownJitter) || math.IsInf(c.CooldownJitter, 0) ||
		c.CooldownJitter < 0 || c.CooldownJitter >= 1 {
		add("cooldown_jitter_fraction must be in [0,1)")
	}

	if len(errs) > 0 {
		return fmt.Errorf("invalid config: %s", strings.Join(errs, "; "))
	}
	return nil
}

func (c Config) D(ms int64) time.Duration { return time.Duration(ms) * time.Millisecond }

func (c Config) AcquireDeadline() time.Duration { return c.D(c.AcquireDeadlineMS) }
func (c Config) MaxAcquireDeadline() time.Duration {
	return c.D(c.MaxAcquireDeadlineMS)
}
func (c Config) ConnectTimeout() time.Duration    { return c.D(c.ConnectTimeoutMS) }
func (c Config) ProbeTotalTimeout() time.Duration { return c.D(c.ProbeTotalTimeoutMS) }
func (c Config) MaxProofAge() time.Duration       { return c.D(c.MaxProofAgeMS) }
func (c Config) LeaseTTL() time.Duration          { return c.D(c.LeaseTTLMS) }
func (c Config) MaxLeaseTTL() time.Duration       { return c.D(c.MaxLeaseTTLMS) }
func (c Config) MinUsable() time.Duration         { return c.D(c.MinUsableMS) }
func (c Config) ExpirySafetyMargin() time.Duration {
	return c.D(c.ExpirySafetyMarginMS)
}
func (c Config) SweepInterval() time.Duration { return c.D(c.SweepIntervalMS) }
func (c Config) AttemptRetention() time.Duration {
	return c.D(c.AttemptRetentionMS)
}
func (c Config) CooldownBase() time.Duration       { return c.D(c.CooldownBaseMS) }
func (c Config) CooldownMax() time.Duration        { return c.D(c.CooldownMaxMS) }
func (c Config) SourceCooldownBase() time.Duration { return c.D(c.SourceCooldownBaseMS) }
func (c Config) SourceCooldownMax() time.Duration  { return c.D(c.SourceCooldownMaxMS) }
func (c Config) PrewarmDeadline() time.Duration    { return c.D(c.PrewarmDeadlineMS) }
func (c Config) PrewarmMaxDeadline() time.Duration {
	return c.D(c.PrewarmMaxDeadlineMS)
}
func (c Config) EvidenceCacheTTL() time.Duration { return c.D(c.EvidenceCacheTTLMS) }

// EffectiveEvidenceCacheTTL caps the reuse window at the dispatch proof age:
// evidence older than MaxProofAge is discarded at dispatch, so a configured
// TTL beyond it can never be honored. Legacy configs that only raise the
// old 2s proof age therefore keep their effective 2s window instead of a
// misleading default 30s.
func (c Config) EffectiveEvidenceCacheTTL() time.Duration {
	ttl, proof := c.EvidenceCacheTTL(), c.MaxProofAge()
	if proof < ttl {
		return proof
	}
	return ttl
}
func (c Config) StaticCacheTTL() time.Duration   { return c.D(c.StaticCacheTTLMS) }
func (c Config) StickyBindingTTL() time.Duration { return c.D(c.StickyBindingTTLMS) }
func (c Config) AdminSessionTTL() time.Duration  { return c.D(c.AdminSessionTTLMS) }
func (c Config) AdminLoginWindow() time.Duration { return c.D(c.AdminLoginWindowMS) }

// TrustedProxies returns the parsed trusted proxy networks. Requests whose
// direct peer is in one of them may set forwarding headers we act on.
func (c Config) TrustedProxies() []*net.IPNet {
	nets := make([]*net.IPNet, 0, len(c.TrustedProxyCIDRs))
	for _, cidr := range c.TrustedProxyCIDRs {
		if _, ipnet, err := net.ParseCIDR(strings.TrimSpace(cidr)); err == nil {
			nets = append(nets, ipnet)
		}
	}
	return nets
}
func (c Config) BarkTimeout() time.Duration { return c.D(c.BarkTimeoutMS) }
func (c Config) CloudUsageTimeout() time.Duration {
	return c.D(c.CloudUsageTimeoutMS)
}
