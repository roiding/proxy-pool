package config

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"
)

const MaxAcquireDeadlineMS = 30000

type Config struct {
	ListenAddr            string `json:"listen_addr"`
	DatabasePath          string `json:"database_path"`
	LockPath              string `json:"lock_path"`
	APIToken              string `json:"api_token"`
	IdentitySecret        string `json:"identity_secret"`
	JudgeURL              string `json:"judge_url"`
	JudgeCAPem            string `json:"judge_ca_pem"`
	JudgeSecret           string `json:"judge_secret"`
	JudgeTrustForwarded   bool   `json:"judge_trust_forwarded"`
	AllowPrivateAddresses bool   `json:"allow_private_addresses"`

	AcquireDeadlineMS    int64 `json:"acquire_deadline_ms"`
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

	SQLiteCacheKiB     int   `json:"sqlite_cache_kib"`
	SweepIntervalMS    int64 `json:"sweep_interval_ms"`
	AttemptRetentionMS int64 `json:"attempt_retention_ms"`

	NightlyValidationTime string `json:"custom_nightly_validation_time"`
	NightlyRetireFailures int    `json:"custom_nightly_retire_failures"`

	CooldownBaseMS int64   `json:"first_failure_cooldown_ms"`
	CooldownMaxMS  int64   `json:"cooldown_max_ms"`
	CooldownJitter float64 `json:"cooldown_jitter_fraction"`
}

func Default() Config {
	return Config{
		ListenAddr:            "127.0.0.1:8080",
		DatabasePath:          "proxy-pool.db",
		LockPath:              "proxy-pool.lock",
		AcquireDeadlineMS:     8000,
		ConnectTimeoutMS:      1000,
		ProbeTotalTimeoutMS:   2500,
		MaxProofAgeMS:         2000,
		LeaseTTLMS:            30000,
		MaxLeaseTTLMS:         300000,
		MinUsableMS:           10000,
		ExpirySafetyMarginMS:  2000,
		MaxCount:              5,
		ParallelPerRequest:    2,
		ProbeSlots:            4,
		MaxActiveAcquires:     8,
		CandidatePage:         8,
		MaxCandidateAttempts:  25,
		SQLiteCacheKiB:        2048,
		SweepIntervalMS:       30000,
		AttemptRetentionMS:    24 * 3600 * 1000,
		NightlyValidationTime: "03:00",
		NightlyRetireFailures: 2,
		CooldownBaseMS:        30000,
		CooldownMaxMS:         15 * 60 * 1000,
		CooldownJitter:        0.25,
	}
}

func Load(path string) (Config, error) {
	cfg := Default()
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return Config{}, fmt.Errorf("read config: %w", err)
		}
		if err := json.Unmarshal(data, &cfg); err != nil {
			return Config{}, fmt.Errorf("parse config: %w", err)
		}
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	var errs []string
	if len(c.APIToken) < 16 {
		errs = append(errs, "api_token must be at least 16 characters")
	}
	if len(c.IdentitySecret) < 16 {
		errs = append(errs, "identity_secret must be at least 16 characters")
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
	if c.NightlyValidationTime != "" {
		if _, err := time.Parse("15:04", c.NightlyValidationTime); err != nil {
			errs = append(errs, fmt.Sprintf("custom_nightly_validation_time %q must be HH:MM", c.NightlyValidationTime))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("invalid config: %s", strings.Join(errs, "; "))
	}
	return nil
}

func (c Config) D(ms int64) time.Duration { return time.Duration(ms) * time.Millisecond }

func (c Config) AcquireDeadline() time.Duration   { return c.D(c.AcquireDeadlineMS) }
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
func (c Config) CooldownBase() time.Duration { return c.D(c.CooldownBaseMS) }
func (c Config) CooldownMax() time.Duration  { return c.D(c.CooldownMaxMS) }
