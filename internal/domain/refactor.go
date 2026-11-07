package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"
)

type ValidationScope struct {
	Mode           string `json:"mode"`
	TargetHostPort string `json:"target_host_port"`
	Profile        string `json:"profile"`
	SecurityHash   string `json:"security_hash"`
}

type ProjectDefaults struct {
	AutoPrewarmTarget      int             `json:"auto_prewarm_target"`
	NetworkReuseCooldownMS int64           `json:"network_reuse_cooldown_ms"`
	CooldownMS             int64           `json:"cooldown_ms"`
	TimeoutMS              int64           `json:"timeout_ms"`
	Validation             ValidationScope `json:"validation"`
}

type Project struct {
	ID               string          `json:"project_id"`
	Name             string          `json:"name"`
	Enabled          bool            `json:"enabled"`
	Defaults         ProjectDefaults `json:"defaults"`
	AllowedSourceIDs []string        `json:"allowed_source_ids"`
	ConfigVersion    int64           `json:"config_version"`
	CreatedAt        time.Time       `json:"created_at"`
	UpdatedAt        time.Time       `json:"updated_at"`
}

type StickyBinding struct {
	ProjectID string `json:"project_id"`
	SessionID string `json:"session_id"`
	Version   int64  `json:"version"`
	// Owner is the request that created or currently claims this binding;
	// it is internal state and never projected to clients.
	Owner       string `json:"-"`
	Fingerprint string `json:"fingerprint"`
	// ResourceID and Incarnation identify the inventory object behind the
	// binding. Ephemeral sources may leave ResourceID empty and are guarded
	// by the live source configuration instead.
	ResourceID          string `json:"-"`
	Incarnation         string `json:"-"`
	SourceID            string `json:"source_id"`
	SourceConfigVersion int64  `json:"source_config_version"`
	// ConnectionCiphertext is AES-GCM material sealed with the deployment
	// secret; it must never be projected to API clients (use json:"-").
	ConnectionCiphertext string    `json:"-"`
	Validation           Evidence  `json:"validation"`
	Deadline             time.Time `json:"deadline"`
	State                string    `json:"state"`
	CreatedAt            time.Time `json:"created_at"`
	UpdatedAt            time.Time `json:"updated_at"`
}

// SourceAllowed applies the project's allowlist semantics: nil means
// unrestricted, an empty list means nothing is allowed. An absent sourceID
// with an unrestricted project is still allowed.
func (p Project) SourceAllowed(sourceID string) bool {
	if p.AllowedSourceIDs == nil {
		return true
	}
	for _, id := range p.AllowedSourceIDs {
		if id == sourceID {
			return true
		}
	}
	return false
}

// FilterAllowedSources narrows requested source IDs to the project allowlist.
// nil allowlist returns the request untouched; an empty allowlist returns an
// empty (not nil) slice so callers cannot mistake "none" for "all".
func (p Project) FilterAllowedSources(requested []string) []string {
	if p.AllowedSourceIDs == nil {
		return requested
	}
	if requested == nil {
		return append([]string{}, p.AllowedSourceIDs...)
	}
	out := make([]string, 0, len(requested))
	for _, id := range requested {
		if p.SourceAllowed(id) {
			out = append(out, id)
		}
	}
	return out
}

// EvidenceScopeHash summarizes the validation scope (mode, target, profile,
// and the caller-computed security hash over judge/signature/CA settings) so
// cached evidence can only be reused inside one exact scope.
func EvidenceScopeHash(scope ValidationScope) string {
	canonical := strings.Join([]string{
		"v1",
		scope.Mode,
		scope.TargetHostPort,
		scope.Profile,
		scope.SecurityHash,
	}, fieldSep)
	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:])
}

type Job struct {
	Automatic  bool       `json:"automatic,omitempty"`
	ID         string     `json:"job_id"`
	ProjectID  string     `json:"project_id"`
	Kind       string     `json:"kind"`
	State      string     `json:"state"`
	ParamsJSON string     `json:"params_json"`
	Target     int        `json:"target"`
	Ready      int        `json:"ready"`
	Probed     int        `json:"probed"`
	Failed     int        `json:"failed"`
	StopReason string     `json:"stop_reason"`
	LastError  string     `json:"last_error"`
	CreatedAt  time.Time  `json:"created_at"`
	UpdatedAt  time.Time  `json:"updated_at"`
	StartedAt  *time.Time `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at"`
}
