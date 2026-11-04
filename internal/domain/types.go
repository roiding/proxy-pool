package domain

import (
	"encoding/json"
	"time"
)

type Transport string

const (
	TransportHTTP   Transport = "http"
	TransportSOCKS5 Transport = "socks5"
)

type ResourceKind string

const (
	KindStatic   ResourceKind = "static_endpoint"
	KindSticky   ResourceKind = "sticky_gateway"
	KindRotating ResourceKind = "rotating_gateway"
)

type SessionMode string

const (
	SessionStatic   SessionMode = "static"
	SessionSticky   SessionMode = "sticky"
	SessionRotating SessionMode = "rotating"
)

type Connection struct {
	Transport Transport `json:"transport"`
	Host      string    `json:"host"`
	Port      int       `json:"port"`
	Username  string    `json:"username,omitempty"`
	Password  string    `json:"password,omitempty"`
}

// MarshalJSON keeps the wire connection faithful to the actual credential
// configuration: an authenticated connection always shows its password,
// including an explicit empty string, while anonymous connections never
// invent credentials. Parsing stays the default unmarshal behavior.
func (c Connection) MarshalJSON() ([]byte, error) {
	type connectionFields struct {
		Transport Transport `json:"transport"`
		Host      string    `json:"host"`
		Port      int       `json:"port"`
		Username  string    `json:"username,omitempty"`
		Password  *string   `json:"password,omitempty"`
	}
	out := connectionFields{Transport: c.Transport, Host: c.Host, Port: c.Port,
		Username: c.Username}
	if c.Username != "" || c.Password != "" {
		out.Password = &c.Password
	}
	return json.Marshal(out)
}

type Source struct {
	ID            string
	Tenant        string
	AdapterType   string
	ParserType    string
	Enabled       bool
	ConfigVersion int64
	ConfigJSON    string
}

type Resource struct {
	ID                string
	Tenant            string
	Conn              Connection
	CredentialVersion string
	Kind              ResourceKind
	Session           SessionMode
	Disabled          bool
}

type Offer struct {
	ID                     string
	SourceID               string
	ResourceID             string
	SourceRecordID         string
	SourceGeneration       string
	ProviderExpiresAt      *time.Time
	LocalRetentionDeadline *time.Time
	ExpiryBasis            string
	Claims                 map[string]string
	MaxPoolLeases          int
	CapacityGroup          string
	State                  string
}

type Candidate struct {
	Offer    Offer
	Resource Resource
	// Ephemeral marks request-local candidates (on-demand API channels).
	// They are never backed by inventory rows: reservations and leases key on
	// the deterministic connection fingerprint instead.
	Ephemeral           bool
	Fingerprint         string `json:"fingerprint"`
	SourceConfigVersion int64  `json:"source_config_version"`
	Incarnation         string `json:"incarnation"`
	LastOutcome         string
	LastPassAt          *time.Time
	ConsecutiveFailures int
	Observation         *Observation
}

// Observation is pool-verified evidence about a resource's last successful
// exit, kept separate from vendor claims on the offer.
type Observation struct {
	ExitIP      string    `json:"exit_ip"`
	CountryCode string    `json:"country_code,omitempty"`
	Country     string    `json:"country,omitempty"`
	Region      string    `json:"region,omitempty"`
	City        string    `json:"city,omitempty"`
	ISP         string    `json:"isp,omitempty"`
	ObservedAt  time.Time `json:"observed_at"`
}

// APIKey is a downstream channel credential. The plaintext token is only
// returned once at creation; storage keeps an HMAC hash and a display prefix.
type APIKey struct {
	ID                string    `json:"key_id"`
	ProjectID         string    `json:"project_id"`
	Name              string    `json:"name"`
	TokenPrefix       string    `json:"token_prefix"`
	Enabled           bool      `json:"enabled"`
	DefaultCooldownMS int64     `json:"default_cooldown_ms"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

type Reservation struct {
	ID         string
	ResourceID string
	OfferID    string
	ContextKey string
	Owner      string
	Fence      int64
	Deadline   time.Time
	State      string
}

const (
	OfferEnabled = "ENABLED"
	LeaseActive  = "ACTIVE"
)

type Evidence struct {
	AttemptID    string    `json:"attempt_id"`
	RequestID    string    `json:"request_id"`
	Profile      string    `json:"profile"`
	Mode         string    `json:"mode"`
	Target       string    `json:"target"`
	CacheHit     bool      `json:"cache_hit"`
	SecurityHash string    `json:"security_hash"`
	Outcome      string    `json:"outcome"`
	StartedAt    time.Time `json:"started_at"`
	CompletedAt  time.Time `json:"completed_at"`
	// ValidUntil is the proof-reuse deadline supplied at delivery time
	// (completed_at plus the configured maximum proof age); evidence at
	// rest may leave it unset.
	ValidUntil     *time.Time `json:"valid_until,omitempty"`
	LatencyMS      int64      `json:"latency_ms"`
	ObservedExitIP string     `json:"observed_exit_ip,omitempty"`
}

type Lease struct {
	ID         string     `json:"lease_id"`
	Tenant     string     `json:"tenant"`
	RequestID  string     `json:"request_id"`
	ResourceID string     `json:"resource_id"`
	OfferID    string     `json:"offer_id"`
	ContextKey string     `json:"-"`
	Mode       string     `json:"mode"`
	Deadline   time.Time  `json:"lease_expires_at"`
	State      string     `json:"state"`
	Conn       Connection `json:"-"`
	Validation Evidence   `json:"validation"`
	CreatedAt  time.Time  `json:"created_at"`
	ReleasedAt *time.Time `json:"-"`
}

type NormalizedCandidate struct {
	OfferID                string
	SourceID               string
	RecordID               string
	Generation             string
	ResourceID             string
	CredentialVersion      string
	Conn                   Connection
	Kind                   ResourceKind
	Session                SessionMode
	ProviderExpiresAt      *time.Time
	LocalRetentionDeadline *time.Time
	ExpiryBasis            string
	Claims                 map[string]string
	MaxPoolLeases          int
	CapacityGroup          string
}

// UploadKey authorizes an external program to push imports into specific
// upload sources and to query those batches' results. Its plaintext token
// is returned once at creation; storage keeps only an HMAC hash and a
// display prefix. It never grants acquisition, admin, job, source
// management or credential-reading rights, and is separate from project
// acquisition keys.
type UploadKey struct {
	ID          string    `json:"key_id"`
	Name        string    `json:"name"`
	TokenPrefix string    `json:"token_prefix"`
	Enabled     bool      `json:"enabled"`
	SourceIDs   []string  `json:"source_ids"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}
