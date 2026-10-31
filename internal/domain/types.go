package domain

import "time"

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
	Offer               Offer
	Resource            Resource
	LastOutcome         string
	LastPassAt          *time.Time
	ConsecutiveFailures int
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
	AttemptID      string    `json:"attempt_id"`
	RequestID      string    `json:"request_id"`
	Profile        string    `json:"profile"`
	Outcome        string    `json:"outcome"`
	StartedAt      time.Time `json:"started_at"`
	CompletedAt    time.Time `json:"completed_at"`
	LatencyMS      int64     `json:"latency_ms"`
	ObservedExitIP string    `json:"observed_exit_ip,omitempty"`
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
