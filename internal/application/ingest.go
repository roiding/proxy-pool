package application

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"proxypool/internal/domain"
	"proxypool/internal/integrations/custom"
	"proxypool/internal/storage/sqlite"
)

var sourceIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

type SourceConfig struct {
	DefaultTransport domain.Transport `json:"default_transport"`
	MaxPoolLeases    int              `json:"max_pool_leases"`
	LocalRetentionMS int64            `json:"local_retention_ms"`
}

type IngestService struct {
	store        *sqlite.Store
	secret       string
	allowPrivate bool
	Now          func() time.Time
}

func NewIngestService(store *sqlite.Store, secret string, allowPrivate bool) *IngestService {
	return &IngestService{store: store, secret: secret, allowPrivate: allowPrivate, Now: time.Now}
}

func (s *IngestService) EnsureSource(ctx context.Context, id string, cfg SourceConfig, enabled bool) error {
	if !sourceIDPattern.MatchString(id) {
		return domain.InvalidRequest("source_id must match [a-z0-9][a-z0-9_-]{0,63}")
	}
	if cfg.MaxPoolLeases <= 0 {
		cfg.MaxPoolLeases = 1
	}
	if cfg.DefaultTransport == "" {
		cfg.DefaultTransport = domain.TransportHTTP
	}
	if cfg.DefaultTransport != domain.TransportHTTP {
		return domain.UnsupportedTransport(string(cfg.DefaultTransport))
	}
	configJSON, err := json.Marshal(cfg)
	if err != nil {
		return domain.AsError(err)
	}
	src := domain.Source{
		ID:          id,
		Tenant:      "default",
		AdapterType: "custom",
		ParserType:  "lines_v1",
		Enabled:     enabled,
		ConfigJSON:  string(configJSON),
	}
	if err := s.store.UpsertSource(ctx, src, s.Now()); err != nil {
		return domain.StoreUnavailable(err)
	}
	return nil
}

type ImportResult struct {
	Added    int                `json:"added"`
	Updated  int                `json:"updated"`
	Skipped  int                `json:"skipped"`
	Revoked  int                `json:"revoked"`
	Rejected []custom.LineError `json:"rejected,omitempty"`
}

func (s *IngestService) ImportLines(ctx context.Context, sourceID, content string) (ImportResult, *domain.Error) {
	src, err := s.store.GetSource(ctx, sourceID)
	if err != nil {
		return ImportResult{}, domain.AsError(err)
	}
	if !src.Enabled {
		return ImportResult{}, domain.InvalidRequest("source %q is disabled", sourceID)
	}
	var cfg SourceConfig
	if err := json.Unmarshal([]byte(src.ConfigJSON), &cfg); err != nil {
		return ImportResult{}, domain.AsError(fmt.Errorf("decode source config: %w", err))
	}
	if cfg.MaxPoolLeases <= 0 {
		cfg.MaxPoolLeases = 1
	}
	parsed, rejected := custom.ParseLines(custom.LinesOptions{
		DefaultTransport: cfg.DefaultTransport,
		AllowPrivate:     s.allowPrivate,
	}, content)
	now := s.Now()
	generation := contentGeneration(content)
	candidates := make([]domain.NormalizedCandidate, 0, len(parsed))
	for _, c := range parsed {
		c.SourceID = sourceID
		c.RecordID = fmt.Sprintf("line-%d", len(candidates)+1)
		c.Generation = generation
		credVer := domain.CredentialVersion(s.secret, c.Conn.Username, c.Conn.Password)
		c.CredentialVersion = credVer
		c.ResourceID = domain.ResourceID(s.secret, domain.IdentityKey{
			Tenant:            src.Tenant,
			Transport:         c.Conn.Transport,
			Host:              c.Conn.Host,
			Port:              c.Conn.Port,
			CredentialVersion: credVer,
			Session:           c.Session,
		})
		c.OfferID = domain.OfferID(s.secret, sourceID, c.RecordID, c.Generation)
		if cfg.LocalRetentionMS > 0 {
			deadline := now.Add(time.Duration(cfg.LocalRetentionMS) * time.Millisecond)
			c.LocalRetentionDeadline = &deadline
			c.ExpiryBasis = "pool_policy"
		} else {
			c.ExpiryBasis = "unknown"
		}
		c.MaxPoolLeases = cfg.MaxPoolLeases
		candidates = append(candidates, c)
	}
	counts, err := s.store.ImportSnapshot(ctx, sourceID, generation, candidates, now)
	if err != nil {
		return ImportResult{}, domain.StoreUnavailable(err)
	}
	return ImportResult{
		Added:    counts.Added,
		Updated:  counts.Updated,
		Skipped:  counts.Skipped,
		Revoked:  counts.Revoked,
		Rejected: rejected,
	}, nil
}

// contentGeneration keys a self-uploaded list by its exact content: identical
// re-uploads are idempotent, changed lists start a new generation.
func contentGeneration(content string) string {
	sum := sha256.Sum256([]byte(content))
	return "imp-" + hex.EncodeToString(sum[:])[:16]
}
