package application

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"sync"
	"time"

	"proxypool/internal/domain"
	"proxypool/internal/integrations/custom"
	"proxypool/internal/integrations/scdn"
	"proxypool/internal/integrations/sites"
	"proxypool/internal/storage/sqlite"
)

var sourceIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

const (
	AdapterCustom     = "custom"
	AdapterProvider   = "provider_api"
	AdapterSite       = "site_fetch"
	ParserLines       = "lines_v1"
	ParserJSONMapping = "json_mapping_v1"
	ParserSite        = "site_v1"
	// The in-memory guard keeps a misbehaving operator from spinning the
	// vendor's free API. Per source, not durable across restarts.
	defaultMinFetchIntervalMS = 1000
	// Free list pages dislike hammering more than JSON APIs do.
	defaultSiteFetchInterval = 15 * time.Second
)

type SourceConfig struct {
	DefaultTransport domain.Transport `json:"default_transport,omitempty"`
	MaxPoolLeases    int              `json:"max_pool_leases"`
	LocalRetentionMS int64            `json:"local_retention_ms,omitempty"`
	Provider         *ProviderConfig  `json:"provider,omitempty"`
	Site             *SiteConfig      `json:"site,omitempty"`
}

// ProviderConfig declares a provider-API channel. The vendor endpoint is
// fixed by the registered adapter (scdn), so the configuration only chooses
// the request parameters and transport mapping.
type ProviderConfig struct {
	Protocol           string `json:"protocol"`
	CountryCode        string `json:"country_code,omitempty"`
	FetchCount         int    `json:"fetch_count,omitempty"`
	MinFetchIntervalMS int64  `json:"min_fetch_interval_ms,omitempty"`
}

func (p ProviderConfig) scdnConfig() scdn.Config {
	return scdn.Config{Protocol: p.Protocol, CountryCode: p.CountryCode, FetchCount: p.FetchCount}
}

func (p ProviderConfig) minFetchInterval() time.Duration {
	if p.MinFetchIntervalMS < defaultMinFetchIntervalMS {
		return defaultMinFetchIntervalMS * time.Millisecond
	}
	return time.Duration(p.MinFetchIntervalMS) * time.Millisecond
}

// SiteConfig selects one catalog entry of the site_fetch adapter. The page
// URLs and parsers are code-owned; the channel only picks which entry and how
// many pages to walk per fetch.
type SiteConfig struct {
	SiteID   string `json:"site_id"`
	MaxPages int    `json:"max_pages,omitempty"`
}

type IngestService struct {
	store        *sqlite.Store
	secret       string
	allowPrivate bool
	// ScdnEndpoint and ScdnHTTPClient are test hooks; production uses the
	// official vendor endpoint and default transport settings.
	ScdnEndpoint   string
	ScdnHTTPClient *http.Client
	// SiteHTTPClient and SiteOverrides are test hooks; production uses the
	// default client and the built-in catalog URLs.
	SiteHTTPClient *http.Client
	SiteOverrides  map[string]sites.Site

	fetchMu     sync.Mutex
	fetchBusy   map[string]bool
	lastFetchAt map[string]time.Time
	Now         func() time.Time
}

func NewIngestService(store *sqlite.Store, secret string, allowPrivate bool) *IngestService {
	return &IngestService{
		store:          store,
		secret:         secret,
		allowPrivate:   allowPrivate,
		ScdnEndpoint:   scdn.DefaultEndpoint,
		ScdnHTTPClient: http.DefaultClient,
		SiteHTTPClient: http.DefaultClient,
		fetchBusy:      map[string]bool{},
		lastFetchAt:    map[string]time.Time{},
		Now:            time.Now,
	}
}

func (s *IngestService) EnsureSource(ctx context.Context, id string, cfg SourceConfig, enabled bool) error {
	if !sourceIDPattern.MatchString(id) {
		return domain.InvalidRequest("source_id must match [a-z0-9][a-z0-9_-]{0,63}")
	}
	adapterType, parserType := AdapterCustom, ParserLines
	if cfg.Provider != nil {
		if err := cfg.Provider.scdnConfig().Validate(); err != nil {
			return domain.InvalidRequest("provider channel config: %v", err)
		}
		adapterType, parserType = AdapterProvider, ParserJSONMapping
	}
	if cfg.Site != nil {
		if _, ok := sites.ByID(cfg.Site.SiteID); !ok {
			return domain.InvalidRequest("unknown site_id %q; see GET /v1/admin/sites", cfg.Site.SiteID)
		}
		if cfg.Site.MaxPages < 0 || cfg.Site.MaxPages > sites.MaxPages {
			return domain.InvalidRequest("max_pages must be within 1..%d", sites.MaxPages)
		}
		adapterType, parserType = AdapterSite, ParserSite
	}
	if cfg.MaxPoolLeases <= 0 {
		cfg.MaxPoolLeases = 1
	}
	if cfg.Provider == nil && cfg.Site == nil {
		if cfg.DefaultTransport == "" {
			cfg.DefaultTransport = domain.TransportHTTP
		}
		switch cfg.DefaultTransport {
		case domain.TransportHTTP, domain.TransportSOCKS5:
		default:
			return domain.UnsupportedTransport(string(cfg.DefaultTransport))
		}
	}
	configJSON, err := json.Marshal(cfg)
	if err != nil {
		return domain.AsError(err)
	}
	src := domain.Source{
		ID:          id,
		Tenant:      "default",
		AdapterType: adapterType,
		ParserType:  parserType,
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
	candidates := make([]domain.NormalizedCandidate, 0, len(parsed))
	for _, c := range parsed {
		cand := s.buildCandidate(src, cfg, c.Conn, now)
		candidates = append(candidates, cand)
	}
	counts, err := s.store.ImportCandidates(ctx, candidates, now)
	if err != nil {
		return ImportResult{}, domain.StoreUnavailable(err)
	}
	return ImportResult{
		Added:    counts.Added,
		Updated:  counts.Updated,
		Skipped:  counts.Skipped,
		Rejected: rejected,
	}, nil
}

// recordFingerprint gives each uploaded proxy a stable source-record ID
// derived from its content, so the same proxy keeps the same offer across
// re-uploads and tombstones survive file reshuffling.
func recordFingerprint(conn domain.Connection) string {
	sum := sha256.Sum256([]byte(string(conn.Transport) + "|" + conn.Host + "|" +
		strconv.Itoa(conn.Port) + "|" + conn.Username + "|" + conn.Password))
	return "px-" + hex.EncodeToString(sum[:])[:16]
}

type FetchResult struct {
	SourceID   string          `json:"source_id"`
	Requested  int             `json:"requested"`
	Fetched    int             `json:"fetched"`
	Added      int             `json:"added"`
	Updated    int             `json:"updated"`
	Skipped    int             `json:"skipped"`
	Rejected   []FetchRowError `json:"rejected,omitempty"`
	PagesDone  int             `json:"pages_done,omitempty"`
	PagesTotal int             `json:"pages_total,omitempty"`
}

// FetchRowError is the shared per-row failure shape for provider batches and
// site pages; it keeps the same JSON fields as the old scdn entry errors.
type FetchRowError struct {
	Line  int    `json:"line"`
	Input string `json:"input"`
	Error string `json:"error"`
}

// FetchSource pulls one batch from a provider-API channel and imports it
// additively. The vendor response carries no expiry data, so offers live
// under the pool's retention policy until maintenance retires them; every
// acquire still performs its own fresh validation before delivery.
func (s *IngestService) FetchSource(ctx context.Context, sourceID string) (FetchResult, *domain.Error) {
	s.fetchMu.Lock()
	if s.fetchBusy[sourceID] {
		s.fetchMu.Unlock()
		return FetchResult{}, domain.E("SOURCE_FETCH_IN_PROGRESS", 409,
			"source %q already has a fetch running", sourceID)
	}
	s.fetchBusy[sourceID] = true
	s.fetchMu.Unlock()
	defer func() {
		s.fetchMu.Lock()
		delete(s.fetchBusy, sourceID)
		s.fetchMu.Unlock()
	}()

	src, err := s.store.GetSource(ctx, sourceID)
	if err != nil {
		return FetchResult{}, domain.AsError(err)
	}
	if src.AdapterType != AdapterProvider && src.AdapterType != AdapterSite {
		return FetchResult{}, domain.InvalidRequest(
			"source %q is a %q channel; only provider_api and site_fetch sources support fetch",
			sourceID, src.AdapterType)
	}
	if !src.Enabled {
		return FetchResult{}, domain.InvalidRequest("source %q is disabled", sourceID)
	}
	if src.AdapterType == AdapterSite {
		return s.fetchSite(ctx, src, sourceID)
	}
	var cfg SourceConfig
	if err := json.Unmarshal([]byte(src.ConfigJSON), &cfg); err != nil {
		return FetchResult{}, domain.AsError(fmt.Errorf("decode source config: %w", err))
	}
	if cfg.Provider == nil {
		return FetchResult{}, domain.InvalidRequest("source %q has no provider config", sourceID)
	}
	if cfg.MaxPoolLeases <= 0 {
		cfg.MaxPoolLeases = 1
	}
	now := s.Now()
	s.fetchMu.Lock()
	last := s.lastFetchAt[sourceID]
	s.fetchMu.Unlock()
	if wait := cfg.Provider.minFetchInterval() - now.Sub(last); wait > 0 {
		return FetchResult{}, domain.E("SOURCE_RATE_LIMITED", 429,
			"source %q was fetched %.1fs ago; retry in %.1fs",
			sourceID, now.Sub(last).Seconds(), wait.Seconds())
	}
	defer func() {
		s.fetchMu.Lock()
		s.lastFetchAt[sourceID] = s.Now()
		s.fetchMu.Unlock()
	}()

	fetchCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	batch, ferr := scdn.Fetch(fetchCtx, s.ScdnHTTPClient, s.ScdnEndpoint,
		cfg.Provider.scdnConfig(), s.allowPrivate)
	if ferr != nil {
		return FetchResult{}, mapScdnError(ferr)
	}
	candidates := make([]domain.NormalizedCandidate, 0, len(batch.Entries))
	for _, entry := range batch.Entries {
		conn := domain.Connection{Transport: entry.Transport, Host: entry.Host, Port: entry.Port}
		cand := s.buildCandidate(src, cfg, conn, now)
		cand.Claims = map[string]string{"vendor_protocol": cfg.Provider.Protocol}
		candidates = append(candidates, cand)
	}
	counts, err := s.store.ImportCandidates(ctx, candidates, now)
	if err != nil {
		return FetchResult{}, domain.StoreUnavailable(err)
	}
	return FetchResult{
		SourceID:  sourceID,
		Requested: batch.Requested,
		Fetched:   len(batch.Entries),
		Added:     counts.Added,
		Updated:   counts.Updated,
		Skipped:   counts.Skipped,
		Rejected:  toFetchRowErrors(batch.Errors),
	}, nil
}

func toFetchRowErrors(errs []scdn.EntryError) []FetchRowError {
	if len(errs) == 0 {
		return nil
	}
	out := make([]FetchRowError, 0, len(errs))
	for _, e := range errs {
		out = append(out, FetchRowError{Line: e.Line, Input: e.Input, Error: e.Error})
	}
	return out
}

// fetchSite walks one catalog site's pages and imports the parsed endpoints
// into the pool under the source's retention/lease settings. Parsed entries
// carry no credentials, so validation still decides every delivery.
func (s *IngestService) fetchSite(ctx context.Context, src domain.Source, sourceID string) (FetchResult, *domain.Error) {
	var cfg SourceConfig
	if err := json.Unmarshal([]byte(src.ConfigJSON), &cfg); err != nil {
		return FetchResult{}, domain.AsError(fmt.Errorf("decode source config: %w", err))
	}
	if cfg.Site == nil || cfg.Site.SiteID == "" {
		return FetchResult{}, domain.InvalidRequest("source %q has no site config", sourceID)
	}
	site, ok := sites.ByID(cfg.Site.SiteID)
	if !ok {
		return FetchResult{}, domain.InvalidRequest("unknown site_id %q", cfg.Site.SiteID)
	}
	if override, ok := s.SiteOverrides[cfg.Site.SiteID]; ok {
		site = override
	}

	now := s.Now()
	s.fetchMu.Lock()
	last := s.lastFetchAt[sourceID]
	s.fetchMu.Unlock()
	if wait := defaultSiteFetchInterval - now.Sub(last); wait > 0 {
		return FetchResult{}, domain.E("SOURCE_RATE_LIMITED", 429,
			"source %q was fetched %.1fs ago; retry in %.1fs",
			sourceID, now.Sub(last).Seconds(), wait.Seconds())
	}
	defer func() {
		s.fetchMu.Lock()
		s.lastFetchAt[sourceID] = s.Now()
		s.fetchMu.Unlock()
	}()

	fetchCtx, cancel := context.WithTimeout(ctx, sites.MinFetchInterval)
	defer cancel()
	output, ferr := sites.FetchSite(fetchCtx, s.SiteHTTPClient, site, cfg.Site.MaxPages, s.allowPrivate)
	if ferr != nil {
		return FetchResult{}, mapSitesError(ferr)
	}
	now = s.Now()
	candidates := make([]domain.NormalizedCandidate, 0, len(output.Entries))
	for _, e := range output.Entries {
		cand := s.buildCandidate(src, cfg, e.Conn, now)
		claims := map[string]string{"source_site": site.ID, "anonymity": string(site.Anonymity)}
		if e.VendorProtocol != "" {
			claims["vendor_protocol"] = e.VendorProtocol
		}
		cand.Claims = claims
		candidates = append(candidates, cand)
	}
	counts, err := s.store.ImportCandidates(ctx, candidates, now)
	if err != nil {
		return FetchResult{}, domain.StoreUnavailable(err)
	}
	result := FetchResult{
		SourceID:   sourceID,
		Fetched:    len(output.Entries),
		Added:      counts.Added,
		Updated:    counts.Updated,
		Skipped:    counts.Skipped,
		PagesDone:  output.PagesDone,
		PagesTotal: output.PagesTotal,
	}
	for _, r := range output.Rejected {
		result.Rejected = append(result.Rejected, FetchRowError{Line: r.Line, Input: r.Input, Error: r.Error})
	}
	return result, nil
}

func mapSitesError(err error) *domain.Error {
	var serr *sites.Error
	if !errors.As(err, &serr) {
		return domain.AsError(err)
	}
	code := "SOURCE_UNAVAILABLE"
	status := 503
	if serr.Kind == sites.KindFormat {
		code = "SOURCE_FORMAT_ERROR"
		status = 502
	}
	return domain.E(code, status, "site fetch failed: %s", serr.Detail)
}

func mapScdnError(err error) *domain.Error {
	var serr *scdn.Error
	if !errors.As(err, &serr) {
		return domain.AsError(err)
	}
	code := "SOURCE_UNAVAILABLE"
	switch serr.Kind {
	case scdn.KindFormat:
		code = "SOURCE_FORMAT_ERROR"
	case scdn.KindProvider:
		code = "SOURCE_PROVIDER_ERROR"
	}
	return domain.E(code, serr.Status, "provider fetch failed: %s", serr.Detail)
}

// buildCandidate normalizes one connection into a stable additive offer:
// content-derived record ID, HMAC resource identity, and pool retention.
func (s *IngestService) buildCandidate(src domain.Source, cfg SourceConfig, conn domain.Connection, now time.Time) domain.NormalizedCandidate {
	if cfg.MaxPoolLeases <= 0 {
		cfg.MaxPoolLeases = 1
	}
	cand := domain.NormalizedCandidate{
		SourceID:   src.ID,
		RecordID:   recordFingerprint(conn),
		Generation: "pool",
		Conn:       conn,
		Kind:       domain.KindStatic,
		Session:    domain.SessionStatic,
	}
	credVer := domain.CredentialVersion(s.secret, conn.Username, conn.Password)
	cand.CredentialVersion = credVer
	cand.ResourceID = domain.ResourceID(s.secret, domain.IdentityKey{
		Tenant:            src.Tenant,
		Transport:         conn.Transport,
		Host:              conn.Host,
		Port:              conn.Port,
		CredentialVersion: credVer,
		Session:           cand.Session,
	})
	cand.OfferID = domain.OfferID(s.secret, src.ID, cand.RecordID, cand.Generation)
	if cfg.LocalRetentionMS > 0 {
		deadline := now.Add(time.Duration(cfg.LocalRetentionMS) * time.Millisecond)
		cand.LocalRetentionDeadline = &deadline
		cand.ExpiryBasis = "pool_policy"
	} else {
		cand.ExpiryBasis = "unknown"
	}
	cand.MaxPoolLeases = cfg.MaxPoolLeases
	return cand
}
