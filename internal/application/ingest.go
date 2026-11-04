package application

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
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
	AdapterStatic     = "static_url"
	ParserLines       = "lines_v1"
	ParserJSONMapping = "json_mapping_v1"
	ParserSite        = "site_v1"
	// The in-memory guard keeps a misbehaving operator from spinning the
	// vendor's free API. Per source, not durable across restarts.
	defaultMinFetchIntervalMS = 1000
	// Free list pages dislike hammering more than JSON APIs do.
	defaultSiteFetchInterval = 15 * time.Second
)

// AdapterDescriptor describes one code-owned adapter capability. Channels are
// instances of these adapters; the registry never grows from runtime input.
type AdapterDescriptor struct {
	ID           string   `json:"id"`
	Version      int      `json:"version"`
	ConfigKey    string   `json:"config_key"`
	Kind         string   `json:"kind"`
	Description  string   `json:"description"`
	Capabilities []string `json:"capabilities"`
}

// AdapterDescriptors is the closed registry: local upload, the on-demand
// provider API, catalogued site fetches, and generic static URL fetches.
func AdapterDescriptors() []AdapterDescriptor {
	return []AdapterDescriptor{
		{ID: AdapterCustom, Version: 1, ConfigKey: "custom", Kind: "upload",
			Description:  "persistent line upload channel",
			Capabilities: []string{"upload", "periodic_validation", "prewarm"}},
		{ID: AdapterProvider, Version: 1, ConfigKey: "provider", Kind: "on_demand",
			Description:  "provider API pulled per request; never enters the pool",
			Capabilities: []string{"on_demand", "diagnostic_fetch"}},
		{ID: AdapterSite, Version: 1, ConfigKey: "site", Kind: "static_fetch",
			Description:  "catalogued public proxy list site",
			Capabilities: []string{"static_fetch", "diagnostic_fetch"}},
		{ID: AdapterStatic, Version: 1, ConfigKey: "static", Kind: "static_fetch",
			Description:  "generic static proxy list URL",
			Capabilities: []string{"static_fetch", "diagnostic_fetch"}},
	}
}

type SourceConfig struct {
	DefaultTransport domain.Transport `json:"default_transport,omitempty"`
	MaxPoolLeases    int              `json:"max_pool_leases"`
	LocalRetentionMS int64            `json:"local_retention_ms,omitempty"`
	Provider         *ProviderConfig  `json:"provider,omitempty"`
	Site             *SiteConfig      `json:"site,omitempty"`
	Static           *StaticConfig    `json:"static,omitempty"`
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

// StaticConfig selects the generic static URL adapter: one absolute http(s)
// list URL parsed with the shared line parser. Fetching/caching is separate.
type StaticConfig struct {
	URL              string           `json:"url"`
	DefaultTransport domain.Transport `json:"default_transport,omitempty"`
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
	static      *staticCache
	Now         func() time.Time
}

// SourceBatchRefiller implements the acquire-side batch surface. Both methods
// return request-local candidates; static lists never enter the SQLite pool.
func (s *IngestService) Scoped(ctx context.Context, sourceID string, wanted int,
	scope RefillScope) ([]domain.Candidate, *domain.Error) {
	src, err := s.store.GetSource(ctx, sourceID)
	if err != nil {
		return nil, domain.AsError(err)
	}
	if !src.Enabled {
		return nil, domain.InvalidRequest("source %q is disabled", sourceID)
	}
	switch src.AdapterType {
	case AdapterProvider:
		cands, derr := s.FetchOnDemand(ctx, sourceID, wanted)
		if derr != nil {
			return nil, derr
		}
		// The provider call is batch-oriented. Keep the full in-scope batch
		// for acquire so one AUTH/FAIL candidate does not suppress the next
		// row; the caller's attempt budget remains the hard cap.
		return scopedCandidates(cands, len(cands), scope), nil
	case AdapterSite, AdapterStatic:
		cands, _, derr := s.ScopedStaticBatch(ctx, sourceID, wanted, scope)
		return cands, derr
	default:
		return nil, domain.InvalidRequest(
			"source %q is a %q channel; only live or static sources can refill",
			sourceID, src.AdapterType)
	}
}

// StaticCandidateCurrent is the bool-only guard the acquire finalization
// surface expects. It also verifies the source is still a static adapter so a
// batch ID cannot be replayed after an adapter/config transition.
func (s *IngestService) StaticCandidateCurrent(sourceID string, sourceConfigVersion int64,
	batchID string) bool {
	if batchID == "" {
		return false
	}
	// Keep this helper purely in-memory. The database source enable/version
	// guard is enforced by the final delivery transaction; an unbounded DB
	// lookup here could outlive the request's total deadline.
	return sourceConfigVersion > 0 && s.StaticBatchCurrent(sourceID, batchID)
}

// Batches walks at most maxRefillBatches sources and one bounded vendor
// batch per source. It stops once enough in-scope candidates are available,
// so one slow or broken channel cannot consume the remaining request
// budget. Failed channels are skipped; the next channel can still supply.
func (s *IngestService) Batches(ctx context.Context, sourceIDs []string, wanted int,
	scope RefillScope) ([]domain.Candidate, *domain.Error) {
	if wanted < 1 {
		wanted = 1
	}
	out := make([]domain.Candidate, 0, wanted)
	seen := make(map[string]bool, wanted)
	var lastErr *domain.Error
	attempted := 0
	for _, sourceID := range sourceIDs {
		if attempted >= maxRefillBatches || ctx.Err() != nil {
			break
		}
		attempted++
		// Always fetch a small overflow batch for a single provider source:
		// the first candidate may be dead, and the request must be able to
		// try the next result from the same vendor batch without immediately
		// treating a bad first row as exhausted supply.
		src, srcErr := s.store.GetSource(ctx, sourceID)
		requestWanted := wanted - len(out)
		isProvider := srcErr == nil && src.AdapterType == AdapterProvider
		if isProvider && requestWanted < 3 {
			requestWanted = 3
		}
		if requestWanted < 1 {
			requestWanted = 1
		}
		cands, derr := s.Scoped(ctx, sourceID, requestWanted, scope)
		if derr != nil {
			lastErr = derr
			continue
		}
		for _, cand := range cands {
			key := candidateFingerprint(s.secret, cand)
			if seen[key] {
				continue
			}
			seen[key] = true
			out = append(out, cand)
			if len(out) >= wanted {
				// Provider batches are independent attempts: one invalid
				// first candidate must not hide the next eligible API source.
				// Static lists can stop immediately because their batch is
				// already a multi-candidate sample.
				if !isProvider {
					return out, nil
				}
			}
		}
	}
	if len(out) == 0 && lastErr != nil {
		return nil, lastErr
	}
	return out, nil
}

// StaticSourcesForRefill is the supply-side view of currently eligible
// static channels. It applies project allowlist semantics here as a defense
// in depth; acquire also rechecks source state and config version.
func (s *IngestService) StaticSourcesForRefill(ctx context.Context, projectID string,
	limit int) ([]string, error) {
	var allowed []string
	restricted := false
	if projectID != "" {
		proj, err := s.store.GetProject(ctx, projectID)
		if err != nil {
			return nil, err
		}
		if !proj.Enabled {
			return nil, nil
		}
		allowed = proj.AllowedSourceIDs
		restricted = allowed != nil
	}
	if restricted && len(allowed) == 0 {
		return nil, nil
	}
	return s.store.StaticSourcesForRefill(ctx, s.Now(), allowed, limit)
}

func scopedCandidates(cands []domain.Candidate, wanted int, scope RefillScope) []domain.Candidate {
	if wanted < 1 {
		wanted = 1
	}
	out := make([]domain.Candidate, 0, wanted)
	for _, cand := range cands {
		if !scopeSupportsCandidate(scope, cand) {
			continue
		}
		out = append(out, cand)
		if len(out) >= wanted {
			break
		}
	}
	return out
}

func scopeSupportsCandidate(scope RefillScope, cand domain.Candidate) bool {
	if len(scope.Transports) > 0 {
		ok := false
		for _, t := range scope.Transports {
			if cand.Resource.Conn.Transport == t {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	if len(scope.Countries) == 0 {
		return true
	}
	// Unknown exits must still be probed: the judge supplies the fresh exit
	// observation and acquire applies the strict country check afterward.
	// Vendor labels are not validation evidence.
	return cand.Observation == nil || cand.Observation.CountryCode == "" ||
		countryMatches(scope.Countries, cand.Observation)
}

func candidateFingerprint(secret string, cand domain.Candidate) string {
	if cand.Fingerprint != "" {
		return cand.Fingerprint
	}
	return domain.ConnectionFingerprint(secret, cand.Resource.Conn.Transport,
		cand.Resource.Conn.Host, cand.Resource.Conn.Port,
		cand.Resource.Conn.Username, cand.Resource.Conn.Password)
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
		static:         newStaticCache(DefaultStaticCacheTTL),
		Now:            time.Now,
	}
}

// ConfigureStaticCache applies the live deployment's TTL and memory caps.
// Main calls this after construction so the visible config fields control the
// actual cache rather than an unrelated built-in default.
func (s *IngestService) ConfigureStaticCache(ttl time.Duration, maxEntries int,
	maxBytes int64) error {
	if ttl <= 0 {
		return fmt.Errorf("static cache ttl must be positive")
	}
	if maxEntries < 1 {
		return fmt.Errorf("static cache max entries must be positive")
	}
	if maxBytes < 1024 {
		return fmt.Errorf("static cache max bytes must be at least 1024")
	}
	next := newStaticCacheWithLimits(ttl, maxEntries, maxBytes)
	s.static.mu.Lock()
	next.entries = s.static.entries
	next.inFlight = s.static.inFlight
	next.lastRefresh = s.static.lastRefresh
	next.minInterval = s.static.minInterval
	ordered := make(map[string]bool, len(next.entries))
	for _, id := range s.static.order {
		if entry, ok := next.entries[id]; ok && !ordered[id] {
			next.order = append(next.order, id)
			next.bytes += staticEntryBytes(entry)
			ordered[id] = true
		}
	}
	// Invalidated entries can leave keys only in the old order queue. Avoid
	// carrying those keys into the live cache after a limit change.
	for id, entry := range next.entries {
		if !ordered[id] {
			next.order = append(next.order, id)
			next.bytes += staticEntryBytes(entry)
		}
	}
	// Evict immediately if the new configured byte cap is smaller than the
	// already cached data.
	for len(next.entries) > next.maxEntries || next.bytes > next.maxBytes {
		if len(next.order) == 0 {
			break
		}
		next.removeEntryLocked(next.order[0])
	}
	// Never copy the struct containing sync.Mutex: the assignment would
	// replace the very lock we still need to unlock. Swap data fields only.
	s.static.ttl = next.ttl
	s.static.minInterval = next.minInterval
	s.static.maxEntries = next.maxEntries
	s.static.maxBytes = next.maxBytes
	s.static.bytes = next.bytes
	s.static.entries = next.entries
	s.static.inFlight = next.inFlight
	s.static.lastRefresh = next.lastRefresh
	s.static.order = next.order
	s.static.mu.Unlock()
	return nil
}

// buildSource validates a channel instance against its adapter and renders
// the stored config JSON. It performs no I/O and never fetches.
func (s *IngestService) buildSource(id string, cfg SourceConfig, enabled bool) (domain.Source, *domain.Error) {
	if !sourceIDPattern.MatchString(id) {
		return domain.Source{}, domain.InvalidRequest("source_id must match [a-z0-9][a-z0-9_-]{0,63}")
	}
	adapterType, parserType := AdapterCustom, ParserLines
	switch {
	case cfg.Provider != nil:
		if err := cfg.Provider.scdnConfig().Validate(); err != nil {
			return domain.Source{}, domain.InvalidRequest("provider channel config: %v", err)
		}
		adapterType, parserType = AdapterProvider, ParserJSONMapping
	case cfg.Site != nil:
		if _, ok := sites.ByID(cfg.Site.SiteID); !ok {
			return domain.Source{}, domain.InvalidRequest("unknown site_id %q; see GET /v1/admin/sites", cfg.Site.SiteID)
		}
		if cfg.Site.MaxPages < 0 || cfg.Site.MaxPages > sites.MaxPages {
			return domain.Source{}, domain.InvalidRequest("max_pages must be within 1..%d", sites.MaxPages)
		}
		adapterType, parserType = AdapterSite, ParserSite
	case cfg.Static != nil:
		u, err := url.Parse(strings.TrimSpace(cfg.Static.URL))
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return domain.Source{}, domain.InvalidRequest(
				"static channel requires an absolute http(s) URL")
		}
		if cfg.Static.DefaultTransport == "" {
			cfg.Static.DefaultTransport = domain.TransportHTTP
		}
		switch cfg.Static.DefaultTransport {
		case domain.TransportHTTP, domain.TransportSOCKS5:
		default:
			return domain.Source{}, domain.UnsupportedTransport(string(cfg.Static.DefaultTransport))
		}
		adapterType, parserType = AdapterStatic, ParserLines
	default:
		if cfg.DefaultTransport == "" {
			cfg.DefaultTransport = domain.TransportHTTP
		}
		switch cfg.DefaultTransport {
		case domain.TransportHTTP, domain.TransportSOCKS5:
		default:
			return domain.Source{}, domain.UnsupportedTransport(string(cfg.DefaultTransport))
		}
	}
	if cfg.MaxPoolLeases <= 0 {
		cfg.MaxPoolLeases = 1
	}
	configJSON, err := json.Marshal(cfg)
	if err != nil {
		return domain.Source{}, domain.AsError(err)
	}
	return domain.Source{
		ID:          id,
		Tenant:      "default",
		AdapterType: adapterType,
		ParserType:  parserType,
		Enabled:     enabled,
		ConfigJSON:  string(configJSON),
	}, nil
}

// CreateSource registers a new channel instance. It persists the definition
// only: no fetch, no probe, no inventory rows.
func (s *IngestService) CreateSource(ctx context.Context, id string, cfg SourceConfig,
	enabled bool) (domain.Source, *domain.Error) {
	src, derr := s.buildSource(id, cfg, enabled)
	if derr != nil {
		return domain.Source{}, derr
	}
	if err := s.store.CreateSource(ctx, src, s.Now()); err != nil {
		return domain.Source{}, domain.AsError(err)
	}
	s.static.invalidate(id)
	return s.GetSource(ctx, id)
}

// DeleteSource removes one channel and immediately drops its in-memory
// static batch. Storage invalidates session snapshots in the same deletion
// transaction; a later create with the same ID starts from a fresh version.
func (s *IngestService) DeleteSource(ctx context.Context, id string) (int, *domain.Error) {
	removed, err := s.store.DeleteSource(ctx, id, s.Now())
	if err != nil {
		return 0, domain.AsError(err)
	}
	s.static.invalidate(id)
	return removed, nil
}

// GetSource returns one channel definition.
func (s *IngestService) GetSource(ctx context.Context, id string) (domain.Source, *domain.Error) {
	src, err := s.store.GetSource(ctx, id)
	if err != nil {
		return domain.Source{}, domain.AsError(err)
	}
	return src, nil
}

// ListSources returns all channel definitions.
func (s *IngestService) ListSources(ctx context.Context) ([]domain.Source, *domain.Error) {
	srcs, err := s.store.ListSources(ctx)
	if err != nil {
		return nil, domain.AsError(err)
	}
	return srcs, nil
}

// UpdateSource changes a channel instance's config and/or enabled state
// guarded by the expected config_version. The adapter type is part of the
// channel's identity: switching it requires delete + create.
func (s *IngestService) UpdateSource(ctx context.Context, id string, cfg SourceConfig,
	enabled bool, expectedVersion int64) (domain.Source, *domain.Error) {
	existing, err := s.store.GetSource(ctx, id)
	if err != nil {
		return domain.Source{}, domain.AsError(err)
	}
	src, derr := s.buildSource(id, cfg, enabled)
	if derr != nil {
		return domain.Source{}, derr
	}
	if src.AdapterType != existing.AdapterType {
		return domain.Source{}, domain.InvalidRequest(
			"source %q cannot change adapter type from %q to %q; delete and create it again",
			id, existing.AdapterType, src.AdapterType)
	}
	if expectedVersion <= 0 {
		expectedVersion = existing.ConfigVersion
	}
	src.Tenant = existing.Tenant
	if err := s.store.UpdateSource(ctx, src, expectedVersion, s.Now()); err != nil {
		return domain.Source{}, domain.AsError(err)
	}
	s.static.invalidate(id)
	return s.GetSource(ctx, id)
}

// SetSourceEnabled flips the channel's enabled state under the expected
// version, keeping the stored config untouched.
func (s *IngestService) SetSourceEnabled(ctx context.Context, id string, enabled bool,
	expectedVersion int64) (domain.Source, *domain.Error) {
	existing, err := s.store.GetSource(ctx, id)
	if err != nil {
		return domain.Source{}, domain.AsError(err)
	}
	if expectedVersion <= 0 {
		expectedVersion = existing.ConfigVersion
	}
	var cfg SourceConfig
	if err := json.Unmarshal([]byte(existing.ConfigJSON), &cfg); err != nil {
		return domain.Source{}, domain.AsError(fmt.Errorf("decode source config: %w", err))
	}
	src, derr := s.buildSource(id, cfg, enabled)
	if derr != nil {
		return domain.Source{}, derr
	}
	src.Tenant = existing.Tenant
	if err := s.store.UpdateSource(ctx, src, expectedVersion, s.Now()); err != nil {
		return domain.Source{}, domain.AsError(err)
	}
	s.static.invalidate(id)
	return s.GetSource(ctx, id)
}

func (s *IngestService) EnsureSource(ctx context.Context, id string, cfg SourceConfig, enabled bool) error {
	// Internal bootstrap path: legacy callers expect upsert semantics. The
	// public API uses CreateSource/UpdateSource, which never overwrite.
	src, derr := s.buildSource(id, cfg, enabled)
	if derr != nil {
		return derr
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
	if sourceID == "" {
		return ImportResult{}, domain.InvalidRequest("source_id is required")
	}
	src, err := s.store.GetSource(ctx, sourceID)
	if err != nil {
		return ImportResult{}, domain.AsError(err)
	}
	if src.AdapterType != AdapterCustom {
		return ImportResult{}, domain.InvalidRequest(
			"source %q is a %q channel; only custom upload sources accept imported lines",
			sourceID, src.AdapterType)
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
	SourceID    string          `json:"source_id"`
	Requested   int             `json:"requested"`
	Fetched     int             `json:"fetched"`
	BatchID     string          `json:"batch_id,omitempty"`
	NotModified bool            `json:"not_modified,omitempty"`
	Added       int             `json:"added"`
	Updated     int             `json:"updated"`
	Skipped     int             `json:"skipped"`
	Rejected    []FetchRowError `json:"rejected,omitempty"`
	PagesDone   int             `json:"pages_done,omitempty"`
	PagesTotal  int             `json:"pages_total,omitempty"`
	Diagnostic  bool            `json:"diagnostic,omitempty"`
}

// FetchRowError is the shared per-row failure shape for provider batches and
// site pages; it keeps the same JSON fields as the old scdn entry errors.
type FetchRowError struct {
	Line  int    `json:"line"`
	Input string `json:"input"`
	Error string `json:"error"`
}

// FetchSource runs one bounded vendor call for a provider-API channel as a
// configuration diagnostic: it reports what the vendor returned but imports
// nothing. On-demand supply happens inside acquire (FetchOnDemand); provider
// proxies never enter the pool.
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
	return FetchResult{
		SourceID:   sourceID,
		Requested:  batch.Requested,
		Fetched:    len(batch.Entries),
		Diagnostic: true,
		Rejected:   toFetchRowErrors(batch.Errors),
	}, nil
}

// FetchOnDemand pulls one vendor batch for a live acquire request and returns
// request-local candidates. Nothing is written to the inventory: entries carry
// deterministic fingerprints so reservations, leases, and consumer CD unify
// with identical static connections, and unused entries die with the request.
func (s *IngestService) FetchOnDemand(ctx context.Context, sourceID string, wanted int) ([]domain.Candidate, *domain.Error) {
	s.fetchMu.Lock()
	if s.fetchBusy[sourceID] {
		s.fetchMu.Unlock()
		return nil, domain.E("SOURCE_FETCH_IN_PROGRESS", 409,
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
		return nil, domain.AsError(err)
	}
	if src.AdapterType != AdapterProvider {
		return nil, domain.InvalidRequest(
			"source %q is a %q channel; only provider_api sources support on-demand fetch",
			sourceID, src.AdapterType)
	}
	if !src.Enabled {
		return nil, domain.InvalidRequest("source %q is disabled", sourceID)
	}
	var cfg SourceConfig
	if err := json.Unmarshal([]byte(src.ConfigJSON), &cfg); err != nil {
		return nil, domain.AsError(fmt.Errorf("decode source config: %w", err))
	}
	if cfg.Provider == nil {
		return nil, domain.InvalidRequest("source %q has no provider config", sourceID)
	}
	now := s.Now()
	s.fetchMu.Lock()
	last := s.lastFetchAt[sourceID]
	s.fetchMu.Unlock()
	if wait := cfg.Provider.minFetchInterval() - now.Sub(last); wait > 0 {
		return nil, domain.E("SOURCE_RATE_LIMITED", 429,
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
		return nil, mapScdnError(ferr)
	}
	if cfg.MaxPoolLeases <= 0 {
		cfg.MaxPoolLeases = 1
	}
	batchIncarnation := domain.NewID("ondemand_")
	seen := map[string]bool{}
	out := make([]domain.Candidate, 0, len(batch.Entries))
	for _, entry := range batch.Entries {
		conn := domain.Connection{Transport: entry.Transport, Host: entry.Host, Port: entry.Port}
		norm := s.buildCandidate(src, cfg, conn, now)
		if seen[norm.ResourceID] {
			continue
		}
		seen[norm.ResourceID] = true
		norm.Generation = "ondemand"
		norm.OfferID = ""
		norm.LocalRetentionDeadline = nil
		norm.ExpiryBasis = "ondemand"
		norm.Claims = map[string]string{"vendor_protocol": cfg.Provider.Protocol,
			"batch_id": batchIncarnation}
		cand := candidateFromNormalized(norm)
		cand.Ephemeral = true
		// The request-local candidate carries the real channel config
		// version and batch incarnation so reservation/evidence keys bind to
		// exactly the configuration that produced it.
		cand.SourceConfigVersion = src.ConfigVersion
		cand.Incarnation = batchIncarnation
		out = append(out, cand)
		if wanted > 0 && len(out) >= wanted {
			break
		}
	}
	return out, nil
}

func candidateFromNormalized(c domain.NormalizedCandidate) domain.Candidate {
	return domain.Candidate{
		Offer: domain.Offer{
			ID:                     c.OfferID,
			SourceID:               c.SourceID,
			ResourceID:             c.ResourceID,
			SourceRecordID:         c.RecordID,
			SourceGeneration:       c.Generation,
			ProviderExpiresAt:      c.ProviderExpiresAt,
			LocalRetentionDeadline: c.LocalRetentionDeadline,
			ExpiryBasis:            c.ExpiryBasis,
			Claims:                 c.Claims,
			MaxPoolLeases:          c.MaxPoolLeases,
			State:                  domain.OfferEnabled,
		},
		Resource: domain.Resource{
			ID:                c.ResourceID,
			Tenant:            "default",
			Conn:              c.Conn,
			CredentialVersion: c.CredentialVersion,
			Kind:              c.Kind,
			Session:           c.Session,
		},
	}
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

// fetchSite walks one catalog site's pages into the bounded static cache and
// returns a diagnostic report. Site entries never enter the persistent pool:
// acquire serves them from the cache and validation still decides delivery.
func (s *IngestService) fetchSite(ctx context.Context, src domain.Source, sourceID string) (FetchResult, *domain.Error) {
	res, derr := s.RefreshStaticSource(ctx, sourceID)
	if derr != nil {
		return FetchResult{}, derr
	}
	return FetchResult{
		SourceID:    sourceID,
		Fetched:     res.Fetched,
		BatchID:     res.BatchID,
		NotModified: res.NotModified,
		Rejected:    res.Rejected,
		PagesDone:   res.PagesDone,
		PagesTotal:  res.PagesTotal,
		Diagnostic:  true,
	}, nil
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
