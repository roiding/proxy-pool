package application

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"proxypool/internal/domain"
	"proxypool/internal/integrations/custom"
	"proxypool/internal/integrations/sites"
	"proxypool/internal/integrations/staticdl"
)

const (
	// DefaultStaticCacheTTL bounds how long a fetched list stays fresh.
	// Static lists are refetchable, so the cache is deliberately not durable.
	DefaultStaticCacheTTL = 10 * time.Minute
	// maxStaticSources bounds the number of cached channels; the oldest
	// entry evicts when a new one pushes past the cap.
	maxStaticSources = 64
	// maxStaticCandidatesPerSource bounds one cached list.
	maxStaticCandidatesPerSource = 1000
	// maxStaticRateWindows retains recently refreshed source IDs without
	// making the cache entry count erase a still-active vendor rate window.
	maxStaticRateWindows = 4096
)

// StaticRefreshResult reports one cache refresh. It never implies any proxy
// is alive: entries are unvalidated until a probe passes.
type StaticRefreshResult struct {
	SourceID    string          `json:"source_id"`
	BatchID     string          `json:"batch_id"`
	Fetched     int             `json:"fetched"`
	Rejected    []FetchRowError `json:"rejected,omitempty"`
	NotModified bool            `json:"not_modified,omitempty"`
	PagesDone   int             `json:"pages_done,omitempty"`
	PagesTotal  int             `json:"pages_total,omitempty"`
}

type staticEntry struct {
	batchID      string
	cachedAt     time.Time
	etag         string
	lastModified string
	candidates   []domain.Candidate
	rejected     []FetchRowError
	pagesDone    int
	pagesTotal   int
}

type staticFlight struct {
	done chan struct{}
	res  StaticRefreshResult
	derr *domain.Error
}

type staticCache struct {
	mu          sync.Mutex
	ttl         time.Duration
	minInterval time.Duration
	maxEntries  int
	maxBytes    int64
	bytes       int64
	entries     map[string]*staticEntry
	inFlight    map[string]*staticFlight
	lastRefresh map[string]time.Time
	order       []string
}

func newStaticCache(ttl time.Duration) *staticCache {
	return newStaticCacheWithLimits(ttl, maxStaticSources, 4<<20)
}

func newStaticCacheWithLimits(ttl time.Duration, maxEntries int,
	maxBytes int64) *staticCache {
	if maxEntries < 1 {
		maxEntries = 1
	}
	if maxBytes < 1 {
		maxBytes = 1
	}
	return &staticCache{
		ttl:         ttl,
		maxEntries:  maxEntries,
		maxBytes:    maxBytes,
		entries:     map[string]*staticEntry{},
		inFlight:    map[string]*staticFlight{},
		lastRefresh: map[string]time.Time{},
	}
}

func (c *staticCache) get(sourceID string, now time.Time) (*staticEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[sourceID]
	if !ok || now.Sub(e.cachedAt) >= c.ttl {
		return nil, false
	}
	return e, true
}

// entryAny returns a retained entry even when its TTL has expired. It is
// only for conditional revalidation and 304 refresh: expired candidates are
// never directly served from this path.
func (c *staticCache) entryAny(sourceID string) (*staticEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[sourceID]
	return e, ok
}

func (c *staticCache) validators(sourceID string) (etag, lastModified string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[sourceID]
	if !ok {
		return "", ""
	}
	return e.etag, e.lastModified
}

func (c *staticCache) batchID(sourceID string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.entries[sourceID]; ok {
		return e.batchID
	}
	return ""
}

func (c *staticCache) beginRefresh(sourceID string) (*staticFlight, bool, *domain.Error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if f, ok := c.inFlight[sourceID]; ok {
		return f, false, nil
	}
	if len(c.inFlight) >= c.maxEntries {
		return nil, false, domain.E("STATIC_CACHE_CAPACITY", 503,
			"static cache has %d downloads in flight", len(c.inFlight))
	}
	f := &staticFlight{done: make(chan struct{})}
	c.inFlight[sourceID] = f
	return f, true, nil
}

// waitRefresh shares the leader's real result with a follower. A follower's
// context only governs how long it waits; the singleflight download itself
// stays alive for the leader's caller.
func waitRefresh(ctx context.Context, f *staticFlight) (StaticRefreshResult, *domain.Error) {
	select {
	case <-f.done:
		return f.res, f.derr
	case <-ctx.Done():
		return StaticRefreshResult{}, domain.E("REQUEST_CANCELED", 499,
			"static refresh was canceled while waiting for an in-flight download: %v", ctx.Err())
	}
}

// invalidate removes a live cached batch after source config or enable-state
// changes. Older candidates already handed to acquire remain visible only to
// their in-flight request and are rejected at commit by StaticBatchCurrent.
func (c *staticCache) invalidate(sourceID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeEntryLocked(sourceID)
}

// finishRefresh closes the flight and installs the new entry. Every flight
// records its result exactly once, so followers never observe zero values as
// a false success.
func (c *staticCache) finishRefresh(sourceID string, f *staticFlight,
	entry *staticEntry, stamp bool, now time.Time) {
	c.finishRefreshResult(sourceID, f, entry, StaticRefreshResult{}, nil, stamp, now)
}

func (c *staticCache) finishRefreshResult(sourceID string, f *staticFlight,
	entry *staticEntry, res StaticRefreshResult, derr *domain.Error, stamp bool,
	now time.Time) {
	if f == nil {
		return
	}
	c.mu.Lock()
	if cur, ok := c.inFlight[sourceID]; ok && cur == f {
		delete(c.inFlight, sourceID)
	}
	if entry != nil {
		c.installEntryLocked(sourceID, entry)
	}
	if stamp || entry != nil {
		// A failed refresh is not a download rate observation. Only a real
		// batch installation or an explicit stamp starts a vendor window.
		c.lastRefresh[sourceID] = now
	}
	for id := range c.lastRefresh {
		if now.Sub(c.lastRefresh[id]) >= c.minInterval && c.minInterval > 0 {
			delete(c.lastRefresh, id)
		}
	}
	for len(c.lastRefresh) > maxStaticRateWindows {
		oldestID := ""
		var oldest time.Time
		for id, at := range c.lastRefresh {
			if oldestID == "" || at.Before(oldest) {
				oldestID, oldest = id, at
			}
		}
		delete(c.lastRefresh, oldestID)
	}
	for len(c.entries) > c.maxEntries || c.bytes > c.maxBytes {
		if len(c.order) == 0 {
			break
		}
		c.removeEntryLocked(c.order[0])
	}
	f.res = res
	f.derr = derr
	c.mu.Unlock()
	close(f.done)
}

// removeEntryLocked keeps every accounting field in sync. Callers hold c.mu.
func (c *staticCache) removeEntryLocked(sourceID string) {
	old, ok := c.entries[sourceID]
	if ok {
		c.bytes -= staticEntryBytes(old)
		delete(c.entries, sourceID)
	}
	if len(c.order) == 0 {
		return
	}
	next := c.order[:0]
	for _, id := range c.order {
		if id != sourceID {
			next = append(next, id)
		}
	}
	c.order = next
}

// installEntryLocked replaces one source's entry without leaving its old
// bytes or order position behind. Reinstalling moves it to the newest slot.
func (c *staticCache) installEntryLocked(sourceID string, entry *staticEntry) {
	c.removeEntryLocked(sourceID)
	c.entries[sourceID] = entry
	c.bytes += staticEntryBytes(entry)
	c.order = append(c.order, sourceID)
}

func staticEntryBytes(e *staticEntry) int64 {
	if e == nil {
		return 0
	}
	var n int64
	for _, cand := range e.candidates {
		n += int64(len(cand.Resource.Conn.Host) + len(cand.Resource.Conn.Username) +
			len(cand.Resource.Conn.Password) + len(cand.Fingerprint) +
			len(cand.Incarnation))
		for k, v := range cand.Offer.Claims {
			n += int64(len(k) + len(v))
		}
	}
	for _, rej := range e.rejected {
		n += int64(len(rej.Input) + len(rej.Error))
	}
	return n
}

func (c *staticCache) rateLimited(sourceID string, minInterval time.Duration, now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if minInterval > c.minInterval {
		c.minInterval = minInterval
	}
	return now.Sub(c.lastRefresh[sourceID]) < minInterval
}

// CachedStaticCandidates returns the current fresh batch for acquire. The
// second return is false when the cache is absent or TTL-expired; stale
// batches must never silently serve.
func (s *IngestService) CachedStaticCandidates(sourceID string) ([]domain.Candidate, string, bool) {
	entry, ok := s.static.get(sourceID, s.Now())
	if !ok {
		return nil, "", false
	}
	return entry.candidates, entry.batchID, true
}

// CurrentStaticBatch exposes the batch incarnation so a commit that started
// from an older batch can be rejected after a refresh or config change.
func (s *IngestService) CurrentStaticBatch(sourceID string) string {
	return s.static.batchID(sourceID)
}

// StaticBatchCurrent reports whether batchID is still the live batch of the
// source. A commit whose candidate carries an older incarnation must be
// rejected: the batch was replaced (or the source refreshed) in the meantime.
func (s *IngestService) StaticBatchCurrent(sourceID, batchID string) bool {
	if batchID == "" {
		return false
	}
	return s.static.batchID(sourceID) == batchID
}

// FetchStaticBatch serves one acquire request from a fresh static batch: it
// refreshes (single-flight, rate-limited) when the cache is stale, then
// returns up to wanted request-local candidates plus the batch incarnation.
// Nothing is written to the inventory; callers must recheck the batch before
// commit via StaticBatchCurrent.
func (s *IngestService) FetchStaticBatch(ctx context.Context, sourceID string,
	wanted int) ([]domain.Candidate, string, *domain.Error) {
	src, derr := s.store.GetSource(ctx, sourceID)
	if derr != nil {
		return nil, "", domain.AsError(derr)
	}
	if src.AdapterType != AdapterSite && src.AdapterType != AdapterStatic {
		return nil, "", domain.InvalidRequest(
			"source %q is a %q channel; only static sources can supply batches",
			sourceID, src.AdapterType)
	}
	if !src.Enabled {
		return nil, "", domain.InvalidRequest("source %q is disabled", sourceID)
	}
	if _, ok := s.static.get(sourceID, s.Now()); !ok {
		if _, derr := s.RefreshStaticSource(ctx, sourceID); derr != nil {
			return nil, "", derr
		}
	}
	entry, ok := s.static.get(sourceID, s.Now())
	if !ok {
		return nil, "", domain.E("SOURCE_STALE", 503,
			"source %q has no fresh static batch", sourceID)
	}
	if wanted > 0 && len(entry.candidates) > wanted {
		return entry.candidates[:wanted], entry.batchID, nil
	}
	return entry.candidates, entry.batchID, nil
}

// RefreshStaticSource refreshes the bounded static cache for one site_fetch
// or static_url channel. It never writes inventory rows: cached candidates
// are request-local until acquire validates and commits them.
func (s *IngestService) RefreshStaticSource(ctx context.Context, sourceID string) (StaticRefreshResult, *domain.Error) {
	src, err := s.store.GetSource(ctx, sourceID)
	if err != nil {
		return StaticRefreshResult{}, domain.AsError(err)
	}
	if src.AdapterType != AdapterSite && src.AdapterType != AdapterStatic {
		return StaticRefreshResult{}, domain.InvalidRequest(
			"source %q is a %q channel; only site_fetch and static_url support static caching",
			sourceID, src.AdapterType)
	}
	if !src.Enabled {
		return StaticRefreshResult{}, domain.InvalidRequest("source %q is disabled", sourceID)
	}
	beforeVersion := src.ConfigVersion
	var cfg SourceConfig
	if err := json.Unmarshal([]byte(src.ConfigJSON), &cfg); err != nil {
		return StaticRefreshResult{}, domain.AsError(fmt.Errorf("decode source config: %w", err))
	}
	f, leader, capErr := s.static.beginRefresh(sourceID)
	if capErr != nil {
		return StaticRefreshResult{}, capErr
	}
	if !leader {
		// Concurrent callers collapse into the in-flight download instead of
		// hammering the list server.
		return waitRefresh(ctx, f)
	}
	if s.static.rateLimited(sourceID, s.staticMinInterval(src), s.Now()) {
		// A fresh batch satisfies the refresh without another download.
		// Callers that reach beginRefresh just after the previous flight
		// finished (straggler followers or sequential retries) otherwise
		// receive a false SOURCE_RATE_LIMITED while the cache is usable.
		if entry, ok := s.static.get(sourceID, s.Now()); ok {
			res := StaticRefreshResult{SourceID: sourceID, BatchID: entry.batchID,
				Fetched: len(entry.candidates), NotModified: true,
				PagesDone: entry.pagesDone, PagesTotal: entry.pagesTotal}
			s.static.finishRefreshResult(sourceID, f, nil, res, nil, false, s.Now())
			return res, nil
		}
		derr := domain.E("SOURCE_RATE_LIMITED", 429,
			"source %q was refreshed recently; retry later", sourceID)
		s.static.finishRefreshResult(sourceID, f, nil, StaticRefreshResult{}, derr,
			false, s.Now())
		return StaticRefreshResult{}, derr
	}
	res, entry, derr := s.downloadStatic(ctx, src, cfg, sourceID)
	// A refresh raced with source reconfiguration. Do not install a batch
	// downloaded under stale credentials, URLs, pagination limits, or parser
	// settings; the config update has already invalidated the old batch.
	if derr == nil || entry != nil {
		current, cerr := s.store.GetSource(ctx, sourceID)
		if cerr != nil {
			derr, entry = domain.AsError(cerr), nil
		} else if current.ConfigVersion != beforeVersion || !current.Enabled ||
			!sameStaticSourceIdentity(current, src) {
			derr = domain.E("SOURCE_CONFIG_CHANGED", 409,
				"source %q changed during refresh", sourceID)
			entry = nil
		}
	}
	if derr != nil {
		s.static.finishRefreshResult(sourceID, f, nil, StaticRefreshResult{}, derr,
			true, s.Now())
		return StaticRefreshResult{}, derr
	}
	s.static.finishRefreshResult(sourceID, f, entry, *res, nil, true, s.Now())
	return *res, nil
}

// ScopedStaticBatch serves a fresh static batch narrowed to the request's
// transport and country filters. It re-reads the source each time, so a
// disabled or reconfigured channel cannot keep using a snapshot that loaded
// before the edit. The returned candidates stay request-local.
func (s *IngestService) ScopedStaticBatch(ctx context.Context, sourceID string,
	wanted int, scope RefillScope) ([]domain.Candidate, string, *domain.Error) {
	cands, batchID, derr := s.FetchStaticBatch(ctx, sourceID, 0)
	if derr != nil {
		return nil, "", derr
	}
	src, err := s.store.GetSource(ctx, sourceID)
	if err != nil {
		return nil, "", domain.AsError(err)
	}
	if !src.Enabled {
		return nil, "", domain.InvalidRequest("source %q is disabled", sourceID)
	}
	var cfg SourceConfig
	if err := json.Unmarshal([]byte(src.ConfigJSON), &cfg); err != nil {
		return nil, "", domain.AsError(fmt.Errorf("decode source config: %w", err))
	}
	capacity := len(cands)
	if wanted > 0 && wanted < capacity {
		capacity = wanted
	}
	out := make([]domain.Candidate, 0, capacity)
	for _, cand := range cands {
		if cand.SourceConfigVersion != src.ConfigVersion ||
			cand.Incarnation != batchID {
			return nil, "", domain.E("SOURCE_CONFIG_CHANGED", 409,
				"source %q changed while its static batch was loading", sourceID)
		}
		if !scopeSupportsCandidate(scope, cand) {
			continue
		}
		out = append(out, cand)
		if wanted > 0 && len(out) >= wanted {
			break
		}
	}
	return out, batchID, nil
}

// sameStaticSourceIdentity rejects a cache snapshot whose config (not merely
// version) no longer matches storage. Versions normally cover this, but the
// explicit comparison also protects legacy rows and race fixtures.
func sameStaticSourceIdentity(a, b domain.Source) bool {
	return a.ID == b.ID && a.Tenant == b.Tenant && a.AdapterType == b.AdapterType &&
		a.ParserType == b.ParserType && a.Enabled == b.Enabled &&
		a.ConfigVersion == b.ConfigVersion && a.ConfigJSON == b.ConfigJSON
}

func (s *IngestService) staticMinInterval(src domain.Source) time.Duration {
	if src.AdapterType == AdapterSite {
		return defaultSiteFetchInterval
	}
	return defaultMinFetchIntervalMS * time.Millisecond
}

func (s *IngestService) downloadStatic(ctx context.Context, src domain.Source,
	cfg SourceConfig, sourceID string) (*StaticRefreshResult, *staticEntry, *domain.Error) {
	now := s.Now()
	etag, lastModified := s.static.validators(sourceID)
	var (
		entries    []sites.Entry
		rejected   []FetchRowError
		pagesDone  int
		pagesTotal int
		notMod     bool
		ferr       *domain.Error
	)
	switch src.AdapterType {
	case AdapterSite:
		ferr = s.fetchSiteEntries(ctx, cfg, sourceID, &entries, &rejected, &pagesDone, &pagesTotal)
	case AdapterStatic:
		ferr = s.fetchStaticURLEntries(ctx, cfg, sourceID, &entries, &rejected,
			&etag, &lastModified, &notMod)
	default:
		return nil, nil, domain.InvalidRequest(
			"source %q is a %q channel; only site_fetch and static_url support static caching",
			sourceID, src.AdapterType)
	}
	if ferr != nil {
		return nil, nil, ferr
	}
	if notMod {
		return s.refreshedEntry(sourceID, src, now, etag, lastModified)
	}
	return s.buildStaticEntry(src, cfg, sourceID, now, entries, rejected,
		pagesDone, pagesTotal, etag, lastModified)
}

func (s *IngestService) fetchSiteEntries(ctx context.Context, cfg SourceConfig,
	sourceID string, entries *[]sites.Entry, rejected *[]FetchRowError,
	pagesDone, pagesTotal *int) *domain.Error {
	if cfg.Site == nil || cfg.Site.SiteID == "" {
		return domain.InvalidRequest("source %q has no site config", sourceID)
	}
	site, ok := sites.ByID(cfg.Site.SiteID)
	if !ok {
		return domain.InvalidRequest("unknown site_id %q", cfg.Site.SiteID)
	}
	if override, ok := s.SiteOverrides[cfg.Site.SiteID]; ok {
		site = override
	}
	fetchCtx, cancel := context.WithTimeout(ctx, sites.MinFetchInterval)
	defer cancel()
	out, err := sites.FetchSite(fetchCtx, s.SiteHTTPClient, site, cfg.Site.MaxPages, s.allowPrivate)
	if err != nil {
		return mapSitesError(err)
	}
	*entries = out.Entries
	*pagesDone, *pagesTotal = out.PagesDone, out.PagesTotal
	for _, r := range out.Rejected {
		*rejected = append(*rejected, FetchRowError{Line: r.Line, Input: r.Input, Error: r.Error})
	}
	return nil
}

func (s *IngestService) fetchStaticURLEntries(ctx context.Context, cfg SourceConfig,
	sourceID string, entries *[]sites.Entry, rejected *[]FetchRowError,
	etag, lastModified *string, notModified *bool) *domain.Error {
	if cfg.Static == nil || cfg.Static.URL == "" {
		return domain.InvalidRequest("source %q has no static URL config", sourceID)
	}
	dlCtx, cancel := context.WithTimeout(ctx, staticdl.DefaultTimeout)
	defer cancel()
	result, err := staticdl.Download(dlCtx, cfg.Static.URL, staticdl.Conditional{
		ETag: *etag, IfModifiedSince: *lastModified,
	}, staticdl.Options{AllowPrivate: s.allowPrivate})
	if err != nil {
		return mapStaticDownloadError(err)
	}
	*etag, *lastModified = result.ETag, result.LastModified
	if result.NotModified {
		*notModified = true
		return nil
	}
	parsed, rejects := custom.ParseLines(custom.LinesOptions{
		DefaultTransport: cfg.Static.DefaultTransport,
		AllowPrivate:     s.allowPrivate,
	}, string(result.Body))
	for _, p := range parsed {
		*entries = append(*entries, sites.Entry{Conn: p.Conn})
	}
	for _, r := range rejects {
		*rejected = append(*rejected, FetchRowError{Line: r.Line, Input: r.Input, Error: r.Error})
	}
	return nil
}

// refreshedEntry handles a 304: keep the cached batch, refresh the timestamp
// and validators. The batch ID does not change because the content did not.
func (s *IngestService) refreshedEntry(sourceID string, _ domain.Source,
	now time.Time, etag, lastModified string) (*StaticRefreshResult, *staticEntry, *domain.Error) {
	previous, ok := s.static.entryAny(sourceID)
	if !ok {
		return nil, nil, domain.E("SOURCE_STALE_NOT_MODIFIED", 502,
			"source %q returned 304 but no cached batch exists", sourceID)
	}
	fresh := &staticEntry{
		batchID: previous.batchID, cachedAt: now, etag: etag,
		lastModified: lastModified, candidates: previous.candidates,
		rejected: previous.rejected, pagesDone: previous.pagesDone,
		pagesTotal: previous.pagesTotal,
	}
	return &StaticRefreshResult{SourceID: sourceID, BatchID: fresh.batchID,
		Fetched: len(fresh.candidates), NotModified: true,
		PagesDone: fresh.pagesDone, PagesTotal: fresh.pagesTotal}, fresh, nil
}

// buildStaticEntry normalizes fetched connections into request-local
// candidates. No offer IDs, no fabricated exit TTLs: acquire validates and
// commits before anything is delivered.
func (s *IngestService) buildStaticEntry(src domain.Source, cfg SourceConfig,
	sourceID string, now time.Time, entries []sites.Entry, rejected []FetchRowError,
	pagesDone, pagesTotal int, etag, lastModified string) (*StaticRefreshResult, *staticEntry, *domain.Error) {
	if len(entries) > maxStaticCandidatesPerSource {
		entries = entries[:maxStaticCandidatesPerSource]
	}
	batchID := domain.NewID("static_")
	candidates := make([]domain.Candidate, 0, len(entries))
	seen := map[string]bool{}
	for _, e := range entries {
		norm := s.buildCandidate(src, cfg, e.Conn, now)
		if seen[norm.ResourceID] {
			continue
		}
		seen[norm.ResourceID] = true
		norm.Generation = "static"
		norm.OfferID = ""
		norm.LocalRetentionDeadline = nil
		norm.ExpiryBasis = "static_cache"
		claims := map[string]string{"batch_id": batchID}
		if cfg.Site != nil {
			claims["source_site"] = cfg.Site.SiteID
			claims["anonymity"] = anonymityClaim(cfg.Site.SiteID)
		}
		if e.VendorProtocol != "" {
			claims["vendor_protocol"] = e.VendorProtocol
		}
		norm.Claims = claims
		cand := candidateFromNormalized(norm)
		cand.Ephemeral = true
		cand.SourceConfigVersion = src.ConfigVersion
		cand.Incarnation = batchID
		cand.Fingerprint = norm.RecordID
		candidates = append(candidates, cand)
	}
	entry := &staticEntry{
		batchID: batchID, cachedAt: now, etag: etag, lastModified: lastModified,
		candidates: candidates, rejected: rejected,
		pagesDone: pagesDone, pagesTotal: pagesTotal,
	}
	return &StaticRefreshResult{SourceID: sourceID, BatchID: batchID,
		Fetched: len(candidates), Rejected: rejected,
		PagesDone: pagesDone, PagesTotal: pagesTotal}, entry, nil
}

func anonymityClaim(siteID string) string {
	if site, ok := sites.ByID(siteID); ok {
		return string(site.Anonymity)
	}
	return string(sites.AnonymityUnknown)
}

func mapStaticDownloadError(err error) *domain.Error {
	code := "SOURCE_UNAVAILABLE"
	status := 503
	msg := err.Error()
	if strings.Contains(msg, "exceeds") || strings.Contains(msg, "scheme") ||
		strings.Contains(msg, "rejected by address policy") {
		code = "SOURCE_FORMAT_ERROR"
		status = 502
	}
	return domain.E(code, status, "static list download failed: %s", msg)
}
