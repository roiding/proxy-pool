package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"time"
	"unsafe"

	"proxypool/internal/domain"
	"proxypool/internal/integrations/scdn"
	"proxypool/internal/integrations/sites"
	"proxypool/internal/integrations/sourcehttp"
	"proxypool/internal/integrations/staticdl"
	"proxypool/internal/integrations/webshare"
	"proxypool/internal/storage/sqlite"
)

const (
	// DefaultStaticCacheTTL bounds how long a fetched list stays fresh.
	// Static lists are refetchable, so the cache is deliberately not durable.
	DefaultStaticCacheTTL = 30 * time.Minute
	// maxStaticSources bounds the number of cached channels; the oldest
	// entry evicts when a new one pushes past the cap.
	maxStaticSources = 64
	// maxStaticCandidatesPerSource bounds one cached list.
	maxStaticCandidatesPerSource = 1000
	// maxStaticRateWindows retains recently refreshed source IDs without
	// making the cache entry count erase a still-active vendor rate window.
	maxStaticRateWindows      = 4096
	maxStaticCandidatesGlobal = 4096
	maxStaticProofsPerSource  = 1024
	// A requested output count is not the number of unvalidated rows to try.
	minStaticRefillCandidates = 32
)

// StaticRefreshResult reports one cache refresh. It never implies any proxy
// is alive: entries are unvalidated until a probe passes.
type StaticRefreshResult struct {
	SourceID        string          `json:"source_id"`
	BatchID         string          `json:"batch_id"`
	Fetched         int             `json:"fetched"`
	Unique          int             `json:"unique"`
	Retained        int             `json:"retained"`
	Rejected        []FetchRowError `json:"rejected,omitempty"`
	NotModified     bool            `json:"not_modified,omitempty"`
	CacheHit        bool            `json:"cache_hit,omitempty"`
	PagesDone       int             `json:"pages_done,omitempty"`
	PagesTotal      int             `json:"pages_total,omitempty"`
	ExpiresAt       *time.Time      `json:"expires_at,omitempty"`
	Truncated       int             `json:"truncated,omitempty"`
	SourceUpdatedAt *time.Time      `json:"source_updated_at,omitempty"`
	RejectedCount   int             `json:"rejected_count,omitempty"`
	Complete        bool            `json:"complete"`
	Sampled         bool            `json:"sampled"`
}

type listFetchMetadata struct {
	Fetched         int
	RejectedCount   int
	Complete        bool
	Sampled         bool
	SourceUpdatedAt *time.Time
}

type staticEntry struct {
	batchID         string
	cachedAt        time.Time
	etag            string
	lastModified    string
	candidates      []domain.Candidate
	rejected        []FetchRowError
	pagesDone       int
	pagesTotal      int
	ttl             time.Duration
	cursor          int
	proofs          map[string]staticProof
	sourceUpdatedAt *time.Time
	truncated       int
	configVersion   int64
	rejectedCount   int
	fetched         int
	unique          int
	complete        bool
	sampled         bool
}

type staticFlight struct {
	done        chan struct{}
	res         StaticRefreshResult
	derr        *domain.Error
	invalidated bool
}

type staticCache struct {
	mu            sync.Mutex
	ttl           time.Duration
	minInterval   time.Duration
	maxEntries    int
	maxBytes      int64
	bytes         int64
	entries       map[string]*staticEntry
	inFlight      map[string]*staticFlight
	lastRefresh   map[string]time.Time
	order         []string
	fence         *cacheGate
	candidates    int
	maxCandidates int
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
		ttl:           ttl,
		maxEntries:    maxEntries,
		maxBytes:      maxBytes,
		entries:       map[string]*staticEntry{},
		inFlight:      map[string]*staticFlight{},
		lastRefresh:   map[string]time.Time{},
		fence:         newCacheGate(),
		maxCandidates: maxStaticCandidatesGlobal,
	}
}

func (c *staticCache) hasFresh(sourceID string, now time.Time, versions ...int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[sourceID]
	if !ok || !c.entryFresh(e, now) || len(versions) > 0 && e.configVersion != versions[0] {
		return false
	}
	return true
}

func (c *staticCache) entryFresh(e *staticEntry, now time.Time) bool {
	ttl := e.ttl
	if ttl <= 0 {
		ttl = c.ttl
	}
	return !e.cachedAt.After(now) && now.Sub(e.cachedAt) < ttl
}

func (c *staticCache) expiresAt(e *staticEntry) time.Time {
	ttl := e.ttl
	if ttl <= 0 {
		ttl = c.ttl
	}
	return e.cachedAt.Add(ttl)
}

// freshResult clones metadata under the mutex. No caller may dereference a
// live cache entry after dropping the lock while AUTH removal/304 can mutate it.
func (c *staticCache) freshResult(sourceID string, version int64, now time.Time) (StaticRefreshResult, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[sourceID]
	if !ok || e.configVersion != version || !c.entryFresh(e, now) {
		return StaticRefreshResult{}, false
	}
	expires := c.expiresAt(e)
	return StaticRefreshResult{SourceID: sourceID, BatchID: e.batchID, Fetched: e.fetched, Unique: e.unique, Retained: len(e.candidates), Rejected: append([]FetchRowError(nil), e.rejected...), RejectedCount: e.rejectedCount,
		CacheHit: true, PagesDone: e.pagesDone, PagesTotal: e.pagesTotal, ExpiresAt: &expires, Truncated: e.truncated, Complete: e.complete, Sampled: e.sampled, SourceUpdatedAt: cloneTime(e.sourceUpdatedAt)}, true
}

func cloneTime(at *time.Time) *time.Time {
	if at == nil {
		return nil
	}
	copy := *at
	return &copy
}

func cloneStaticRefreshResult(res StaticRefreshResult) StaticRefreshResult {
	res.Rejected = append([]FetchRowError(nil), res.Rejected...)
	res.ExpiresAt = cloneTime(res.ExpiresAt)
	res.SourceUpdatedAt = cloneTime(res.SourceUpdatedAt)
	return res
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
		return cloneStaticRefreshResult(f.res), f.derr
	case <-ctx.Done():
		return StaticRefreshResult{}, domain.E("REQUEST_CANCELED", 499,
			"static refresh was canceled while waiting for an in-flight download: %v", ctx.Err())
	}
}

// invalidate removes a live cached batch after source config or enable-state
// changes. Older candidates already handed to acquire remain visible only to
// their in-flight request and are rejected at commit by StaticBatchCurrent.
func (c *staticCache) invalidate(sourceID string) {
	release, _ := c.fence.lock(context.Background(), true)
	defer release()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeEntryLocked(sourceID)
	if f := c.inFlight[sourceID]; f != nil {
		f.invalidated = true
	}
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
	now time.Time) (StaticRefreshResult, *domain.Error) {
	if f == nil {
		return cloneStaticRefreshResult(res), derr
	}
	release, _ := c.fence.lock(context.Background(), true)
	defer release()
	c.mu.Lock()
	if f.invalidated {
		entry = nil
		res = StaticRefreshResult{}
		derr = domain.E("SOURCE_CONFIG_CHANGED", 409, "source %q changed during refresh", sourceID)
	}
	if entry != nil && res.NotModified {
		current, ok := c.entries[sourceID]
		if !ok || current.batchID != entry.batchID || current.configVersion != entry.configVersion {
			entry = nil
			res = StaticRefreshResult{}
			derr = domain.E("SOURCE_CONFIG_CHANGED", 409, "conditional refresh belongs to an obsolete source batch")
		} else {
			// Carry writes/removals made AFTER the 304 was prepared. Copying a
			// map earlier in downloadStatic is not enough to fence a late FAIL.
			entry.proofs = make(map[string]staticProof, len(current.proofs))
			for key, proof := range current.proofs {
				entry.proofs[key] = proof
			}
			entry.cursor = current.cursor
			entry.candidates = append([]domain.Candidate(nil), current.candidates...)
			expires := c.expiresAt(entry)
			for i := range entry.candidates {
				entry.candidates[i].CacheExpiresAt = &expires
			}
			res.Retained = len(entry.candidates)
		}
	}
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
	c.evictLocked()
	if entry != nil && c.entries[sourceID] != entry {
		res.Retained = 0
		derr = domain.E("STATIC_CACHE_CAPACITY", 503, "source batch cannot fit the configured cache limits")
	}
	f.res = cloneStaticRefreshResult(res)
	f.derr = derr
	c.mu.Unlock()
	close(f.done)
	return cloneStaticRefreshResult(res), derr
}

// removeEntryLocked keeps every accounting field in sync. Callers hold c.mu.
func (c *staticCache) removeEntryLocked(sourceID string) {
	old, ok := c.entries[sourceID]
	if ok {
		c.bytes -= staticEntryBytes(old)
		c.candidates -= len(old.candidates)
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
	c.candidates += len(entry.candidates)
	c.order = append(c.order, sourceID)
}

func (c *staticCache) evictLocked() {
	for len(c.entries) > c.maxEntries || c.bytes > c.maxBytes || c.candidates > c.maxCandidates {
		if len(c.order) == 0 {
			break
		}
		c.removeEntryLocked(c.order[0])
	}
}

func staticEntryBytes(e *staticEntry) int64 {
	if e == nil {
		return 0
	}
	n := int64(unsafe.Sizeof(*e)) + int64(cap(e.candidates))*int64(unsafe.Sizeof(domain.Candidate{})) + int64(cap(e.rejected))*int64(unsafe.Sizeof(FetchRowError{}))
	// Charge a fixed allowance for the outer source index, order keys and
	// map buckets, including an empty batch. This remains an estimate rather
	// than a claim about Go allocator/RSS overhead or temporary parse space.
	n += 256
	n += int64(len(e.batchID) + len(e.etag) + len(e.lastModified))
	if e.sourceUpdatedAt != nil {
		n += int64(unsafe.Sizeof(time.Time{}))
	}
	for _, cand := range e.candidates {
		n += int64(len(cand.Resource.Conn.Host) + len(cand.Resource.Conn.Username) +
			len(cand.Resource.Conn.Password) + len(cand.Fingerprint) +
			len(cand.Incarnation) + len(cand.Resource.ID) + len(cand.Resource.CredentialVersion) + len(cand.Resource.Tenant) +
			len(cand.Offer.ID) + len(cand.Offer.SourceID) + len(cand.Offer.ResourceID) + len(cand.Offer.SourceRecordID) + len(cand.Offer.SourceGeneration) + len(cand.Offer.ExpiryBasis))
		if cand.CacheExpiresAt != nil {
			n += int64(unsafe.Sizeof(time.Time{}))
		}
		if cand.Offer.ProviderExpiresAt != nil {
			n += int64(unsafe.Sizeof(time.Time{}))
		}
		if cand.Offer.LocalRetentionDeadline != nil {
			n += int64(unsafe.Sizeof(time.Time{}))
		}
		if cand.Offer.Claims != nil {
			n += 64
		}
		for k, v := range cand.Offer.Claims {
			n += int64(len(k)+len(v)) + 64
		}
	}
	for _, rej := range e.rejected {
		n += int64(len(rej.Input) + len(rej.Error))
	}
	for k, p := range e.proofs {
		n += int64(len(k)) + int64(unsafe.Sizeof(p)) + 128
		ev := p.Evidence
		n += int64(len(ev.AttemptID) + len(ev.RequestID) + len(ev.Profile) + len(ev.Mode) + len(ev.Target) + len(ev.SecurityHash) + len(ev.Outcome) + len(ev.ObservedExitIP))
		if ev.ValidUntil != nil {
			n += int64(unsafe.Sizeof(time.Time{}))
		}
		if p.Observation != nil {
			n += int64(unsafe.Sizeof(*p.Observation)) + int64(len(p.Observation.ExitIP)+len(p.Observation.CountryCode)+len(p.Observation.Country)+len(p.Observation.Region)+len(p.Observation.City)+len(p.Observation.ISP))
		}
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
	s.static.mu.Lock()
	defer s.static.mu.Unlock()
	entry, ok := s.static.entries[sourceID]
	if !ok || !s.static.entryFresh(entry, s.Now()) {
		return nil, "", false
	}
	out := make([]domain.Candidate, len(entry.candidates))
	for i, cand := range entry.candidates {
		out[i] = cloneStaticCandidate(cand)
	}
	return out, entry.batchID, true
}

func cloneStaticCandidate(c domain.Candidate) domain.Candidate {
	if c.Offer.Claims != nil {
		claims := make(map[string]string, len(c.Offer.Claims))
		for k, v := range c.Offer.Claims {
			claims[k] = v
		}
		c.Offer.Claims = claims
	}
	if c.Observation != nil {
		obs := *c.Observation
		c.Observation = &obs
	}
	if c.CacheExpiresAt != nil {
		at := *c.CacheExpiresAt
		c.CacheExpiresAt = &at
	}
	if c.Offer.ProviderExpiresAt != nil {
		at := *c.Offer.ProviderExpiresAt
		c.Offer.ProviderExpiresAt = &at
	}
	if c.Offer.LocalRetentionDeadline != nil {
		at := *c.Offer.LocalRetentionDeadline
		c.Offer.LocalRetentionDeadline = &at
	}
	return c
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
	s.static.mu.Lock()
	defer s.static.mu.Unlock()
	e, ok := s.static.entries[sourceID]
	return ok && s.static.entryFresh(e, s.Now()) && e.batchID == batchID
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
	if !isListAdapter(src.AdapterType) {
		return nil, "", domain.InvalidRequest(
			"source %q is a %q channel; only static sources can supply batches",
			sourceID, src.AdapterType)
	}
	if !src.Enabled {
		return nil, "", domain.InvalidRequest("source %q is disabled", sourceID)
	}
	if !s.static.hasFresh(sourceID, s.Now(), src.ConfigVersion) {
		if _, derr := s.RefreshStaticSource(ctx, sourceID); derr != nil {
			return nil, "", derr
		}
	}
	s.static.mu.Lock()
	defer s.static.mu.Unlock()
	entry, ok := s.static.entries[sourceID]
	if !ok || entry.configVersion != src.ConfigVersion || !s.static.entryFresh(entry, s.Now()) {
		return nil, "", domain.E("SOURCE_STALE", 503, "static batch changed during selection")
	}
	n := len(entry.candidates)
	if wanted > 0 && wanted < n {
		n = wanted
	}
	out := make([]domain.Candidate, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, cloneStaticCandidate(entry.candidates[(entry.cursor+i)%len(entry.candidates)]))
	}
	if len(entry.candidates) > 0 {
		entry.cursor = (entry.cursor + n) % len(entry.candidates)
	}
	return out, entry.batchID, nil
}

// RefreshStaticSource refreshes the bounded static cache for one site_fetch
// or static_url channel. It never writes inventory rows: cached candidates
// are request-local until acquire validates and commits them.
func (s *IngestService) RefreshStaticSource(ctx context.Context, sourceID string) (StaticRefreshResult, *domain.Error) {
	if err := s.PurgeExpiredStaticCache(ctx); err != nil {
		return StaticRefreshResult{}, domain.AsError(err)
	}
	src, err := s.store.GetSource(ctx, sourceID)
	if err != nil {
		return StaticRefreshResult{}, domain.AsError(err)
	}
	if !isListAdapter(src.AdapterType) {
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
	if src.AdapterType == AdapterAccount {
		if _, derr := s.readAccountToken(ctx, sourceID); derr != nil {
			return StaticRefreshResult{}, derr
		}
	}
	// A retained snapshot (including an exhausted/blacklisted one) is not a
	// reason to hammer the same upstream. Only expiry/config change fetches.
	if res, ok := s.static.freshResult(sourceID, src.ConfigVersion, s.Now()); ok {
		return res, nil
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
		if res, ok := s.static.freshResult(sourceID, src.ConfigVersion, s.Now()); ok {
			return s.static.finishRefreshResult(sourceID, f, nil, res, nil, false, s.Now())
		}
		derr := domain.E("SOURCE_RATE_LIMITED", 429,
			"source %q was refreshed recently; retry later", sourceID)
		s.static.finishRefreshResult(sourceID, f, nil, StaticRefreshResult{}, derr,
			false, s.Now())
		return StaticRefreshResult{}, derr
	}
	if ctx.Err() != nil {
		derr := domain.E("REQUEST_CANCELED", 499, "source refresh was cancelled")
		s.static.finishRefreshResult(sourceID, f, nil, StaticRefreshResult{}, derr, false, s.Now())
		return StaticRefreshResult{}, derr
	}
	select {
	case s.fetchSlots <- struct{}{}:
		defer func() { <-s.fetchSlots }()
	default:
		derr := domain.E("SOURCE_FETCH_CAPACITY", 503, "source download capacity is full")
		s.static.finishRefreshResult(sourceID, f, nil, StaticRefreshResult{}, derr, false, s.Now())
		return StaticRefreshResult{}, derr
	}
	started := s.Now()
	minInterval := s.staticMinInterval(src)
	if err := s.store.ClaimSourceFetch(ctx, sourceID, src.ConfigVersion, started, minInterval); err != nil {
		derr := domain.AsError(err)
		if errors.Is(err, sqlite.ErrStale) {
			derr = domain.E("SOURCE_CONFIG_CHANGED", 409, "source changed before fetching")
		}
		if derr.Code == "SOURCE_RATE_LIMITED" {
			if res, ok := s.static.freshResult(sourceID, src.ConfigVersion, s.Now()); ok {
				return s.static.finishRefreshResult(sourceID, f, nil, res, nil, false, s.Now())
			}
		}
		s.static.finishRefreshResult(sourceID, f, nil, StaticRefreshResult{}, derr, false, s.Now())
		return StaticRefreshResult{}, derr
	}
	realStarted := time.Now()
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
	if derr == nil && entry != nil {
		// A new entry is installed newest, so ordinary global pressure can
		// evict older entries. But an entry that cannot fit even by itself
		// must fail before persisting a fictitious retained/success count.
		s.static.mu.Lock()
		fits := s.static.maxEntries > 0 && len(entry.candidates) <= s.static.maxCandidates && staticEntryBytes(entry) <= s.static.maxBytes
		s.static.mu.Unlock()
		if !fits {
			derr = domain.E("STATIC_CACHE_CAPACITY", 503, "source batch cannot fit the configured cache limits")
			entry = nil
		}
	}
	completed := s.Now()
	next := s.nextSourceFetchAt(ctx, sourceID, completed, minInterval, derr)
	outcome := sqlite.SourceFetchOutcome{SourceID: sourceID, ConfigVersion: src.ConfigVersion, StartedAt: started, CompletedAt: completed, NextFetchAt: next, DurationMS: time.Since(realStarted).Milliseconds()}
	if cfg.Refresh != nil && cfg.Refresh.Mode == "interval" {
		outcome.NextRefreshAt = completed.Add(time.Duration(cfg.Refresh.IntervalMS) * time.Millisecond)
	}
	if derr != nil {
		outcome.ErrorCode = derr.Code
	} else {
		outcome.Fetched = res.Fetched
		outcome.Retained = res.Retained
		outcome.Rejected = res.RejectedCount
	}
	writeCtx, writeCancel := context.WithTimeout(context.Background(), time.Second)
	stateErr := s.store.FinishSourceFetch(writeCtx, outcome)
	writeCancel()
	if stateErr != nil {
		if errors.Is(stateErr, sqlite.ErrStale) {
			derr = domain.E("SOURCE_CONFIG_CHANGED", 409, "source changed before refresh completion")
		} else {
			derr = domain.StoreUnavailable(stateErr)
		}
		entry = nil
	}
	if derr != nil {
		s.static.finishRefreshResult(sourceID, f, nil, StaticRefreshResult{}, derr,
			true, s.Now())
		return StaticRefreshResult{}, derr
	}
	installed, installErr := s.static.finishRefreshResult(sourceID, f, entry, *res, nil, true, s.Now())
	if installErr != nil {
		// A late config invalidation or 304 eviction can still defeat a
		// prepared installation. Correct only this owned run's summary; a
		// newer version/run cannot be overwritten by FinishSourceFetch.
		outcome.ErrorCode = installErr.Code
		outcome.Retained = installed.Retained
		outcome.NextFetchAt = s.nextSourceFetchAt(ctx, sourceID, s.Now(), minInterval, installErr)
		writeCtx, stop := context.WithTimeout(context.Background(), time.Second)
		_ = s.store.FinishSourceFetch(writeCtx, outcome)
		stop()
	}
	return installed, installErr
}

// ScopedStaticBatch serves a fresh static batch narrowed to the request's
// transport and country filters. It re-reads the source each time, so a
// disabled or reconfigured channel cannot keep using a snapshot that loaded
// before the edit. The returned candidates stay request-local.
func (s *IngestService) ScopedStaticBatch(ctx context.Context, sourceID string,
	wanted int, scope RefillScope) ([]domain.Candidate, string, *domain.Error) {
	before, err := s.store.GetSource(ctx, sourceID)
	if err != nil {
		return nil, "", domain.AsError(err)
	}
	var accessCfg SourceConfig
	if json.Unmarshal([]byte(before.ConfigJSON), &accessCfg) != nil {
		return nil, "", domain.E("SOURCE_CONFIG_ERROR", 409, "source config could not be decoded")
	}
	if !sourceProjectAllowed(accessCfg, scope.ProjectID) {
		return nil, "", domain.E("SOURCE_NOT_ALLOWED", 403, "source is not allowed for this project")
	}
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
	if !sourceProjectAllowed(cfg, scope.ProjectID) {
		return nil, "", domain.E("SOURCE_NOT_ALLOWED", 403, "source is not allowed for this project")
	}
	// Cached PASS may be anywhere in the downloaded list. Select it before
	// truncating and retain a small fallback batch for dead first rows.
	limit := len(cands)
	if wanted > 0 {
		limit = wanted
		if limit < minStaticRefillCandidates {
			limit = minStaticRefillCandidates
		}
		if limit > len(cands) {
			limit = len(cands)
		}
	}
	eligible := make([]domain.Candidate, 0, len(cands))
	blocked, berr := s.store.SourceBlacklist(ctx, sourceID, s.Now())
	if berr != nil {
		return nil, "", domain.AsError(berr)
	}
	for _, cand := range cands {
		if blocked[cand.Resource.ID] {
			continue
		}
		if cand.SourceConfigVersion != src.ConfigVersion ||
			cand.Incarnation != batchID {
			return nil, "", domain.E("SOURCE_CONFIG_CHANGED", 409,
				"source %q changed while its static batch was loading", sourceID)
		}
		if !scopeSupportsCandidate(scope, cand) {
			continue
		}
		eligible = append(eligible, cand)
	}
	s.static.mu.Lock()
	defer s.static.mu.Unlock()
	entry := s.static.entries[sourceID]
	now := s.Now()
	rank := func(cand domain.Candidate) int {
		if scope.EvidenceScopeHash == "" || entry == nil ||
			entry.batchID != batchID || !s.static.entryFresh(entry, now) {
			return 1
		}
		p, ok := entry.proofs[staticProofKey(s.fingerprint(cand), scope.EvidenceScopeHash)]
		if !ok || p.Evidence.CompletedAt.After(now) {
			return 1
		}
		if p.Evidence.Outcome == "PASS" {
			return 0
		}
		return 2
	}
	out := make([]domain.Candidate, 0, limit)
	for priority := 0; priority < 3 && len(out) < limit; priority++ {
		for _, cand := range eligible {
			if rank(cand) == priority {
				out = append(out, cand)
				if len(out) == limit {
					break
				}
			}
		}
	}
	if entry != nil && entry.batchID == batchID && len(entry.candidates) > 0 {
		entry.cursor = (entry.cursor + len(out)) % len(entry.candidates)
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
	if src.AdapterType == AdapterProvider {
		var cfg SourceConfig
		if json.Unmarshal([]byte(src.ConfigJSON), &cfg) == nil && cfg.Provider != nil {
			return max(defaultSiteFetchInterval, cfg.Provider.minFetchInterval())
		}
		return defaultSiteFetchInterval
	}
	if src.AdapterType == AdapterSite {
		return defaultSiteFetchInterval
	}
	if src.AdapterType == AdapterAccount {
		return 30 * time.Second
	}
	return defaultMinFetchIntervalMS * time.Millisecond
}

func (s *IngestService) downloadStatic(ctx context.Context, src domain.Source,
	cfg SourceConfig, sourceID string) (*StaticRefreshResult, *staticEntry, *domain.Error) {
	etag, lastModified := s.static.validators(sourceID)
	var (
		entries    []sites.Entry
		rejected   []FetchRowError
		pagesDone  int
		pagesTotal int
		notMod     bool
		ferr       *domain.Error
	)
	meta := listFetchMetadata{Complete: true}
	switch src.AdapterType {
	case AdapterProvider:
		if cfg.Provider == nil {
			return nil, nil, domain.InvalidRequest("source %q has no provider configuration", sourceID)
		}
		var batch scdn.FetchResult
		var err error
		batch, err = scdn.Fetch(ctx, s.ScdnHTTPClient, s.ScdnEndpoint, cfg.Provider.scdnConfig(), s.allowPrivate)
		if err != nil {
			ferr = mapScdnError(err)
		} else {
			for _, e := range batch.Entries {
				entries = append(entries, sites.Entry{Conn: domain.Connection{Transport: e.Transport, Host: e.Host, Port: e.Port}, VendorProtocol: cfg.Provider.Protocol})
			}
			rejected = toFetchRowErrors(batch.Errors)
			meta.Fetched, meta.RejectedCount = batch.Fetched, batch.RejectedCount
		}
	case AdapterSite:
		ferr = s.fetchSiteEntries(ctx, cfg, sourceID, &entries, &rejected, &pagesDone, &pagesTotal, &meta)
	case AdapterStatic:
		ferr = s.fetchStaticURLEntries(ctx, cfg, sourceID, &entries, &rejected,
			&etag, &lastModified, &notMod, &meta)
	case AdapterAccount:
		var result webshare.Result
		result, ferr = s.fetchAccountList(ctx, cfg, sourceID)
		if ferr == nil {
			for _, e := range result.Entries {
				entries = append(entries, sites.Entry{Conn: e.Conn, RecordID: e.RecordID, Claims: e.Claims, VendorProtocol: string(e.Conn.Transport)})
			}
			pagesDone, pagesTotal = result.PagesDone, result.PagesTotal
			meta.Fetched, meta.RejectedCount, meta.Complete = result.Fetched, result.Rejected, result.Complete
		}
	default:
		return nil, nil, domain.InvalidRequest(
			"source %q is a %q channel; only site_fetch and static_url support static caching",
			sourceID, src.AdapterType)
	}
	if ferr != nil {
		return nil, nil, ferr
	}
	// A list TTL starts after the completed download/parse, not before a
	// slow upstream request. It never acts as the supplier's proxy lifetime.
	now := s.Now()
	if notMod {
		return s.refreshedEntry(sourceID, src, now, etag, lastModified)
	}
	return s.buildStaticEntry(src, cfg, sourceID, now, entries, rejected,
		pagesDone, pagesTotal, etag, lastModified, meta)
}

func (s *IngestService) fetchSiteEntries(ctx context.Context, cfg SourceConfig,
	sourceID string, entries *[]sites.Entry, rejected *[]FetchRowError,
	pagesDone, pagesTotal *int, meta *listFetchMetadata) *domain.Error {
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
	out, err := sites.FetchSite(ctx, s.SiteHTTPClient, site, cfg.Site.MaxPages, s.allowPrivate)
	if err != nil {
		return mapSitesError(err)
	}
	*entries = out.Entries
	meta.Fetched, meta.Complete, meta.Sampled, meta.SourceUpdatedAt = out.Fetched, out.Complete, out.Truncated, out.SourceUpdatedAt
	*pagesDone, *pagesTotal = out.PagesDone, out.PagesTotal
	for _, r := range out.Rejected {
		*rejected = append(*rejected, FetchRowError{Line: r.Line, Input: r.Input, Error: r.Error})
	}
	meta.RejectedCount = out.RejectedCount
	return nil
}

func (s *IngestService) fetchStaticURLEntries(ctx context.Context, cfg SourceConfig,
	sourceID string, entries *[]sites.Entry, rejected *[]FetchRowError,
	etag, lastModified *string, notModified *bool, meta *listFetchMetadata) *domain.Error {
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
	// Sparse 304 headers retain existing validators. They renew list checks,
	// not proof timestamps or any supplier-declared proxy expiry.
	if !result.NotModified || result.ETag != "" {
		*etag = result.ETag
	}
	if !result.NotModified || result.LastModified != "" {
		*lastModified = result.LastModified
	}
	if result.NotModified {
		*notModified = true
		return nil
	}
	// Generic lines_v1 retains explicit per-row transports; only bare rows
	// inherit the configured default. The bounded site parser enforces URI
	// shape, credential/line limits and rejects HTML/error responses rather
	// than stripping an endpoint out of arbitrary vendor text.
	parsed, parseErr := sites.ParseBody(sites.ParseOptions{
		DefaultTransport: cfg.Static.DefaultTransport,
		AllowPrivate:     s.allowPrivate,
	}, sites.ParserLines, result.Body)
	if parseErr != nil {
		return mapSitesError(parseErr)
	}
	*entries = parsed.Entries
	for _, r := range parsed.Rejected {
		*rejected = append(*rejected, FetchRowError{Line: r.Line, Error: r.Error})
	}
	meta.Fetched, meta.RejectedCount = parsed.Fetched, parsed.RejectedCount
	meta.Complete, meta.Sampled = parsed.Complete, parsed.Truncated
	return nil
}

// refreshedEntry handles a 304: retain the batch and original lifetime,
// updating validators only. A conditional check cannot extend retention.
func (s *IngestService) refreshedEntry(sourceID string, _ domain.Source,
	now time.Time, etag, lastModified string) (*StaticRefreshResult, *staticEntry, *domain.Error) {
	release, err := s.static.fence.lock(context.Background(), false)
	if err != nil {
		return nil, nil, domain.AsError(err)
	}
	defer release()
	s.static.mu.Lock()
	defer s.static.mu.Unlock()
	previous, ok := s.static.entries[sourceID]
	if !ok {
		return nil, nil, domain.E("SOURCE_STALE_NOT_MODIFIED", 502,
			"source %q returned 304 but no cached batch exists", sourceID)
	}
	fresh := &staticEntry{
		batchID: previous.batchID, cachedAt: previous.cachedAt, etag: etag,
		lastModified: lastModified, candidates: previous.candidates,
		rejected: previous.rejected, pagesDone: previous.pagesDone,
		pagesTotal: previous.pagesTotal,
		ttl:        previous.ttl, cursor: previous.cursor, proofs: previous.proofs, sourceUpdatedAt: previous.sourceUpdatedAt, truncated: previous.truncated,
		configVersion: previous.configVersion,
		rejectedCount: previous.rejectedCount, fetched: previous.fetched, unique: previous.unique, complete: previous.complete, sampled: previous.sampled,
	}
	// The old entry remains mutable until installation. Clone the proof map
	// under its mutex so intervening writes cannot corrupt accounting or erase
	// a newer negative proof when a 304 is installed.
	fresh.proofs = make(map[string]staticProof, len(previous.proofs))
	for key, proof := range previous.proofs {
		fresh.proofs[key] = proof
	}
	expires := fresh.cachedAt.Add(fresh.ttl)
	if fresh.ttl <= 0 {
		expires = fresh.cachedAt.Add(s.static.ttl)
	}
	// A 304 refreshes list freshness only. It does not extend any proof.
	fresh.candidates = append([]domain.Candidate(nil), previous.candidates...)
	for i := range fresh.candidates {
		fresh.candidates[i].CacheExpiresAt = &expires
	}
	return &StaticRefreshResult{SourceID: sourceID, BatchID: fresh.batchID,
		Fetched: fresh.fetched, Unique: fresh.unique, Retained: len(fresh.candidates), NotModified: true, Rejected: append([]FetchRowError(nil), fresh.rejected...), RejectedCount: fresh.rejectedCount,
		PagesDone: fresh.pagesDone, PagesTotal: fresh.pagesTotal, ExpiresAt: &expires, Truncated: fresh.truncated, SourceUpdatedAt: fresh.sourceUpdatedAt, Complete: fresh.complete, Sampled: fresh.sampled}, fresh, nil
}

// buildStaticEntry normalizes fetched connections into request-local
// candidates. No offer IDs, no fabricated exit TTLs: acquire validates and
// commits before anything is delivered.
func (s *IngestService) buildStaticEntry(src domain.Source, cfg SourceConfig,
	sourceID string, now time.Time, entries []sites.Entry, rejected []FetchRowError,
	pagesDone, pagesTotal int, etag, lastModified string, metadata ...listFetchMetadata) (*StaticRefreshResult, *staticEntry, *domain.Error) {
	meta := listFetchMetadata{Fetched: len(entries), RejectedCount: len(rejected), Complete: true}
	if len(metadata) > 0 {
		meta = metadata[0]
		if meta.Fetched < len(entries) {
			meta.Fetched = len(entries)
		}
	}
	// Deduplicate before sampling, so repeated front-page rows cannot starve
	// a large list of its configured unique-candidate capacity.
	unique := make([]sites.Entry, 0, len(entries))
	seenEntries := map[string]bool{}
	for _, e := range entries {
		e.Conn.Host = domain.CanonicalHost(e.Conn.Host)
		fp := domain.ConnectionFingerprint(s.secret, e.Conn.Transport, e.Conn.Host, e.Conn.Port, e.Conn.Username, e.Conn.Password)
		if !seenEntries[fp] {
			seenEntries[fp] = true
			unique = append(unique, e)
		}
	}
	entries = unique
	uniqueCount := len(entries)
	limit := sourceCandidateLimit(cfg)
	truncated := 0
	if len(entries) > limit {
		truncated = len(entries) - limit
		// Keep a small quality-biased prefix and a bounded random sample of
		// the remainder. Supplier sort order is a claim, not proof.
		prefix := limit / 4
		sample := append([]sites.Entry(nil), entries[:limit]...)
		for i := limit; i < len(entries); i++ {
			j := prefix + rand.IntN(i-prefix+1)
			if j < limit {
				sample[j] = entries[i]
			}
		}
		entries = sample
	}
	meta.Complete = meta.Complete && truncated == 0 && !meta.Sampled
	batchID := domain.NewID("static_")
	ttl := s.sourceCacheTTL(cfg)
	expires := now.Add(ttl)
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
		for key, value := range e.Claims {
			if key != "batch_id" {
				claims[key] = value
			}
		}
		if e.RecordID != "" {
			norm.RecordID = e.RecordID
		}
		norm.ProviderExpiresAt = e.ProviderExpiresAt
		norm.Claims = claims
		cand := candidateFromNormalized(norm)
		cand.Ephemeral = true
		cand.SourceConfigVersion = src.ConfigVersion
		cand.Incarnation = batchID
		cand.Fingerprint = s.fingerprint(cand)
		cand.CacheExpiresAt = &expires
		candidates = append(candidates, cand)
	}
	entry := &staticEntry{
		batchID: batchID, cachedAt: now, etag: etag, lastModified: lastModified,
		candidates: candidates, rejected: rejected,
		pagesDone: pagesDone, pagesTotal: pagesTotal,
		ttl: ttl, truncated: truncated,
		configVersion: src.ConfigVersion,
		rejectedCount: meta.RejectedCount, fetched: meta.Fetched, unique: uniqueCount, complete: meta.Complete, sampled: meta.Sampled || truncated > 0, sourceUpdatedAt: meta.SourceUpdatedAt,
	}
	return &StaticRefreshResult{SourceID: sourceID, BatchID: batchID,
		Fetched: meta.Fetched, Unique: uniqueCount, Retained: len(candidates), Rejected: rejected, RejectedCount: meta.RejectedCount,
		PagesDone: pagesDone, PagesTotal: pagesTotal, ExpiresAt: &expires, Truncated: truncated, SourceUpdatedAt: meta.SourceUpdatedAt, Complete: meta.Complete, Sampled: meta.Sampled || truncated > 0}, entry, nil
}

type StaticCacheView struct {
	SourceID        string     `json:"source_id"`
	BatchID         string     `json:"batch_id,omitempty"`
	ConfigVersion   int64      `json:"config_version"`
	FetchedAt       *time.Time `json:"fetched_at"`
	ExpiresAt       *time.Time `json:"expires_at"`
	Fresh           bool       `json:"fresh"`
	Candidates      int        `json:"candidates"`
	Bytes           int64      `json:"bytes"`
	Proofs          int        `json:"proofs"`
	Verified        int        `json:"verified"`
	Blacklisted     int        `json:"blacklisted"`
	Rejected        int        `json:"rejected"`
	Truncated       int        `json:"truncated"`
	PagesDone       int        `json:"pages_done"`
	PagesTotal      int        `json:"pages_total"`
	SourceUpdatedAt *time.Time `json:"source_updated_at,omitempty"`
	Fetched         int        `json:"fetched"`
	Unique          int        `json:"unique"`
	Complete        bool       `json:"complete"`
	Sampled         bool       `json:"sampled"`
}

// CacheView publishes metadata only; candidate credentials remain private.
func (s *IngestService) CacheView(sourceID string) StaticCacheView {
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	blocked, blockErr := s.store.SourceBlacklist(ctx, sourceID, s.Now())
	cancel()
	s.static.mu.Lock()
	defer s.static.mu.Unlock()
	out := StaticCacheView{SourceID: sourceID}
	if e, ok := s.static.entries[sourceID]; ok {
		expires := s.static.expiresAt(e)
		at := e.cachedAt
		out.BatchID = e.batchID
		out.ConfigVersion = e.configVersion
		out.FetchedAt = &at
		out.ExpiresAt = &expires
		out.Fresh = s.static.entryFresh(e, s.Now())
		if blockErr != nil {
			out.Fresh = false
		}
		out.Candidates = len(e.candidates)
		out.Bytes = staticEntryBytes(e)
		out.Proofs = len(e.proofs)
		if out.Fresh {
			for _, c := range e.candidates {
				if blocked[c.Resource.ID] {
					out.Blacklisted++
				}
			}
			blockedFP := map[string]bool{}
			for _, c := range e.candidates {
				if blocked[c.Resource.ID] {
					blockedFP[s.fingerprint(c)] = true
				}
			}
			verified := map[string]bool{}
			for key, p := range e.proofs {
				if p.Evidence.Outcome == "PASS" && !p.Evidence.CompletedAt.IsZero() && !p.Evidence.CompletedAt.After(s.Now()) {
					fp, _, _ := strings.Cut(key, "|")
					if !blockedFP[fp] {
						verified[fp] = true
					}
				}
			}
			out.Verified = len(verified)
		} else {
			out.Candidates, out.Proofs = 0, 0
		}
		out.Rejected = e.rejectedCount
		out.Truncated = e.truncated
		out.PagesDone = e.pagesDone
		out.PagesTotal = e.pagesTotal
		out.SourceUpdatedAt = cloneTime(e.sourceUpdatedAt)
		out.Fetched, out.Unique, out.Complete, out.Sampled = e.fetched, e.unique, e.complete, e.sampled
	}
	return out
}

// CacheViewForSource prevents an already-committed disable/config edit from
// being reported as a fresh batch in the small invalidation handoff window.
// Final delivery separately guards the live source inside its transaction.
func (s *IngestService) CacheViewForSource(src domain.Source) StaticCacheView {
	out := s.CacheView(src.ID)
	if !src.Enabled || out.ConfigVersion != src.ConfigVersion {
		out.Fresh = false
	}
	return out
}

func anonymityClaim(siteID string) string {
	if site, ok := sites.ByID(siteID); ok {
		return string(site.Anonymity)
	}
	return string(sites.AnonymityUnknown)
}

func mapStaticDownloadError(err error) *domain.Error {
	var ferr *sourcehttp.Error
	if errors.As(err, &ferr) {
		derr := domain.E(ferr.Code, ferr.Status, "static list download failed: %s", ferr.Detail)
		derr.RetryAfter = ferr.RetryAfter
		return derr
	}
	// A URL error from an injected client can contain query keys, userinfo
	// or a redirect Location. Never relay that underlying diagnostic.
	if errors.Is(err, context.DeadlineExceeded) {
		return domain.E("SOURCE_TIMEOUT", 504, "static list download timed out")
	}
	if errors.Is(err, context.Canceled) {
		return domain.E("REQUEST_CANCELED", 499, "static list download was cancelled")
	}
	return domain.E("SOURCE_UNAVAILABLE", 503, "static list download failed")
}
