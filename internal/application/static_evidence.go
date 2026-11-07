package application

import (
	"context"
	"strings"
	"sync"
	"time"

	"proxypool/internal/domain"
	"proxypool/internal/geoip"
)

// cacheGate is a context-aware read/write fence. Cache replacement and newer
// failures hold a writer; final SQLite delivery holds a reader. No network
// request or long-lived lease holds this fence.
type cacheGate struct {
	mu             sync.Mutex
	readers        int
	writer         bool
	waitingWriters int
	wait           chan struct{}
}

func newCacheGate() *cacheGate { return &cacheGate{wait: make(chan struct{})} }

func (g *cacheGate) lock(ctx context.Context, write bool) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if write {
		g.mu.Lock()
		g.waitingWriters++
		g.mu.Unlock()
	}
	for {
		g.mu.Lock()
		if ctx.Err() != nil {
			if write {
				g.waitingWriters--
				close(g.wait)
				g.wait = make(chan struct{})
			}
			g.mu.Unlock()
			return nil, ctx.Err()
		}
		if !g.writer && (write && g.readers == 0 || !write && g.waitingWriters == 0) {
			if write {
				g.waitingWriters--
				g.writer = true
			} else {
				g.readers++
			}
			g.mu.Unlock()
			var once sync.Once
			return func() {
				once.Do(func() {
					g.mu.Lock()
					if write {
						g.writer = false
					} else {
						g.readers--
					}
					close(g.wait)
					g.wait = make(chan struct{})
					g.mu.Unlock()
				})
			}, nil
		}
		wait := g.wait
		g.mu.Unlock()
		select {
		case <-ctx.Done():
			if write {
				g.mu.Lock()
				g.waitingWriters--
				close(g.wait)
				g.wait = make(chan struct{})
				g.mu.Unlock()
			}
			return nil, ctx.Err()
		case <-wait:
		}
	}
}

type staticProof struct {
	Evidence    domain.Evidence
	Observation *domain.Observation
}

func staticProofKey(fingerprint, scopeHash string) string { return fingerprint + "|" + scopeHash }

func isStaticCandidate(c domain.Candidate) bool {
	return c.Ephemeral && c.Offer.ExpiryBasis == "static_cache"
}

// StaticEvidenceProvider is shared by acquire and prewarm; inventory proof
// stays in SQLite and on-demand provider candidates never use this cache.
type StaticEvidenceProvider interface {
	StaticRecentEvidence(c domain.Candidate, scopeHash string, notBefore time.Time) (domain.Evidence, *domain.Observation, bool)
	RecordStaticEvidence(ctx context.Context, c domain.Candidate, scopeHash string, ev domain.Evidence, obs *domain.Observation) error
	RemoveStaticCandidate(ctx context.Context, c domain.Candidate) error
	LockStaticDelivery(ctx context.Context, candidates []domain.Candidate, proofs []domain.Evidence, scopeHash string) (func(), map[string]bool, error)
	// ConsumeStaticDelivery is called only for committed deliveries while
	// holding the exclusive delivery fence. No database or network I/O.
	ConsumeStaticDelivery(candidates []domain.Candidate)
}

func (s *IngestService) StaticRecentEvidence(cand domain.Candidate, scopeHash string, notBefore time.Time) (domain.Evidence, *domain.Observation, bool) {
	s.static.mu.Lock()
	defer s.static.mu.Unlock()
	e, ok := s.static.entries[cand.Offer.SourceID]
	if !ok || !s.static.entryFresh(e, s.Now()) || e.batchID != cand.Incarnation || !staticEntryHasCandidate(e, cand) {
		return domain.Evidence{}, nil, false
	}
	p, ok := e.proofs[staticProofKey(s.fingerprint(cand), scopeHash)]
	if !ok || p.Evidence.Outcome != "PASS" || p.Evidence.CompletedAt.After(s.Now()) {
		return domain.Evidence{}, nil, false
	}
	ev := cloneStaticProofEvidence(p.Evidence)
	ev.CacheHit = true
	var obs *domain.Observation
	if p.Observation != nil {
		copy := *p.Observation
		obs = &copy
	}
	return ev, obs, true
}

func (s *IngestService) fingerprint(c domain.Candidate) string {
	conn := c.Resource.Conn
	return domain.ConnectionFingerprint(s.secret, conn.Transport, conn.Host, conn.Port, conn.Username, conn.Password)
}

func staticEntryHasCandidate(e *staticEntry, cand domain.Candidate) bool {
	for _, c := range e.candidates {
		if c.SourceConfigVersion == cand.SourceConfigVersion && c.Resource.ID == cand.Resource.ID {
			return true
		}
	}
	return false
}

func (s *IngestService) RecordStaticEvidence(ctx context.Context, cand domain.Candidate, scopeHash string, ev domain.Evidence, obs *domain.Observation) error {
	if !isStaticCandidate(cand) || scopeHash == "" || ev.CompletedAt.IsZero() || ev.CompletedAt.After(s.Now()) || !ev.StartedAt.IsZero() && ev.StartedAt.After(ev.CompletedAt) || ev.Outcome != "PASS" && ev.Outcome != "FAIL" {
		return domain.InvalidRequest("incomplete static evidence")
	}
	release, err := s.static.fence.lock(ctx, true)
	if err != nil {
		return err
	}
	defer release()
	s.static.mu.Lock()
	defer s.static.mu.Unlock()
	e, ok := s.static.entries[cand.Offer.SourceID]
	if !ok || !s.static.entryFresh(e, s.Now()) || e.batchID != cand.Incarnation || !staticEntryHasCandidate(e, cand) {
		return domain.E("SOURCE_STALE", 409, "static evidence belongs to an obsolete batch")
	}
	key := staticProofKey(s.fingerprint(cand), scopeHash)
	prev, ok := e.proofs[key]
	if ok && (prev.Evidence.CompletedAt.After(ev.CompletedAt) || prev.Evidence.CompletedAt.Equal(ev.CompletedAt) && prev.Evidence.Outcome == "FAIL" && ev.Outcome == "PASS") {
		return domain.E("EVIDENCE_STALE", 409, "newer static evidence already exists")
	}
	before := staticEntryBytes(e)
	if e.proofs == nil {
		e.proofs = map[string]staticProof{}
	}
	// Bound target scopes as well as candidates. Proof eviction only costs a
	// new probe; it never changes a project CD window.
	if len(e.proofs) >= maxStaticProofsPerSource && !ok {
		oldestKey := ""
		var oldest time.Time
		for k, p := range e.proofs {
			if oldestKey == "" || p.Evidence.CompletedAt.Before(oldest) {
				oldestKey, oldest = k, p.Evidence.CompletedAt
			}
		}
		delete(e.proofs, oldestKey)
	}
	if obs != nil {
		copy := *obs
		obs = &copy
	}
	e.proofs[key] = staticProof{Evidence: cloneStaticProofEvidence(ev), Observation: obs}
	s.static.bytes += staticEntryBytes(e) - before
	s.static.evictLocked()
	return nil
}

func cloneStaticProofEvidence(ev domain.Evidence) domain.Evidence {
	if ev.ValidUntil != nil {
		deadline := *ev.ValidUntil
		ev.ValidUntil = &deadline
	}
	return ev
}

func (s *IngestService) RemoveStaticCandidate(ctx context.Context, cand domain.Candidate) error {
	release, err := s.static.fence.lock(ctx, true)
	if err != nil {
		return err
	}
	defer release()
	s.static.mu.Lock()
	e, ok := s.static.entries[cand.Offer.SourceID]
	if !ok || e.batchID != cand.Incarnation || e.configVersion != cand.SourceConfigVersion || !staticEntryHasCandidate(e, cand) {
		s.static.mu.Unlock()
		return nil
	}
	s.static.mu.Unlock()
	// Keep the same writer fence across this short database update and the
	// removal. A final sticky transaction either precedes AUTH removal or
	// observes an invalidated binding; deletion cannot erase the only FAIL
	// signal and leave an old private PASS reusable.
	_, bindingErr := s.store.ExpireListBindingsOwned(ctx, cand.Offer.SourceID,
		cand.SourceConfigVersion, s.fingerprint(cand), s.Now())
	s.static.mu.Lock()
	defer s.static.mu.Unlock()
	before := staticEntryBytes(e)
	next := make([]domain.Candidate, 0, len(e.candidates))
	for _, c := range e.candidates {
		if c.Resource.ID != cand.Resource.ID {
			next = append(next, c)
		}
	}
	s.static.candidates -= len(e.candidates) - len(next)
	e.candidates = next
	fp := s.fingerprint(cand) + "|"
	for key := range e.proofs {
		if len(key) >= len(fp) && key[:len(fp)] == fp {
			delete(e.proofs, key)
		}
	}
	s.static.bytes += staticEntryBytes(e) - before
	return bindingErr
}

// LockStaticDelivery freezes the candidate generations and latest proof
// until the short final transaction returns. Its deadline field is checked
// again against the transaction's actual time in the SQLite layer.
func (s *IngestService) LockStaticDelivery(ctx context.Context, candidates []domain.Candidate, proofs []domain.Evidence, scopeHash string) (func(), map[string]bool, error) {
	// Exclusive across eligibility, SQLite commit and removal. A second
	// client must observe the consumed candidate even with shared leases.
	release, err := s.static.fence.lock(ctx, true)
	if err != nil {
		return nil, nil, err
	}
	s.static.mu.Lock()
	defer s.static.mu.Unlock()
	eligible := map[string]bool{}
	for i, c := range candidates {
		if !isStaticCandidate(c) {
			continue
		}
		fp := s.fingerprint(c)
		e, ok := s.static.entries[c.Offer.SourceID]
		if !ok || !s.static.entryFresh(e, s.Now()) || e.batchID != c.Incarnation || !staticEntryHasCandidate(e, c) {
			continue
		}
		latest, exists := e.proofs[staticProofKey(fp, scopeHash)]
		if !exists || i >= len(proofs) || latest.Evidence.Outcome != "PASS" || latest.Evidence.CompletedAt.Before(proofs[i].CompletedAt) || latest.Evidence.ObservedExitIP != proofs[i].ObservedExitIP {
			continue
		}
		eligible[fp] = true
	}
	return release, eligible, nil
}

// ConsumeStaticDelivery removes a connection from every network list in
// which it currently appears. Uploaded inventory and existing client leases
// are deliberately untouched. The caller already owns the writer fence.
func (s *IngestService) ConsumeStaticDelivery(candidates []domain.Candidate) {
	s.static.mu.Lock()
	defer s.static.mu.Unlock()
	for _, c := range candidates {
		if isStaticCandidate(c) {
			if e := s.static.entries[c.Offer.SourceID]; e != nil {
				before := staticEntryBytes(e)
				for key := range e.proofs {
					fp, _, _ := strings.Cut(key, "|")
					if fp == s.fingerprint(c) {
						delete(e.proofs, key)
					}
				}
				s.static.bytes += staticEntryBytes(e) - before
			}
		}
	}
}

// MarkStaticUnavailable retains the downloaded row but prevents another
// probe/delivery from this source until its persistent blacklist expires.
func (s *IngestService) MarkStaticUnavailable(ctx context.Context, c domain.Candidate, projectID string) error {
	release, err := s.static.fence.lock(ctx, true)
	if err != nil {
		return err
	}
	defer release()
	if err := s.store.MarkSourceProxyUnavailable(ctx, c, s.fingerprint(c), projectID, "validation_failed", s.Now()); err != nil {
		return err
	}
	_, err = s.store.ExpireListBindingsOwned(ctx, c.Offer.SourceID, c.SourceConfigVersion, s.fingerprint(c), s.Now())
	s.ConsumeStaticDelivery([]domain.Candidate{c})
	return err
}

// PurgeExpiredStaticCache deletes entire expired batches, including proofs,
// validators and credential-bearing candidates. The scheduler calls this
// each second, and refresh calls it before building conditional headers.
func (s *IngestService) PurgeExpiredStaticCache(ctx context.Context) error {
	release, err := s.static.fence.lock(ctx, true)
	if err != nil {
		return err
	}
	defer release()
	s.static.mu.Lock()
	defer s.static.mu.Unlock()
	for id, e := range s.static.entries {
		if !s.static.entryFresh(e, s.Now()) {
			s.static.removeEntryLocked(id)
		}
	}
	return nil
}

func observedFromEvidence(geo geoip.Resolver, ev domain.Evidence, now time.Time) *domain.Observation {
	return buildObservation(geo, ev, now)
}
