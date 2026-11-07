package application

import (
	"context"

	"proxypool/internal/domain"
)

// A used sticky binding is a private connection snapshot, not list inventory.
// Missing/evicted list proof forces revalidation of that bound connection;
// it never authorizes reuse of the older private PASS on its own.
type StaticBindingEvidenceProvider interface {
	StaticBindingCandidate(sourceID string, version int64, fingerprint string) (domain.Candidate, bool)
	LockStaticBindingDelivery(ctx context.Context, sourceID string, version int64, fingerprint, scopeHash string, proof domain.Evidence, requireCurrent bool) (func(), bool, error)
}

func (s *IngestService) StaticBindingCandidate(sourceID string, version int64, fingerprint string) (domain.Candidate, bool) {
	s.static.mu.Lock()
	defer s.static.mu.Unlock()
	e, ok := s.static.entries[sourceID]
	if !ok || e.configVersion != version || !s.static.entryFresh(e, s.Now()) {
		return domain.Candidate{}, false
	}
	for _, c := range e.candidates {
		if s.fingerprint(c) == fingerprint {
			return cloneStaticCandidate(c), true
		}
	}
	return domain.Candidate{}, false
}

func (s *IngestService) LockStaticBindingDelivery(ctx context.Context, sourceID string, version int64, fingerprint, scopeHash string, proof domain.Evidence, requireCurrent bool) (func(), bool, error) {
	release, err := s.static.fence.lock(ctx, false)
	if err != nil {
		return nil, false, err
	}
	s.static.mu.Lock()
	defer s.static.mu.Unlock()
	e, ok := s.static.entries[sourceID]
	if ok && e.configVersion == version {
		if latest, exists := e.proofs[staticProofKey(fingerprint, scopeHash)]; exists && latest.Evidence.Outcome == "FAIL" && !latest.Evidence.CompletedAt.Before(proof.CompletedAt) {
			return release, false, nil
		}
		if proof.CacheHit || requireCurrent {
			latest, exists := e.proofs[staticProofKey(fingerprint, scopeHash)]
			if !s.static.entryFresh(e, s.Now()) || !exists || latest.Evidence.Outcome != "PASS" || latest.Evidence.CompletedAt.Before(proof.CompletedAt) || latest.Evidence.ObservedExitIP != proof.ObservedExitIP {
				return release, false, nil
			}
			for _, c := range e.candidates {
				if s.fingerprint(c) == fingerprint {
					return release, true, nil
				}
			}
			return release, false, nil
		}
	}
	if requireCurrent {
		return release, false, nil
	}
	// A real revalidation may succeed while the disposable list is absent.
	// SQLite still checks actual proof age, source/version/access/binding CAS.
	return release, !proof.CacheHit, nil
}
