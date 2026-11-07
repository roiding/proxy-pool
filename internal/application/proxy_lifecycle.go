package application

import (
	"context"
	"sort"
	"time"

	"proxypool/internal/domain"
	"proxypool/internal/storage/sqlite"
	"proxypool/internal/validation"
)

// A cancelled request/total job budget is not a proxy verdict. A completed
// probe that failed (including a target/Judge check) removes that candidate.
func probeHasVerdict(ctx context.Context, fail *validation.Failure) bool {
	// The per-candidate probe can report BUDGET when its own timeout fires.
	// Only the parent request/job cancellation is exempt from blacklisting.
	return fail != nil && ctx.Err() == nil
}

func retireFailedCandidate(ctx context.Context, store *sqlite.Store, supply any,
	c domain.Candidate, secret string, now time.Time, projectID string) error {
	if isStaticCandidate(c) {
		if cache, ok := supply.(*IngestService); ok && cache != nil {
			return cache.MarkStaticUnavailable(ctx, c, projectID)
		}
	}
	if c.Ephemeral {
		fp := domain.ConnectionFingerprint(secret, c.Resource.Conn.Transport, c.Resource.Conn.Host, c.Resource.Conn.Port, c.Resource.Conn.Username, c.Resource.Conn.Password)
		return store.MarkSourceProxyUnavailable(ctx, c, fp, projectID, "validation_failed", now)
	}
	src, err := store.GetSource(ctx, c.Offer.SourceID)
	if err != nil {
		return err
	}
	if src.AdapterType != AdapterCustom {
		return nil
	}
	n, err := store.RetireResourceOwned(ctx, c.Resource.ID, c.Incarnation,
		c.Offer.SourceID, c.SourceConfigVersion, "VALIDATION_FAILED", now)
	if err != nil || n == 0 {
		return err
	}
	fp := domain.ConnectionFingerprint(secret, c.Resource.Conn.Transport,
		c.Resource.Conn.Host, c.Resource.Conn.Port, c.Resource.Conn.Username, c.Resource.Conn.Password)
	if _, err := store.ExpireBindingsForFingerprints(ctx, []string{fp}, now); err != nil {
		return err
	}
	return store.InvalidateEvidence(ctx, []string{fp})
}

// All jobs share a bounded background pool and the deployment probe pool.
// Keep a slot for client requests where the deployment has more than one.
func prewarmWorkerLimit(slots chan struct{}) int {
	n := cap(slots) - 1
	if n < 1 {
		n = 1
	}
	return n
}

// Round-robin source instances rather than exhausting a large first list.
// Fingerprint deduplication is still done by the caller across all sources.
func fairCandidateOrder(rows []domain.Candidate, turns ...map[string]int) []domain.Candidate {
	groups := map[string][]domain.Candidate{}
	var ids []string
	for _, c := range rows {
		id := c.Offer.SourceID
		if _, ok := groups[id]; !ok {
			ids = append(ids, id)
		}
		groups[id] = append(groups[id], c)
	}
	if len(turns) > 0 {
		sort.SliceStable(ids, func(i, j int) bool { return turns[0][ids[i]] < turns[0][ids[j]] })
	}
	out := make([]domain.Candidate, 0, len(rows))
	for offset := 0; len(out) < len(rows); offset++ {
		for _, id := range ids {
			if offset < len(groups[id]) {
				out = append(out, groups[id][offset])
			}
		}
	}
	return out
}
