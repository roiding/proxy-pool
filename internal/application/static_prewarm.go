package application

import (
	"context"
	"encoding/json"
	"sync"

	"proxypool/internal/domain"
)

func scopeSourceAllowed(scope prewarmScope, id string) bool {
	if scope.SourceEmpty {
		return false
	}
	if scope.SourceUnrestricted {
		return true
	}
	for _, allowed := range scope.SourceIDs {
		if id == allowed {
			return true
		}
	}
	return false
}

func staticKindAllowed(scope prewarmScope) bool {
	if len(scope.Kinds) == 0 {
		return true
	}
	for _, k := range scope.Kinds {
		if k == domain.KindStatic {
			return true
		}
	}
	return false
}

func (p *PrewarmService) loadStaticLists(ctx context.Context, projectID string, scope prewarmScope) {
	if scope.SourceEmpty || !staticKindAllowed(scope) {
		return
	}
	ids, err := p.StaticSupply.StaticSourcesForRefill(ctx, projectID, 64)
	if err != nil {
		return
	}
	var pending []string
	for _, id := range ids {
		if !scopeSourceAllowed(scope, id) {
			continue
		}
		src, err := p.store.GetSource(ctx, id)
		if err != nil || !src.Enabled {
			continue
		}
		var cfg SourceConfig
		if json.Unmarshal([]byte(src.ConfigJSON), &cfg) != nil || !sourceProjectAllowed(cfg, projectID) {
			continue
		}
		if _, _, ok := p.StaticSupply.CachedStaticCandidates(id); ok {
			continue
		}
		if ctx.Err() != nil {
			return
		}
		pending = append(pending, id)
	}
	var wg sync.WaitGroup
	jobs := make(chan string)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range jobs {
				if ctx.Err() == nil {
					_, _ = p.StaticSupply.RefreshStaticSource(ctx, id)
				}
			}
		}()
	}
	for _, id := range pending {
		select {
		case jobs <- id:
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return
		}
	}
	close(jobs)
	wg.Wait()
}

// staticCandidates reads only live cached snapshots. Refresh is a separate,
// bounded phase once per job; Ready evaluation never performs network I/O.
func (p *PrewarmService) staticCandidates(ctx context.Context, projectID string, scope prewarmScope, tried map[string]bool, limit int) ([]domain.Candidate, error) {
	if limit <= 0 || scope.SourceEmpty || !staticKindAllowed(scope) {
		return nil, nil
	}
	ids, err := p.StaticSupply.StaticSourcesForRefill(ctx, projectID, 64)
	if err != nil {
		return nil, err
	}
	out := make([]domain.Candidate, 0, limit)
	for _, id := range ids {
		if !scopeSourceAllowed(scope, id) {
			continue
		}
		src, err := p.store.GetSource(ctx, id)
		if err != nil {
			return nil, err
		}
		var cfg SourceConfig
		if json.Unmarshal([]byte(src.ConfigJSON), &cfg) != nil || !sourceProjectAllowed(cfg, projectID) {
			continue
		}
		candidates, _, fresh := p.StaticSupply.CachedStaticCandidates(id)
		if !fresh {
			continue
		}
		for _, cand := range candidates {
			if cand.SourceConfigVersion != src.ConfigVersion || tried[cand.Resource.ID] {
				continue
			}
			if !scopeSupportsCandidate(RefillScope{ProjectID: projectID, Transports: scope.Transports, Countries: scope.Countries}, cand) {
				continue
			}
			available, err := p.store.EphemeralCandidateAvailable(ctx, cand, p.Now())
			if err != nil {
				return nil, err
			}
			if !available {
				continue
			}
			out = append(out, cand)
		}
	}
	out = fairCandidateOrder(out)
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
