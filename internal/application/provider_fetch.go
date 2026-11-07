package application

import (
	"context"
	"errors"
	"time"

	"proxypool/internal/domain"
	"proxypool/internal/integrations/scdn"
	"proxypool/internal/storage/sqlite"
)

func (s *IngestService) fetchProviderBatch(ctx context.Context, src domain.Source, cfg ProviderConfig, wanted int) (scdn.FetchResult, *domain.Error) {
	if err := cfg.Validate(); err != nil {
		return scdn.FetchResult{}, domain.E("SOURCE_CONFIG_ERROR", 409, "provider config: %v", err)
	}
	select {
	case s.fetchSlots <- struct{}{}:
		defer func() { <-s.fetchSlots }()
	default:
		return scdn.FetchResult{}, domain.E("SOURCE_FETCH_CAPACITY", 503, "source download capacity is full")
	}
	started := s.Now()
	if err := s.store.ClaimSourceFetch(ctx, src.ID, src.ConfigVersion, started, cfg.minFetchInterval()); err != nil {
		if errors.Is(err, sqlite.ErrStale) {
			return scdn.FetchResult{}, domain.E("SOURCE_CONFIG_CHANGED", 409, "source changed before extraction")
		}
		return scdn.FetchResult{}, domain.AsError(err)
	}
	providerCfg := cfg.scdnConfig()
	if wanted > 0 && (providerCfg.FetchCount == 0 || providerCfg.FetchCount > wanted) {
		providerCfg.FetchCount = wanted
	}
	if providerCfg.FetchCount > scdn.MaxCount {
		providerCfg.FetchCount = scdn.MaxCount
	}
	fetchCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	realStarted := time.Now()
	batch, fetchErr := scdn.Fetch(fetchCtx, s.ScdnHTTPClient, s.ScdnEndpoint, providerCfg, s.allowPrivate)
	completed := s.Now()
	var derr *domain.Error
	if fetchErr != nil {
		derr = mapScdnError(fetchErr)
	}
	if ctx.Err() == nil {
		current, err := s.store.GetSource(ctx, src.ID)
		if err != nil {
			derr = domain.AsError(err)
		} else if current.ConfigVersion != src.ConfigVersion || !current.Enabled {
			derr = domain.E("SOURCE_CONFIG_CHANGED", 409, "source changed during extraction")
		}
	} else if derr == nil {
		derr = domain.E("REQUEST_CANCELED", 499, "provider extraction was cancelled")
	}
	outcome := sqlite.SourceFetchOutcome{SourceID: src.ID, ConfigVersion: src.ConfigVersion, StartedAt: started, CompletedAt: completed, NextFetchAt: s.nextSourceFetchAt(ctx, src.ID, completed, cfg.minFetchInterval(), derr), Fetched: batch.Fetched, Rejected: batch.RejectedCount, DurationMS: time.Since(realStarted).Milliseconds()}
	if derr != nil {
		outcome.ErrorCode = derr.Code
	}
	writeCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := s.store.FinishSourceFetch(writeCtx, outcome); err != nil {
		if errors.Is(err, sqlite.ErrStale) {
			return scdn.FetchResult{}, domain.E("SOURCE_CONFIG_CHANGED", 409, "source changed before extraction completion")
		}
		return scdn.FetchResult{}, domain.StoreUnavailable(err)
	}
	return batch, derr
}
