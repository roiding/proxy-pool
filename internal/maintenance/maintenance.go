package maintenance

import (
	"context"
	"log/slog"
	"time"

	"proxypool/internal/storage/sqlite"
)

// RunOnce expires leases/reservations, prunes history, and retires provider
// resources whose request-time validation evidence reached the failure
// threshold. It never performs proxy liveness probes: that is a design
// invariant, not an optimization.
func RunOnce(ctx context.Context, store *sqlite.Store, retention time.Duration,
	providerRetireThreshold int, log *slog.Logger) {
	stats, err := store.SweepExpired(ctx, time.Now(), retention)
	if err != nil {
		log.Error("maintenance sweep failed", "error", err)
		return
	}
	if stats.ExpiredLeases > 0 || stats.ExpiredReservations > 0 ||
		stats.PrunedAttempts > 0 || stats.PrunedFeedback > 0 || stats.PrunedCooldowns > 0 {
		log.Info("maintenance sweep", "expired_leases", stats.ExpiredLeases,
			"expired_reservations", stats.ExpiredReservations,
			"pruned_attempts", stats.PrunedAttempts, "pruned_feedback", stats.PrunedFeedback,
			"pruned_cooldowns", stats.PrunedCooldowns)
	}
	if providerRetireThreshold > 0 {
		retireFailedProviders(ctx, store, providerRetireThreshold, log)
	}
}

// retireFailedProviders mirrors the custom-source consecutive-failure
// eviction for provider channels: acquire attempts are real validation
// evidence, so resources that keep failing are tombstoned and removed.
func retireFailedProviders(ctx context.Context, store *sqlite.Store, threshold int, log *slog.Logger) {
	resourceIDs, err := store.RetireCandidates(ctx, threshold, "provider_api", "")
	if err != nil {
		log.Error("provider retirement lookup failed", "error", err)
		return
	}
	retired := 0
	for _, resourceID := range resourceIDs {
		if count, err := store.RetireResource(ctx, resourceID, "EVICTED", time.Now()); err == nil {
			retired += count
		}
	}
	if retired > 0 {
		log.Info("provider resource retirement", "retired", retired)
	}
}

func Run(ctx context.Context, store *sqlite.Store, interval, retention time.Duration,
	providerRetireThreshold int, log *slog.Logger) {
	RunOnce(ctx, store, retention, providerRetireThreshold, log)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			RunOnce(ctx, store, retention, providerRetireThreshold, log)
		}
	}
}
