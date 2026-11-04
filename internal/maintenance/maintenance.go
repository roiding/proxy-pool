package maintenance

import (
	"context"
	"log/slog"
	"time"

	"proxypool/internal/storage/sqlite"
)

// RunOnce expires leases/reservations, prunes history, and clears fully
// elapsed project-CD windows. It never performs proxy liveness probes and it
// never deletes inventory by failure counts: uncertain outcomes keep their
// candidates and backoff. The providerRetireThreshold parameter is retained
// for call-site stability but no longer drives any eviction.
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
	_ = providerRetireThreshold
	if n, err := store.ClearExpiredProjectCooldowns(ctx, time.Now()); err != nil {
		log.Error("expired project cooldown sweep failed", "error", err)
	} else if n > 0 {
		log.Info("expired project cooldown sweep", "cleared", n)
	}
	if n, err := store.SweepOrphanResources(ctx); err != nil {
		log.Error("orphan resource sweep failed", "error", err)
	} else if n > 0 {
		log.Info("orphan resource sweep", "removed", n)
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
