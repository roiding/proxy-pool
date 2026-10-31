package maintenance

import (
	"context"
	"log/slog"
	"time"

	"proxypool/internal/storage/sqlite"
)

// RunOnce expires leases/reservations and prunes history. It never performs
// proxy liveness probes: that is a design invariant, not an optimization.
func RunOnce(ctx context.Context, store *sqlite.Store, retention time.Duration, log *slog.Logger) {
	stats, err := store.SweepExpired(ctx, time.Now(), retention)
	if err != nil {
		log.Error("maintenance sweep failed", "error", err)
		return
	}
	if stats.ExpiredLeases > 0 || stats.ExpiredReservations > 0 ||
		stats.PrunedAttempts > 0 || stats.PrunedFeedback > 0 {
		log.Info("maintenance sweep", "expired_leases", stats.ExpiredLeases,
			"expired_reservations", stats.ExpiredReservations,
			"pruned_attempts", stats.PrunedAttempts, "pruned_feedback", stats.PrunedFeedback)
	}
}

func Run(ctx context.Context, store *sqlite.Store, interval, retention time.Duration, log *slog.Logger) {
	RunOnce(ctx, store, retention, log)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			RunOnce(ctx, store, retention, log)
		}
	}
}
