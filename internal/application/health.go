package application

import (
	"context"
	"time"

	"proxypool/internal/config"
	"proxypool/internal/policies"
	"proxypool/internal/storage/sqlite"
)

func newHealthRecorder(store *sqlite.Store, cfg config.Config) *HealthRecorder {
	return &HealthRecorder{Store: store, Cooldown: CooldownConfig{
		Base: cfg.CooldownBase(), Max: cfg.CooldownMax(), Jitter: cfg.CooldownJitter,
	}, Now: time.Now}
}

// HealthRecorder turns a probe outcome into scoped health state: PASS resets
// the failure streak and stamps last_pass; FAIL applies the backoff cooldown.
type HealthRecorder struct {
	Store    *sqlite.Store
	Cooldown CooldownConfig
	Now      func() time.Time
}

func (h *HealthRecorder) Record(ctx context.Context, resourceID, contextKey, profile, outcome string) error {
	now := h.Now()
	if outcome == "PASS" {
		return h.Store.UpdateHealth(ctx, sqlite.HealthUpdate{
			ResourceID: resourceID,
			ContextKey: contextKey,
			Profile:    profile,
			Outcome:    "PASS",
			Now:        now,
		})
	}
	health, err := h.Store.CurrentHealth(ctx, resourceID, contextKey, profile)
	if err != nil {
		return err
	}
	until := now.Add(policies.NextCooldown(
		health.ConsecutiveFailures+1, h.Cooldown.Base, h.Cooldown.Max, h.Cooldown.Jitter))
	return h.Store.UpdateHealth(ctx, sqlite.HealthUpdate{
		ResourceID:    resourceID,
		ContextKey:    contextKey,
		Profile:       profile,
		Outcome:       "FAIL",
		CooldownUntil: &until,
		Now:           now,
	})
}
