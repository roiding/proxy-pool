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
	return h.record(ctx, resourceID, contextKey, profile, outcome, "")
}

// RecordScoped pins the health write to the resource incarnation the probe
// selected, so a delete + re-import between probe start and completion can
// never attach the stale result to the recreated candidate.
func (h *HealthRecorder) RecordScoped(ctx context.Context, resourceID, contextKey,
	profile, outcome, incarnation string) error {
	return h.record(ctx, resourceID, contextKey, profile, outcome, incarnation)
}

func (h *HealthRecorder) record(ctx context.Context, resourceID, contextKey,
	profile, outcome, incarnation string) error {
	now := h.Now()
	if outcome == "PASS" {
		return h.Store.UpdateHealth(ctx, sqlite.HealthUpdate{
			ResourceID:  resourceID,
			ContextKey:  contextKey,
			Profile:     profile,
			Outcome:     "PASS",
			Incarnation: incarnation,
			Now:         now,
		})
	}
	health, err := h.Store.CurrentHealthGuarded(ctx, resourceID, contextKey,
		profile, incarnation)
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
		Incarnation:   incarnation,
		Now:           now,
	})
}
