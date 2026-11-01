package application

import (
	"context"
	"encoding/json"
	"time"

	"proxypool/internal/domain"
	"proxypool/internal/storage/sqlite"
)

type LeaseService struct {
	store  *sqlite.Store
	health *HealthRecorder
	Now    func() time.Time
}

type CooldownConfig struct {
	Base   time.Duration
	Max    time.Duration
	Jitter float64
}

func NewLeaseService(store *sqlite.Store, cfg CooldownConfig) *LeaseService {
	return &LeaseService{store: store,
		health: &HealthRecorder{Store: store, Cooldown: cfg, Now: time.Now}, Now: time.Now}
}

// ownsLease denies callers that are neither admins nor the consumer the lease
// was delivered to; the not-found error hides the lease's existence.
func ownsLease(row sqlite.LeaseRow, consumerID string, isAdmin bool) *domain.Error {
	if isAdmin {
		return nil
	}
	if consumerID != "" && row.ConsumerID == consumerID {
		return nil
	}
	return domain.NotFound("lease")
}

func (s *LeaseService) Release(ctx context.Context, leaseID, consumerID string, isAdmin bool) (bool, *domain.Error) {
	row, err := s.store.GetLease(ctx, leaseID)
	if err != nil {
		return false, domain.AsError(err)
	}
	if derr := ownsLease(row, consumerID, isAdmin); derr != nil {
		return false, derr
	}
	ok, err := s.store.SetLeaseState(ctx, leaseID, "RELEASED", s.Now())
	if err != nil {
		return false, domain.StoreUnavailable(err)
	}
	if !ok {
		row, err := s.store.GetLease(ctx, leaseID)
		if err != nil {
			return false, domain.AsError(err)
		}
		if row.Lease.State == "ACTIVE" {
			return false, domain.E("LEASE_STATE_CONFLICT", 409, "lease cannot be released in state %s", row.Lease.State)
		}
	}
	return ok, nil
}

func (s *LeaseService) Feedback(ctx context.Context, leaseID, outcome, stage, note, consumerID string, isAdmin bool) *domain.Error {
	if outcome != "success" && outcome != "failure" {
		return domain.InvalidRequest("outcome must be success or failure")
	}
	row, err := s.store.GetLease(ctx, leaseID)
	if err != nil {
		return domain.AsError(err)
	}
	if derr := ownsLease(row, consumerID, isAdmin); derr != nil {
		return derr
	}
	now := s.Now()
	if err := s.store.RecordFeedback(ctx, leaseID, outcome, stage, note, now); err != nil {
		return domain.StoreUnavailable(err)
	}
	if outcome == "failure" {
		var evidence domain.Evidence
		if err := json.Unmarshal([]byte(row.ValidationJSON), &evidence); err != nil {
			return domain.AsError(err)
		}
		if err := s.health.Record(ctx, row.Lease.ResourceID, row.Lease.ContextKey, evidence.Profile, "FAIL"); err != nil {
			return domain.StoreUnavailable(err)
		}
	}
	return nil
}
