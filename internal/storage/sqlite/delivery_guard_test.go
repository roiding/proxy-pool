package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"proxypool/internal/domain"
)

func TestLiveResourceEligibleAcceptsOnlyNewerSameScopePass(t *testing.T) {
	store := newGuardStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	resourceID, incarnation := seedGuardResource(t, store, ctx, now)
	var offerID string
	if err := store.DB().QueryRowContext(ctx, `
		SELECT offer_id FROM source_offers WHERE resource_id = ?`,
		resourceID).Scan(&offerID); err != nil {
		t.Fatal(err)
	}

	const fingerprint = "fp-delivery-scope-guard"
	scope := domain.ValidationScope{
		Mode: "basic", Profile: "https_basic_v1", SecurityHash: "security-v1",
	}
	scopeHash := domain.EvidenceScopeHash(scope)
	prepared := domain.Evidence{
		AttemptID: "prepared-scope", Profile: scope.Profile, Mode: scope.Mode,
		SecurityHash: scope.SecurityHash, Outcome: "PASS",
		StartedAt: now.Add(-2 * time.Second), CompletedAt: now.Add(-time.Second),
	}
	validationJSON, err := json.Marshal(prepared)
	if err != nil {
		t.Fatal(err)
	}
	key := EvidenceKey{
		ResourceID: resourceID, Fingerprint: fingerprint, SourceID: "src-guard",
		SourceConfigVersion: 1, Incarnation: incarnation,
	}
	candidate := DeliveryCandidate{
		LeaseID: "lease-scope-guard", ReservationID: "res-scope-guard",
		ResourceID: resourceID, OfferID: offerID, SourceID: "src-guard",
		SourceConfigVersion: 1, Incarnation: incarnation, Fingerprint: fingerprint,
		ConnJSON:     `{"transport":"http","host":"203.0.113.5","port":8080}`,
		DeadlineText: fmtTime(now.Add(time.Minute)), MaxPoolLeases: 2,
		ExpectedEvidence: prepared, ValidationJSON: string(validationJSON),
	}

	putCurrent := func(ev domain.Evidence, currentScopeHash string) {
		t.Helper()
		if err := store.PutEvidenceProduction(ctx, key, currentScopeHash, ev); err != nil {
			t.Fatalf("put current evidence: %v", err)
		}
	}
	check := func() error {
		t.Helper()
		var got error
		if err := store.tx(ctx, func(tx *sql.Tx) error {
			_, got = liveResourceEligibleTx(tx, ctx, candidate, now, 0)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return got
	}

	newerSameScope := prepared
	newerSameScope.AttemptID = "current-same-scope"
	newerSameScope.CompletedAt = now
	putCurrent(newerSameScope, scopeHash)
	if err := check(); err != nil {
		t.Fatalf("newer same-scope PASS must replace the prepared PASS: %v", err)
	}

	for index, tc := range []struct {
		name   string
		mutate func(*domain.Evidence)
		want   error
	}{
		{
			name: "changed-mode",
			mutate: func(ev *domain.Evidence) {
				ev.Mode = "tls"
			},
			want: ErrStale,
		},
		{
			name: "changed-target",
			mutate: func(ev *domain.Evidence) {
				ev.Target = "example.com:443"
			},
			want: ErrStale,
		},
		{
			name: "changed-profile",
			mutate: func(ev *domain.Evidence) {
				ev.Profile = "target_tls_v1"
			},
			want: ErrStale,
		},
		{
			name: "changed-security-hash",
			mutate: func(ev *domain.Evidence) {
				ev.SecurityHash = "security-v2"
			},
			want: ErrStale,
		},
		{
			name: "newer-fail",
			mutate: func(ev *domain.Evidence) {
				ev.Outcome = "FAIL"
			},
			want: ErrStale,
		},
		{
			name: "future-pass",
			mutate: func(ev *domain.Evidence) {
				ev.CompletedAt = now.Add(time.Second)
			},
			want: ErrNotEligible,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			current := newerSameScope
			current.AttemptID = "current-" + tc.name
			current.CompletedAt = now.Add(time.Duration(index+1) * time.Microsecond)
			tc.mutate(&current)
			putCurrent(current, scopeHash)
			if err := check(); !errors.Is(err, tc.want) {
				t.Fatalf("current evidence error = %v, want %v", err, tc.want)
			}
		})
	}
}
