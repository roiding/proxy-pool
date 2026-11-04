package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"proxypool/internal/domain"
	"proxypool/internal/storage/sqlite"
)

func auditEvidenceKey(t *testing.T, e *env, now time.Time) sqlite.EvidenceKey {
	t.Helper()
	candidate := firstCandidate(t, e, now)
	source, err := e.store.GetSource(context.Background(), candidate.Offer.SourceID)
	if err != nil {
		t.Fatal(err)
	}
	return sqlite.EvidenceKey{
		ResourceID:          candidate.Resource.ID,
		Fingerprint:         e.acquire.FingerprintOf(candidate.Resource.Conn),
		SourceID:            source.ID,
		SourceConfigVersion: source.ConfigVersion,
		Incarnation:         candidate.Incarnation,
	}
}

func TestAuditWritingOldEvidenceDoesNotMakeItFresh(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	e.importProxy(t, "audit-evidence-age", startFakeProxy(t).URL)
	now := time.Now()
	key := auditEvidenceKey(t, e, now)
	ev := domain.Evidence{
		AttemptID: "audit-old-evidence", RequestID: "audit-request", Profile: "https_basic_v1",
		Mode: "basic", Outcome: "PASS", StartedAt: now.Add(-41 * time.Second),
		CompletedAt: now.Add(-40 * time.Second),
	}
	if err := e.store.PutEvidence(context.Background(), key, "audit-age-scope", ev); err != nil &&
		!errors.Is(err, sqlite.ErrStale) && !errors.Is(err, sqlite.ErrNotEligible) {
		t.Fatal(err)
	}
	recent, err := e.store.RecentEvidence(context.Background(), "audit-age-scope",
		[]sqlite.EvidenceKey{key}, now.Add(-30*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(recent) != 0 {
		t.Fatal("persisting old evidence reset its freshness window")
	}
}

func TestAuditLateOldPassCannotOverwriteNewerFailure(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	e.importProxy(t, "audit-evidence-order", startFakeProxy(t).URL)
	now := time.Now()
	key := auditEvidenceKey(t, e, now)
	negative := domain.Evidence{
		AttemptID: "audit-new-failure", RequestID: "audit-request", Profile: "https_basic_v1",
		Mode: "basic", Outcome: "FAIL", StartedAt: now.Add(-time.Second), CompletedAt: now,
	}
	if err := e.store.PutEvidence(context.Background(), key, "audit-order-scope", negative); err != nil {
		t.Fatal(err)
	}
	older := negative
	older.AttemptID = "audit-late-old-pass"
	older.Outcome = "PASS"
	older.StartedAt = now.Add(-3 * time.Second)
	older.CompletedAt = now.Add(-2 * time.Second)
	if err := e.store.PutEvidence(context.Background(), key, "audit-order-scope", older); err != nil &&
		!errors.Is(err, sqlite.ErrStale) && !errors.Is(err, sqlite.ErrNotEligible) {
		t.Fatal(err)
	}
	recent, err := e.store.RecentEvidence(context.Background(), "audit-order-scope",
		[]sqlite.EvidenceKey{key}, now.Add(-30*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(recent) != 0 {
		t.Fatal("an out-of-order old PASS replaced newer negative evidence")
	}
}

func TestAuditEvidenceWithoutCompletionCannotBecomeFreshPass(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	e.importProxy(t, "audit-incomplete-evidence", startFakeProxy(t).URL)
	now := time.Now()
	key := auditEvidenceKey(t, e, now)
	ev := domain.Evidence{
		AttemptID: "audit-no-completion", RequestID: "audit-request", Profile: "https_basic_v1",
		Mode: "basic", Outcome: "PASS", StartedAt: now.Add(-time.Minute),
	}
	if err := e.store.PutEvidence(context.Background(), key, "audit-incomplete-scope", ev); err != nil {
		return
	}
	recent, err := e.store.RecentEvidence(context.Background(), "audit-incomplete-scope",
		[]sqlite.EvidenceKey{key}, now.Add(-30*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(recent) != 0 {
		t.Fatal("an incomplete PASS acquired a fabricated fresh completion timestamp")
	}
}

func TestAuditSameSecondNewFailureHidesWholeSecondPass(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	e.importProxy(t, "audit-fractional-order", startFakeProxy(t).URL)
	whole := time.Now().UTC().Truncate(time.Second).Add(-time.Second)
	key := auditEvidenceKey(t, e, time.Now())
	pass := domain.Evidence{
		AttemptID: "audit-whole-second-pass", RequestID: "audit-request", Profile: "https_basic_v1",
		Mode: "basic", Outcome: "PASS", StartedAt: whole.Add(-time.Second), CompletedAt: whole,
	}
	if err := e.store.PutEvidence(context.Background(), key, "audit-fraction-scope", pass); err != nil {
		t.Fatal(err)
	}
	negative := pass
	negative.AttemptID = "audit-half-second-new-fail"
	negative.Outcome = "FAIL"
	negative.StartedAt = whole.Add(100 * time.Millisecond)
	negative.CompletedAt = whole.Add(500 * time.Millisecond)
	if err := e.store.PutEvidence(context.Background(), key, "audit-fraction-scope", negative); err != nil {
		t.Errorf("a chronologically newer failure was rejected: %v", err)
	}
	recent, err := e.store.RecentEvidence(context.Background(), "audit-fraction-scope",
		[]sqlite.EvidenceKey{key}, whole.Add(-time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(recent) != 0 {
		t.Fatal("RFC3339Nano text ordering retained a whole-second PASS after a newer fractional-second FAIL")
	}
}

func TestAuditOldIncarnationEvidenceCannotEvictCurrentPass(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	proxy := startFakeProxy(t)
	e.importProxy(t, "audit-evidence-incarnation", proxy.URL)
	ctx := context.Background()
	started := time.Now().UTC()
	old := auditEvidenceKey(t, e, started)
	retired, err := e.store.RetireResourceOwned(ctx, old.ResourceID, old.Incarnation,
		old.SourceID, old.SourceConfigVersion, "AUTH_FAILED", time.Now())
	if err != nil || retired == 0 {
		t.Fatalf("fixture resource retirement: offers=%d err=%v", retired, err)
	}
	if _, err := e.ingest.ImportLines(ctx, old.SourceID, proxy.URL+"\n"); err != nil {
		t.Fatal(err)
	}
	current := auditEvidenceKey(t, e, time.Now())
	if current.Incarnation == old.Incarnation {
		t.Fatal("fixture reimport did not create a new resource incarnation")
	}
	now := time.Now().UTC()
	pass := domain.Evidence{
		AttemptID: "audit-current-incarnation-pass", RequestID: "audit-request", Profile: "https_basic_v1",
		Mode: "basic", Outcome: "PASS", StartedAt: now, CompletedAt: now,
	}
	if err := e.store.PutEvidence(ctx, current, "audit-incarnation-scope", pass); err != nil {
		t.Fatal(err)
	}
	late := pass
	late.AttemptID = "audit-old-incarnation-late-failure"
	late.Outcome = "FAIL"
	late.StartedAt = started
	late.CompletedAt = time.Now().UTC()
	if err := e.store.PutEvidence(ctx, old, "audit-incarnation-scope", late); err != nil &&
		!errors.Is(err, sqlite.ErrStale) && !errors.Is(err, sqlite.ErrNotEligible) {
		t.Fatal(err)
	}
	recent, err := e.store.RecentEvidence(ctx, "audit-incarnation-scope",
		[]sqlite.EvidenceKey{current}, now.Add(-time.Minute))
	if err != nil || recent[current.Fingerprint].AttemptID != pass.AttemptID {
		t.Fatalf("a late old-incarnation result evicted current proof: recent=%+v err=%v", recent, err)
	}
}

func TestAuditObsoleteSourceProofCannotBeInserted(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		name := "updated"
		if !enabled {
			name = "disabled"
		}
		t.Run(name, func(t *testing.T) {
			e := newEnv(t, startJudge(t), nil)
			e.importProxy(t, "audit-source-proof", startFakeProxy(t).URL)
			ctx := context.Background()
			now := time.Now().UTC()
			key := auditEvidenceKey(t, e, now)
			proof := domain.Evidence{
				AttemptID: "audit-source-proof", RequestID: "audit-request",
				Profile: "https_basic_v1", Mode: "basic", Outcome: "PASS",
				StartedAt: now, CompletedAt: now,
			}
			if err := e.store.PutEvidenceProduction(ctx, key, "audit-current-source-control", proof); err != nil {
				t.Fatalf("current source proof must be accepted before the update: %v", err)
			}
			source, err := e.store.GetSource(ctx, key.SourceID)
			if err != nil {
				t.Fatal(err)
			}
			source.Enabled = enabled
			if err := e.store.UpdateSource(ctx, source, key.SourceConfigVersion, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			for _, resourceID := range []string{key.ResourceID, ""} {
				kind := "inventory"
				if resourceID == "" {
					kind = "resource-less"
				}
				t.Run(kind, func(t *testing.T) {
					// There is no existing row for this scope: INSERT must enforce
					// source version even without a public inventory foreign key.
					scope := "audit-obsolete-source-first-write-" + kind
					obsolete := key
					obsolete.ResourceID = resourceID
					err := e.store.PutEvidenceProduction(ctx, obsolete, scope, proof)
					if err == nil {
						t.Error("an old source-version proof was accepted on its first INSERT")
					} else if !errors.Is(err, sqlite.ErrStale) && !errors.Is(err, sqlite.ErrNotEligible) {
						t.Fatalf("obsolete proof must fail eligibility, not storage execution: %v", err)
					}
					var count int
					if err := e.store.DB().QueryRow(`SELECT COUNT(*) FROM validation_evidence WHERE scope_hash = ?`, scope).Scan(&count); err != nil {
						t.Fatal(err)
					}
					if count != 0 {
						t.Fatalf("obsolete source proof polluted the evidence cache: rows=%d", count)
					}
				})
			}
		})
	}
}
