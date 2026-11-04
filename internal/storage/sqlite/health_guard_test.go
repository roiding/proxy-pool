package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"proxypool/internal/domain"
)

func newGuardStore(t *testing.T) *Store {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "guard.db"), 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	store := New(db)
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return store
}

func seedGuardResource(t *testing.T, s *Store, ctx context.Context, now time.Time) (string, string) {
	t.Helper()
	if err := s.CreateSource(ctx, domain.Source{ID: "src-guard", Tenant: "default",
		AdapterType: "custom", ParserType: "lines_v1", Enabled: true,
		ConfigJSON: "{}"}, now); err != nil {
		var derr *domain.Error
		if !errors.As(err, &derr) || derr.Code != "SOURCE_EXISTS" {
			t.Fatalf("create source: %v", err)
		}
	}
	cand := domain.NormalizedCandidate{
		SourceID: "src-guard", RecordID: "rec-1", Generation: "pool",
		Conn: domain.Connection{Transport: domain.TransportHTTP, Host: "203.0.113.5", Port: 8080},
		Kind: domain.KindStatic, Session: domain.SessionStatic, MaxPoolLeases: 1,
	}
	cand.CredentialVersion = domain.CredentialVersion("secret", "", "")
	cand.ResourceID = domain.ResourceID("secret", domain.IdentityKey{
		Tenant: "default", Transport: cand.Conn.Transport, Host: cand.Conn.Host,
		Port: cand.Conn.Port, CredentialVersion: cand.CredentialVersion, Session: cand.Session})
	cand.OfferID = domain.OfferID("secret", cand.SourceID, cand.RecordID, cand.Generation)
	if _, err := s.ImportCandidates(ctx, []domain.NormalizedCandidate{cand}, now); err != nil {
		t.Fatalf("import: %v", err)
	}
	var incarnation string
	if err := s.db.QueryRowContext(ctx,
		`SELECT incarnation FROM proxy_resources WHERE resource_id = ?`, cand.ResourceID).
		Scan(&incarnation); err != nil {
		t.Fatalf("read incarnation: %v", err)
	}
	if incarnation == "" {
		t.Fatal("fresh resource must carry an incarnation")
	}
	return cand.ResourceID, incarnation
}

// TestUpdateHealthIncarnationGuard pins the delete/re-import protection: a
// slow probe holding the old incarnation can never write health into the
// recreated resource.
func TestUpdateHealthIncarnationGuard(t *testing.T) {
	store := newGuardStore(t)
	ctx := context.Background()
	now := time.Now()
	resourceID, incarnation := seedGuardResource(t, store, ctx, now)
	contextKey := domain.ContextKey("", domain.SessionStatic)

	ok := HealthUpdate{ResourceID: resourceID, ContextKey: contextKey,
		Profile: "https_basic_v1", Outcome: "PASS", Incarnation: incarnation, Now: now}
	if err := store.UpdateHealth(ctx, ok); err != nil {
		t.Fatalf("guarded pass: %v", err)
	}
	row, err := store.CurrentHealth(ctx, resourceID, contextKey, "https_basic_v1")
	if err != nil || row.LastOutcome != "PASS" {
		t.Fatalf("pass write: %+v %v", row, err)
	}
	fail := ok
	fail.Outcome = "FAIL"
	until := now.Add(-time.Second)
	fail.CooldownUntil = &until
	if err := store.UpdateHealth(ctx, fail); err != nil {
		t.Fatalf("guarded fail: %v", err)
	}
	row, _ = store.CurrentHealth(ctx, resourceID, contextKey, "https_basic_v1")
	if row.LastOutcome != "FAIL" || row.ConsecutiveFailures != 1 {
		t.Fatalf("fail write: %+v", row)
	}

	// Delete + re-import: a new incarnation appears; the old probe's write
	// must be dropped instead of resurrecting state.
	if _, err := store.RetireResource(ctx, resourceID, "AUTH_FAILED", now); err != nil {
		t.Fatalf("retire: %v", err)
	}
	resourceID2, incarnation2 := seedGuardResource(t, store, ctx, now)
	if resourceID2 != resourceID || incarnation2 == incarnation {
		t.Fatalf("re-import must keep the id but change the incarnation: %q %q", incarnation, incarnation2)
	}
	stale := ok
	if err := store.UpdateHealth(ctx, stale); err != nil {
		t.Fatalf("stale write must not error: %v", err)
	}
	if err := store.UpdateHealth(ctx, HealthUpdate{
		ResourceID: resourceID, ContextKey: contextKey, Profile: "https_basic_v1",
		Outcome: "PASS", Incarnation: incarnation2, Now: now,
	}); err != nil {
		t.Fatalf("new incarnation pass: %v", err)
	}
	if err := store.UpdateHealth(ctx, stale); err != nil {
		t.Fatalf("old stale failure must not error: %v", err)
	}
	row, _ = store.CurrentHealth(ctx, resourceID, contextKey, "https_basic_v1")
	if row.LastOutcome != "PASS" {
		t.Fatalf("stale incarnation write overwrote replacement health: %+v", row)
	}
}

// TestRecordAttemptGuard pins the same protection for attempt rows.
func TestRecordAttemptGuard(t *testing.T) {
	store := newGuardStore(t)
	ctx := context.Background()
	now := time.Now()
	resourceID, incarnation := seedGuardResource(t, store, ctx, now)
	ev := domain.Evidence{AttemptID: "att-1", Profile: "https_basic_v1",
		Outcome: "PASS", StartedAt: now, CompletedAt: now}
	if err := store.RecordAttemptScoped(ctx, ev, resourceID, "ck", "", incarnation); err != nil {
		t.Fatalf("scoped attempt: %v", err)
	}
	var n int
	if err := store.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM validation_attempts WHERE attempt_id = 'att-1'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("attempt row: %d %v", n, err)
	}
	if _, err := store.RetireResource(ctx, resourceID, "AUTH_FAILED", now); err != nil {
		t.Fatalf("retire: %v", err)
	}
	_, incarnation2 := seedGuardResource(t, store, ctx, now)
	stale := ev
	stale.AttemptID = "att-2"
	if err := store.RecordAttemptScoped(ctx, stale, resourceID, "ck", "", incarnation); err != nil {
		t.Fatalf("stale attempt must not error: %v", err)
	}
	if err := store.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM validation_attempts WHERE attempt_id = 'att-2'`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("stale incarnation attempt must be dropped: %d %v", n, err)
	}
	_ = incarnation2
}

func TestRetireResourceOwnedRejectsChangedSourceVersionAndIncarnation(t *testing.T) {
	store := newGuardStore(t)
	ctx := context.Background()
	now := time.Now()
	resourceID, incarnation := seedGuardResource(t, store, ctx, now)

	// A stale config version must not delete the offer or the resource.
	if _, err := store.RetireResourceOwned(ctx, resourceID, incarnation,
		"src-guard", 999, "AUTH_FAILED", now); err != nil {
		t.Fatalf("stale version retire: %v", err)
	}
	var offers int
	if err := store.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM source_offers WHERE resource_id = ?`, resourceID).
		Scan(&offers); err != nil || offers != 1 {
		t.Fatalf("stale version must leave offer: count=%d err=%v", offers, err)
	}

	// Re-import under a new incarnation, then retire with the old token. The
	// old AUTH result must not remove the new object.
	if _, err := store.RetireResourceOwned(ctx, resourceID, incarnation,
		"src-guard", 1, "AUTH_FAILED", now); err != nil {
		t.Fatalf("owned retire: %v", err)
	}
	resourceID2, incarnation2 := seedGuardResource(t, store, ctx, now)
	if resourceID2 != resourceID || incarnation2 == incarnation {
		t.Fatal("fixture must recreate the same resource under a new incarnation")
	}
	if _, err := store.RetireResourceOwned(ctx, resourceID, incarnation,
		"src-guard", 1, "AUTH_FAILED", now); err != nil {
		t.Fatalf("stale incarnation retire: %v", err)
	}
	stats, err := store.Stats(ctx)
	if err != nil || stats.Resources != 1 || stats.EnabledOffers != 1 {
		t.Fatalf("stale retire removed the replacement: stats=%+v err=%v", stats, err)
	}
}

func TestRecordAttemptAttributedSurvivesOfferRetire(t *testing.T) {
	store := newGuardStore(t)
	ctx := context.Background()
	now := time.Now()
	if err := store.EnsureSupplySchema(ctx); err != nil {
		t.Fatal(err)
	}
	resourceID, incarnation := seedGuardResource(t, store, ctx, now)
	ev := domain.Evidence{AttemptID: "att-auth", Profile: "https_basic_v1",
		Outcome: "FAIL", StartedAt: now, CompletedAt: now}
	if err := store.RecordAttemptAttributed(ctx, ev, resourceID, "ck", "AUTH: rejected",
		incarnation, "src-guard", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RetireResourceOwned(ctx, resourceID, incarnation,
		"src-guard", 1, "AUTH_FAILED", now); err != nil {
		t.Fatal(err)
	}
	stats, err := store.DailySourceStats(ctx, now.Add(-time.Hour), now.Add(time.Hour))
	if err != nil || len(stats) != 1 || stats[0].Fail != 1 || stats[0].SourceID != "src-guard" {
		t.Fatalf("source failure attribution was lost with the offer: %+v err=%v", stats, err)
	}
	daily, err := store.SourceStats(ctx, now.Add(-time.Hour), now.Add(time.Hour))
	if err != nil || len(daily) != 1 || daily[0].Fail != 1 {
		t.Fatalf("durable source counter lost failure: %+v err=%v", daily, err)
	}
	// A re-imported object must not inherit old health after a stale-auth
	// guarded write; the new incarnation has no health row yet.
	_, incarnation2 := seedGuardResource(t, store, ctx, now)
	row, err := store.CurrentHealthGuarded(ctx, resourceID, "ck", "https_basic_v1", incarnation2)
	if err != nil || row.LastOutcome != "UNKNOWN" {
		t.Fatalf("replacement health: %+v err=%v", row, err)
	}
}

func TestEphemeralAttemptAttributionCountsWithoutInventory(t *testing.T) {
	store := newGuardStore(t)
	ctx := context.Background()
	now := time.Now()
	if err := store.EnsureSupplySchema(ctx); err != nil {
		t.Fatal(err)
	}
	for i, outcome := range []string{"PASS", "FAIL"} {
		ev := domain.Evidence{
			AttemptID: domain.NewID("att_ephemeral_"), Outcome: outcome,
			StartedAt: now, CompletedAt: now.Add(time.Duration(i) * time.Millisecond),
			Profile: "https_basic_v1",
		}
		if err := store.RecordAttemptAttributed(ctx, ev, "ephemeral-id", "ck", "",
			"ondemand_batch", "ephemeral-source", 1); err != nil {
			t.Fatal(err)
		}
	}
	stats, err := store.DailySourceStats(ctx, now.Add(-time.Minute), now.Add(time.Minute))
	if err != nil || len(stats) != 1 || stats[0].Pass != 1 || stats[0].Fail != 1 {
		t.Fatalf("ephemeral validation missing from stats: %+v err=%v", stats, err)
	}
	var resources int
	if err := store.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM proxy_resources`).Scan(&resources); err != nil || resources != 0 {
		t.Fatalf("ephemeral attribution created inventory: %d %v", resources, err)
	}
}
