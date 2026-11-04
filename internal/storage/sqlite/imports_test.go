package sqlite

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"proxypool/internal/domain"
)

func newImportStore(t *testing.T) *Store {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "imports.db"), 0)
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

func importCandidate(sourceID string) domain.NormalizedCandidate {
	return domain.NormalizedCandidate{
		SourceID: sourceID, RecordID: "rec-1", Generation: "pool",
		ResourceID: "v1-test-resource", OfferID: "off-test",
		Conn: domain.Connection{Transport: domain.TransportHTTP, Host: "203.0.113.9", Port: 8080},
		Kind: domain.KindStatic, Session: domain.SessionStatic, MaxPoolLeases: 1,
	}
}

func seedImportSource(t *testing.T, store *Store, ctx context.Context, now time.Time) {
	t.Helper()
	if err := store.CreateSource(ctx, domain.Source{ID: "src", Tenant: "default",
		AdapterType: "custom", ParserType: "lines_v1", Enabled: true,
		ConfigJSON: "{}"}, now); err != nil {
		t.Fatalf("create source: %v", err)
	}
}

func TestApplyImportBatchIdempotencyAndAtomicity(t *testing.T) {
	store := newImportStore(t)
	ctx := context.Background()
	now := time.Now()
	seedImportSource(t, store, ctx, now)
	first, err := store.ApplyImportBatch(ctx, ImportBatchApply{
		SourceID: "src", UploaderID: "uk_1", IdempotencyKey: "k1",
		ContentHash: "hash-1", Candidates: []domain.NormalizedCandidate{importCandidate("src")},
		Now: now,
	})
	if err != nil || first.Replayed || first.Added != 1 || first.ImportID == "" {
		t.Fatalf("first apply: %+v err=%v", first, err)
	}
	replay, err := store.ApplyImportBatch(ctx, ImportBatchApply{
		SourceID: "src", UploaderID: "uk_1", IdempotencyKey: "k1",
		ContentHash: "hash-1", Candidates: []domain.NormalizedCandidate{importCandidate("src")},
		Now: now,
	})
	if err != nil || !replay.Replayed || replay.ImportID != first.ImportID {
		t.Fatalf("replay: %+v err=%v", replay, err)
	}
	if replay.Added != first.Added || replay.Rejected != first.Rejected {
		t.Fatalf("replay must return the stored counts without re-applying: %+v vs %+v",
			replay, first)
	}
	_, err = store.ApplyImportBatch(ctx, ImportBatchApply{
		SourceID: "src", UploaderID: "uk_1", IdempotencyKey: "k1",
		ContentHash: "hash-2", Candidates: []domain.NormalizedCandidate{importCandidate("src")},
		Now: now,
	})
	var derr *domain.Error
	if err == nil || !errorsAs(err, &derr) || derr.Code != "IDEMPOTENCY_KEY_CONFLICT" {
		t.Fatalf("different content must conflict: err=%v", err)
	}
	batch, err := store.GetImportBatch(ctx, "uk_1", "src", first.ImportID)
	if err != nil || batch.ImportID != first.ImportID {
		t.Fatalf("result read: %+v err=%v", batch, err)
	}
	if _, err := store.GetImportBatch(ctx, "uk_other", "src", first.ImportID); err == nil {
		t.Fatal("uploader-scoped read must not return another identity's batch")
	}
}

func errorsAs(err error, target **domain.Error) bool {
	d, ok := err.(*domain.Error)
	if ok {
		*target = d
	}
	return ok
}

func TestImportRetentionCleanupIsBounded(t *testing.T) {
	store := newImportStore(t)
	ctx := context.Background()
	now := time.Now()
	seedImportSource(t, store, ctx, now)
	old := now.Add(-ImportRecordRetention - time.Hour)
	// One apply creates the lazy import schema before seeding expired rows.
	if _, err := store.ApplyImportBatch(ctx, ImportBatchApply{
		SourceID: "src", UploaderID: "uk_seed", ContentHash: "hash-seed", Now: now,
	}); err != nil {
		t.Fatalf("schema apply: %v", err)
	}
	// Seed more expired rows than one bounded cleanup pass removes.
	for i := 0; i < maxImportCleanupRows+50; i++ {
		if _, err := store.db.ExecContext(ctx, `
			INSERT INTO import_batches(import_id, source_id, uploader_id, status,
			  added, updated, duplicate, rejected, errors_json, content_hash, created_at)
			VALUES(?,?,?,?,0,0,0,0,'[]','h',?)`,
			fmt.Sprintf("imp_old_%04d", i), "src", "uk_old", "completed",
			fmtTime(old)); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	if _, err := store.ApplyImportBatch(ctx, ImportBatchApply{
		SourceID: "src", UploaderID: "uk_new", ContentHash: "hash-new",
		Candidates: []domain.NormalizedCandidate{importCandidate("src")}, Now: now,
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	var remaining int
	if err := store.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM import_batches WHERE import_id LIKE 'imp_old_%'`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 50 {
		t.Fatalf("cleanup must be bounded to %d rows per apply, removed %d",
			maxImportCleanupRows, maxImportCleanupRows+50-remaining)
	}
}

func TestApplyImportBatchFinalSourceGuard(t *testing.T) {
	store := newImportStore(t)
	ctx := context.Background()
	now := time.Now()
	seedImportSource(t, store, ctx, now)
	// A concurrent disable between parse and commit must reject the batch.
	if err := store.UpdateSource(ctx, domain.Source{ID: "src", Tenant: "default",
		AdapterType: "custom", ParserType: "lines_v1", Enabled: false,
		ConfigJSON: "{}"}, 1, now); err != nil {
		t.Fatalf("disable source: %v", err)
	}
	res, err := store.ApplyImportBatch(ctx, ImportBatchApply{
		SourceID: "src", UploaderID: "uk_guard", SourceVersion: 1, ContentHash: "h",
		Candidates: []domain.NormalizedCandidate{importCandidate("src")}, Now: now,
	})
	var derr *domain.Error
	if err == nil || !errorsAs(err, &derr) || derr.Code != "SOURCE_STATE_CHANGED" {
		t.Fatalf("disabled source must reject at commit: %+v err=%v", res, err)
	}
	stats, err := store.Stats(ctx)
	if err != nil || stats.Resources != 0 {
		t.Fatalf("rejected commit must not change inventory: %+v err=%v", stats, err)
	}
	// Deleting the source entirely is a 404, never an inventory write.
	if _, err := store.db.ExecContext(ctx, `DELETE FROM sources WHERE source_id = 'src'`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplyImportBatch(ctx, ImportBatchApply{
		SourceID: "src", UploaderID: "uk_guard", SourceVersion: 1, ContentHash: "h", Now: now,
	}); err == nil {
		t.Fatal("deleted source must be a commit-time 404")
	}
}

func TestApplyImportBatchRejectsChangedVersionBeforeReplay(t *testing.T) {
	store := newImportStore(t)
	ctx := context.Background()
	now := time.Now()
	seedImportSource(t, store, ctx, now)
	first, err := store.ApplyImportBatch(ctx, ImportBatchApply{
		SourceID: "src", UploaderID: "uk_guard", SourceVersion: 1,
		IdempotencyKey: "version-guard", ContentHash: "h",
		Candidates: []domain.NormalizedCandidate{importCandidate("src")}, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateSource(ctx, domain.Source{ID: "src", Tenant: "default",
		AdapterType: "custom", ParserType: "lines_v1", Enabled: true,
		ConfigJSON: `{"max_pool_leases":2}`}, 1, now); err != nil {
		t.Fatal(err)
	}
	_, err = store.ApplyImportBatch(ctx, ImportBatchApply{
		SourceID: "src", UploaderID: "uk_guard", SourceVersion: 1,
		IdempotencyKey: "version-guard", ContentHash: "h",
		Candidates: []domain.NormalizedCandidate{importCandidate("src")}, Now: now,
	})
	var derr *domain.Error
	if err == nil || !errorsAs(err, &derr) || derr.Code != "SOURCE_STATE_CHANGED" {
		t.Fatalf("stale source version replay must fail closed: err=%v", err)
	}
	got, err := store.GetImportBatch(ctx, "uk_guard", "src", first.ImportID)
	if err != nil || got.ImportID != first.ImportID {
		t.Fatalf("rejected replay must preserve the original result: %+v err=%v", got, err)
	}
}

func TestApplyImportBatchCorruptStoredResultDoesNotReapply(t *testing.T) {
	store := newImportStore(t)
	ctx := context.Background()
	now := time.Now()
	seedImportSource(t, store, ctx, now)
	first, err := store.ApplyImportBatch(ctx, ImportBatchApply{
		SourceID: "src", UploaderID: "uk_corrupt", SourceVersion: 1,
		IdempotencyKey: "corrupt", ContentHash: "h",
		Candidates: []domain.NormalizedCandidate{importCandidate("src")}, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx,
		`UPDATE import_batches SET errors_json='not-json' WHERE import_id=?`,
		first.ImportID); err != nil {
		t.Fatal(err)
	}
	_, err = store.ApplyImportBatch(ctx, ImportBatchApply{
		SourceID: "src", UploaderID: "uk_corrupt", SourceVersion: 1,
		IdempotencyKey: "corrupt", ContentHash: "h",
		Candidates: []domain.NormalizedCandidate{importCandidate("src")}, Now: now,
	})
	if err == nil {
		t.Fatal("corrupt stored result must fail instead of re-applying")
	}
	var batches, offers int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM import_batches`).Scan(&batches); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM source_offers`).Scan(&offers); err != nil {
		t.Fatal(err)
	}
	if batches != 1 || offers != 1 {
		t.Fatalf("corrupt replay changed committed data: batches=%d offers=%d", batches, offers)
	}
}

func TestGetImportBatchWithoutSchemaIsNotFound(t *testing.T) {
	store := newImportStore(t)
	if _, err := store.GetImportBatch(context.Background(), "", "src", "imp_none"); err == nil {
		t.Fatal("querying a store with no imports must be a not-found, not an error")
	} else {
		var derr *domain.Error
		if !errorsAs(err, &derr) || derr.Status != 404 {
			t.Fatalf("missing schema must surface as 404: err=%v", err)
		}
	}
}
