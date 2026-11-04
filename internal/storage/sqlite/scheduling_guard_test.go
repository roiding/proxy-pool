package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"proxypool/internal/domain"
)

func newSchedulingStore(t *testing.T) (*Store, context.Context) {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "scheduling.db"), 1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store := New(db)
	ctx := context.Background()
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return store, ctx
}

func createSchedulingSource(t *testing.T, store *Store, ctx context.Context,
	id string, now time.Time) domain.Source {
	t.Helper()
	err := store.CreateSource(ctx, domain.Source{
		ID: id, Tenant: "default", AdapterType: "custom", ParserType: "lines",
		Enabled: true, ConfigJSON: `{"revision":1}`,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	src, err := store.GetSource(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return src
}

func updateSchedulingSource(t *testing.T, store *Store, ctx context.Context,
	src domain.Source, now time.Time) domain.Source {
	t.Helper()
	src.ConfigJSON = `{"revision":2}`
	if err := store.UpdateSource(ctx, src, src.ConfigVersion, now); err != nil {
		t.Fatal(err)
	}
	updated, err := store.GetSource(ctx, src.ID)
	if err != nil {
		t.Fatal(err)
	}
	return updated
}

func sourceCircuitState(t *testing.T, store *Store, ctx context.Context,
	sourceID string) (string, int) {
	t.Helper()
	var state string
	var failures int
	if err := store.DB().QueryRowContext(ctx, `
		SELECT state, consecutive_failures FROM source_circuits
		WHERE source_id = ?`, sourceID).Scan(&state, &failures); err != nil {
		t.Fatal(err)
	}
	return state, failures
}

func TestRecordSourceOutcomeOwnedRejectsStaleVersion(t *testing.T) {
	policy := SourceCircuitPolicy{
		FailureThreshold: 1,
		Base:             time.Minute,
		Max:              time.Hour,
	}
	for _, mode := range []string{"update", "delete-recreate"} {
		t.Run(mode, func(t *testing.T) {
			store, ctx := newSchedulingStore(t)
			now := time.Now().UTC()
			old := createSchedulingSource(t, store, ctx, "source-aba", now)
			var current domain.Source
			switch mode {
			case "update":
				current = updateSchedulingSource(t, store, ctx, old, now)
			case "delete-recreate":
				if _, err := store.DeleteSource(ctx, old.ID, now); err != nil {
					t.Fatal(err)
				}
				current = createSchedulingSource(t, store, ctx, old.ID, now)
			}
			if current.ConfigVersion <= old.ConfigVersion {
				t.Fatalf("test source version did not advance: old=%d current=%d",
					old.ConfigVersion, current.ConfigVersion)
			}

			if _, err := store.RecordSourceOutcomeOwned(ctx, current.ID,
				current.ConfigVersion, "FAIL", now, policy); err != nil {
				t.Fatal(err)
			}
			if state, _ := sourceCircuitState(t, store, ctx, current.ID); state != CircuitOpen {
				t.Fatalf("current circuit did not open: %s", state)
			}
			if _, err := store.RecordSourceOutcomeOwned(ctx, old.ID,
				old.ConfigVersion, "PASS", now.Add(time.Second), policy); !errors.Is(err, ErrStale) {
				t.Fatalf("old PASS guard error = %v, want ErrStale", err)
			}
			if state, _ := sourceCircuitState(t, store, ctx, current.ID); state != CircuitOpen {
				t.Fatalf("old PASS closed the current circuit: %s", state)
			}

			if _, err := store.RecordSourceOutcomeOwned(ctx, current.ID,
				current.ConfigVersion, "PASS", now.Add(2*time.Second), policy); err != nil {
				t.Fatal(err)
			}
			if _, err := store.RecordSourceOutcomeOwned(ctx, old.ID,
				old.ConfigVersion, "FAIL", now.Add(3*time.Second), policy); !errors.Is(err, ErrStale) {
				t.Fatalf("old FAIL guard error = %v, want ErrStale", err)
			}
			state, failures := sourceCircuitState(t, store, ctx, current.ID)
			if state != CircuitClosed || failures != 0 {
				t.Fatalf("old FAIL mutated the current circuit: state=%s failures=%d",
					state, failures)
			}
		})
	}
}

func TestRecordSourceOutcomeOwnedRequiresEnabledSource(t *testing.T) {
	store, ctx := newSchedulingStore(t)
	now := time.Now().UTC()
	src := createSchedulingSource(t, store, ctx, "source-disabled", now)
	src.Enabled = false
	if err := store.UpdateSource(ctx, src, src.ConfigVersion, now); err != nil {
		t.Fatal(err)
	}
	current, err := store.GetSource(ctx, src.ID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.RecordSourceOutcomeOwned(ctx, current.ID, current.ConfigVersion,
		"FAIL", now, SourceCircuitPolicy{FailureThreshold: 1})
	if !errors.Is(err, ErrNotEligible) {
		t.Fatalf("disabled source guard error = %v, want ErrNotEligible", err)
	}
	var count int
	if err := store.DB().QueryRowContext(ctx, `
		SELECT COUNT(*) FROM source_circuits WHERE source_id = ?`,
		current.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("disabled source outcome created %d circuit rows", count)
	}
}
