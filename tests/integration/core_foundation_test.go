package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"proxypool/internal/domain"
	"proxypool/internal/storage/sqlite"
)

func newCoreStore(t *testing.T) *sqlite.Store {
	t.Helper()
	e := newEnv(t, startJudge(t), nil)
	return e.store
}

func TestProjectAllowedSourcesNilVsEmpty(t *testing.T) {
	store := newCoreStore(t)
	ctx := context.Background()
	now := time.Now()
	unrestricted := domain.Project{ID: "proj-all", Name: "all", Enabled: true,
		AllowedSourceIDs: nil, CreatedAt: now, UpdatedAt: now}
	if _, err := store.UpsertProject(ctx, unrestricted, now); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetProject(ctx, "proj-all")
	if err != nil || got.AllowedSourceIDs != nil {
		t.Fatalf("nil allowlist must stay unrestricted: %+v err=%v", got.AllowedSourceIDs, err)
	}
	if !got.SourceAllowed("any-source") {
		t.Fatal("nil allowlist must allow any source")
	}
	none := domain.Project{ID: "proj-none", Name: "none", Enabled: true,
		AllowedSourceIDs: []string{}, CreatedAt: now, UpdatedAt: now}
	if _, err := store.UpsertProject(ctx, none, now); err != nil {
		t.Fatal(err)
	}
	got, err = store.GetProject(ctx, "proj-none")
	if err != nil || got.AllowedSourceIDs == nil || len(got.AllowedSourceIDs) != 0 {
		t.Fatalf("empty allowlist must stay none: %#v err=%v", got.AllowedSourceIDs, err)
	}
	if got.SourceAllowed("any-source") {
		t.Fatal("empty allowlist must allow nothing")
	}
	narrowed := got.FilterAllowedSources([]string{"s1", "s2"})
	if narrowed == nil || len(narrowed) != 0 {
		t.Fatalf("an empty intersection must never read as all: %#v", narrowed)
	}
	if _, err := store.UpsertProject(ctx, domain.Project{ID: "proj-none", Name: "none2",
		Enabled: true, AllowedSourceIDs: []string{"s1"}, CreatedAt: now, UpdatedAt: now}, now); err != nil {
		t.Fatal(err)
	}
	got, _ = store.GetProject(ctx, "proj-none")
	if got.ConfigVersion != 2 || got.Name != "none2" || len(got.AllowedSourceIDs) != 1 {
		t.Fatalf("upsert must bump config version and replace fields: %+v", got)
	}
}

func TestProjectCooldownMergeAndKeyDeletionRetention(t *testing.T) {
	store := newCoreStore(t)
	ctx := context.Background()
	now := time.Now()
	if err := store.CreateAPIKey(ctx, domain.APIKey{
		ID: "key-del", ProjectID: sqlite.DefaultProjectID, Name: "key-del",
		TokenPrefix: "pk_key_del", Enabled: true, CreatedAt: now, UpdatedAt: now,
	}, "hash-key-del"); err != nil {
		t.Fatal(err)
	}
	fp := domain.ConnectionFingerprint("secret", domain.TransportHTTP, "10.1.1.1", 8080, "", "")
	if err := store.RecordProjectDeliveries(ctx, sqlite.DefaultProjectID, 10*time.Minute, now, []string{fp}); err != nil {
		t.Fatal(err)
	}
	blocked, err := store.ProjectCooldownBlocked(ctx, sqlite.DefaultProjectID, now.Add(time.Second), []string{fp})
	if err != nil || len(blocked) != 1 {
		t.Fatalf("delivery must open the window: %+v err=%v", blocked, err)
	}
	// A later, shorter delivery must never shorten the live window.
	if err := store.RecordProjectDeliveries(ctx, sqlite.DefaultProjectID, time.Minute,
		now.Add(5*time.Second), []string{fp}); err != nil {
		t.Fatal(err)
	}
	still, _ := store.ProjectCooldownBlocked(ctx, sqlite.DefaultProjectID, now.Add(time.Second), []string{fp})
	if !still[fp].Equal(blocked[fp]) {
		t.Fatalf("shorter cooldown shortened the window: %s -> %s", blocked[fp], still[fp])
	}
	// Deleting the key must not touch the project CD.
	if err := store.DeleteAPIKey(ctx, "key-del"); err != nil {
		t.Fatal(err)
	}
	after, err := store.ProjectCooldownBlocked(ctx, sqlite.DefaultProjectID, now.Add(time.Second), []string{fp})
	if err != nil || len(after) != 1 {
		t.Fatalf("key deletion must not clear project CD: %+v err=%v", after, err)
	}
	// The window is gone once it expires.
	gone, _ := store.ProjectCooldownBlocked(ctx, sqlite.DefaultProjectID,
		blocked[fp].Add(time.Second), []string{fp})
	if len(gone) != 0 {
		t.Fatalf("expired window still blocks: %+v", gone)
	}
	if _, err := store.APIKeyByID(ctx, "key-del"); err == nil {
		t.Fatalf("deleted key must not resolve: %v", err)
	}
}

func TestStickyBindingVersionCASAndExpiry(t *testing.T) {
	store := newCoreStore(t)
	ctx := context.Background()
	now := time.Now()
	b := domain.StickyBinding{
		ProjectID: "proj-sticky", SessionID: "sess-1", Fingerprint: "fp-a",
		SourceID: "src-a", SourceConfigVersion: 3, ConnectionCiphertext: "cipher-a",
		Deadline: now.Add(time.Hour),
	}
	if err := store.PutBinding(ctx, b, 0, now); err != nil {
		t.Fatal(err)
	}
	if err := store.PutBinding(ctx, b, 0, now); !errors.Is(err, sqlite.ErrBindingVersionConflict) {
		t.Fatalf("second create must conflict: %v", err)
	}
	got, err := store.GetBinding(ctx, "proj-sticky", "sess-1")
	if err != nil || got.Version != 1 || got.Fingerprint != "fp-a" || got.State != "ACTIVE" {
		t.Fatalf("binding readback: %+v err=%v", got, err)
	}
	rebind := b
	rebind.Fingerprint = "fp-b"
	rebind.ConnectionCiphertext = "cipher-b"
	if err := store.PutBinding(ctx, rebind, 1, now); err != nil {
		t.Fatal(err)
	}
	got, _ = store.GetBinding(ctx, "proj-sticky", "sess-1")
	if got.Version != 2 || got.Fingerprint != "fp-b" {
		t.Fatalf("rebind must bump version atomically: %+v", got)
	}
	if err := store.PutBinding(ctx, rebind, 1, now); !errors.Is(err, sqlite.ErrBindingVersionConflict) {
		t.Fatalf("stale writer must not overwrite: %v", err)
	}
	expired := b
	expired.SessionID = "sess-2"
	expired.Deadline = now.Add(-time.Second)
	if err := store.PutBinding(ctx, expired, 0, now); err != nil {
		t.Fatal(err)
	}
	n, err := store.ExpireBindings(ctx, now)
	if err != nil || n != 1 {
		t.Fatalf("expire bindings: n=%d err=%v", n, err)
	}
	got, _ = store.GetBinding(ctx, "proj-sticky", "sess-2")
	if got.State != "EXPIRED" {
		t.Fatalf("lapsed binding must be expired: %+v", got)
	}
	if err := store.DeleteBinding(ctx, "proj-sticky", "sess-2"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetBinding(ctx, "proj-sticky", "sess-2"); err == nil {
		t.Fatal("deleted binding still readable")
	}
}

func TestJobsLifecycle(t *testing.T) {
	store := newCoreStore(t)
	ctx := context.Background()
	now := time.Now()
	job := domain.Job{ID: "job-1", ProjectID: sqlite.DefaultProjectID, Kind: "prewarm",
		ParamsJSON: `{"source":"s1"}`, Target: 10}
	if err := store.CreateJob(ctx, job, now); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetJob(ctx, "job-1")
	if err != nil || got.State != "PENDING" || got.Target != 10 {
		t.Fatalf("job readback: %+v err=%v", got, err)
	}
	if err := store.StartJobRun(ctx, "job-1", now); err != nil {
		t.Fatal(err)
	}
	if err := store.SetJobProgress(ctx, "job-1", 1, 2, 1, "boom", now); err != nil {
		t.Fatal(err)
	}
	got, _ = store.GetJob(ctx, "job-1")
	if got.State != "RUNNING" || got.Ready != 1 || got.Probed != 2 || got.Failed != 1 || got.LastError != "boom" {
		t.Fatalf("job progress: %+v", got)
	}
	if err := store.FinishJob(ctx, "job-1", "COMPLETED", "target_reached", "", now); err != nil {
		t.Fatal(err)
	}
	got, _ = store.GetJob(ctx, "job-1")
	if got.State != "COMPLETED" || got.StopReason != "target_reached" || got.FinishedAt == nil {
		t.Fatalf("job finish: %+v", got)
	}
	if err := store.SetJobProgress(ctx, "job-1", 2, 0, 0, "", now); err == nil {
		t.Fatal("finished job must not accept progress")
	}
	jobs, err := store.ListProjectJobs(ctx, sqlite.DefaultProjectID, 10)
	if err != nil || len(jobs) != 1 || jobs[0].ID != "job-1" {
		t.Fatalf("project jobs: %+v err=%v", jobs, err)
	}
}

func TestEvidenceScopeAndIncarnationGuards(t *testing.T) {
	store := newCoreStore(t)
	ctx := context.Background()
	now := time.Now()
	scopeHash := domain.EvidenceScopeHash(domain.ValidationScope{
		Mode: "tls", TargetHostPort: "example.com:443", Profile: "target_tls_v1"})
	key := sqlite.EvidenceKey{Fingerprint: "fp-ev", SourceID: "src-ev",
		SourceConfigVersion: 4, Incarnation: "inc-1"}
	pass := domain.Evidence{AttemptID: "att-1", Outcome: "PASS",
		StartedAt: now, CompletedAt: now}
	if err := store.PutEvidence(ctx, key, scopeHash, pass); err != nil {
		t.Fatal(err)
	}
	hits, err := store.RecentEvidence(ctx, scopeHash, []sqlite.EvidenceKey{key},
		now.Add(-time.Minute))
	if err != nil || len(hits) != 1 || !hits["fp-ev"].CacheHit {
		t.Fatalf("fresh pass evidence must hit: %+v err=%v", hits, err)
	}
	// A new incarnation (delete + reimport) must not inherit the old PASS.
	moved := key
	moved.Incarnation = "inc-2"
	hits, _ = store.RecentEvidence(ctx, scopeHash, []sqlite.EvidenceKey{moved}, now.Add(-time.Minute))
	if len(hits) != 0 {
		t.Fatalf("evidence leaked across incarnations: %+v", hits)
	}
	// A source config edit must not inherit the old PASS.
	edited := key
	edited.SourceConfigVersion = 5
	hits, _ = store.RecentEvidence(ctx, scopeHash, []sqlite.EvidenceKey{edited}, now.Add(-time.Minute))
	if len(hits) != 0 {
		t.Fatalf("evidence leaked across source versions: %+v", hits)
	}
	// A different scope (different judge/CA regime) must not hit.
	otherScope := domain.EvidenceScopeHash(domain.ValidationScope{
		Mode: "tls", TargetHostPort: "example.org:443", Profile: "target_tls_v1"})
	hits, _ = store.RecentEvidence(ctx, otherScope, []sqlite.EvidenceKey{key}, now.Add(-time.Minute))
	if len(hits) != 0 {
		t.Fatalf("evidence leaked across scopes: %+v", hits)
	}
	// The newest FAIL hides the older PASS inside the same key.
	fail := pass
	fail.Outcome = "FAIL"
	if err := store.PutEvidence(ctx, key, scopeHash, fail); err != nil {
		t.Fatal(err)
	}
	hits, _ = store.RecentEvidence(ctx, scopeHash, []sqlite.EvidenceKey{key}, now.Add(-time.Minute))
	if len(hits) != 0 {
		t.Fatalf("negative evidence must not be reusable: %+v", hits)
	}
	// A late-arriving PASS completed before the FAIL must be rejected, not
	// silently overwrite the newer negative result.
	stale := pass
	stale.StartedAt = now.Add(-3 * time.Second)
	stale.CompletedAt = now.Add(-2 * time.Second)
	if err := store.PutEvidence(ctx, key, scopeHash, stale); !errors.Is(err, sqlite.ErrStale) {
		t.Fatalf("stale pass must be rejected with ErrStale, got %v", err)
	}
	hits, _ = store.RecentEvidence(ctx, scopeHash, []sqlite.EvidenceKey{key}, now.Add(-time.Minute))
	if len(hits) != 0 {
		t.Fatalf("stale pass must not resurrect evidence: %+v", hits)
	}
	// Freshness follows ev.CompletedAt, never the caller's clock: a result
	// completed 40s ago stays invisible inside a 30s reuse window.
	if err := store.InvalidateEvidence(ctx, []string{"fp-ev"}); err != nil {
		t.Fatal(err)
	}
	old := pass
	old.StartedAt = now.Add(-41 * time.Second)
	old.CompletedAt = now.Add(-40 * time.Second)
	if err := store.PutEvidence(ctx, key, scopeHash, old); err != nil {
		t.Fatal(err)
	}
	hits, _ = store.RecentEvidence(ctx, scopeHash, []sqlite.EvidenceKey{key}, now.Add(-30*time.Second))
	if len(hits) != 0 {
		t.Fatalf("evidence freshness must follow ev.CompletedAt: %+v", hits)
	}
	fresh := pass
	fresh.StartedAt = now
	fresh.CompletedAt = now.Add(time.Second)
	if err := store.PutEvidence(ctx, key, scopeHash, fresh); err != nil {
		t.Fatal(err)
	}
	if err := store.InvalidateEvidence(ctx, []string{"fp-ev"}); err != nil {
		t.Fatal(err)
	}
	hits, _ = store.RecentEvidence(ctx, scopeHash, []sqlite.EvidenceKey{key}, now.Add(-time.Minute))
	if len(hits) != 0 {
		t.Fatalf("invalidation must clear evidence: %+v", hits)
	}
}

func TestCoreResourceLessEvidenceRequiresCurrentSource(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		name := "updated"
		if !enabled {
			name = "disabled"
		}
		t.Run(name, func(t *testing.T) {
			store := newCoreStore(t)
			ctx := context.Background()
			now := time.Now().UTC()
			source := domain.Source{
				ID: "core-resource-less-source", Tenant: "default",
				AdapterType: "provider_api", ParserType: "lines_v1",
				Enabled: true, ConfigJSON: "{}",
			}
			if err := store.UpsertSource(ctx, source, now); err != nil {
				t.Fatal(err)
			}
			source, err := store.GetSource(ctx, source.ID)
			if err != nil {
				t.Fatal(err)
			}
			proof := domain.Evidence{
				AttemptID: "core-resource-less-current", Outcome: "PASS",
				Profile: "https_basic_v1", StartedAt: now, CompletedAt: now,
			}
			key := sqlite.EvidenceKey{
				Fingerprint: "core-resource-less-fingerprint",
				SourceID:    source.ID, SourceConfigVersion: source.ConfigVersion,
				Incarnation: "ondemand_core",
			}
			if err := store.PutEvidenceProduction(ctx, key,
				"core-resource-less-current", proof); err != nil {
				t.Fatalf("current resource-less proof was rejected: %v", err)
			}

			source.Enabled = enabled
			if err := store.UpsertSource(ctx, source, now.Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			err = store.PutEvidenceProduction(ctx, key,
				"core-resource-less-obsolete", proof)
			if !errors.Is(err, sqlite.ErrStale) &&
				!errors.Is(err, sqlite.ErrNotEligible) {
				t.Fatalf("obsolete resource-less proof must fail eligibility, got %v", err)
			}
			var count int
			if err := store.DB().QueryRowContext(ctx, `
				SELECT COUNT(*) FROM validation_evidence WHERE scope_hash = ?`,
				"core-resource-less-obsolete").Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 0 {
				t.Fatalf("obsolete resource-less proof was cached: rows=%d", count)
			}
		})
	}
}
