package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"proxypool/internal/domain"
	"proxypool/internal/storage/sqlite"
	"proxypool/internal/validation"
)

func TestPrewarmEvidenceRejectsChangedSourceVersion(t *testing.T) {
	svc := newTestIngest(t)
	ctx := context.Background()
	src, derr := svc.CreateSource(ctx, "prewarm-stale-source", SourceConfig{
		MaxPoolLeases: 1,
	}, true)
	if derr != nil {
		t.Fatal(derr)
	}
	if _, derr = svc.ImportLines(ctx, src.ID,
		"http://203.0.113.10:8080\n"); derr != nil {
		t.Fatal(derr)
	}
	candidates, err := svc.store.SelectPeriodicCandidates(ctx, sqlite.PeriodicFilters{
		Profile: validation.ProfileHTTPSBasicV1,
		Now:     time.Now(),
		Limit:   1,
	})
	if err != nil || len(candidates) != 1 {
		t.Fatalf("select candidate: candidates=%d err=%v", len(candidates), err)
	}
	cand := candidates[0]
	if _, derr = svc.UpdateSource(ctx, src.ID, SourceConfig{
		MaxPoolLeases: 2,
	}, true, src.ConfigVersion); derr != nil {
		t.Fatal(derr)
	}

	prewarm := &PrewarmService{store: svc.store}
	err = prewarm.putCandidateEvidence(ctx, cand, "scope-after-source-change",
		domain.Evidence{
			Outcome:     "PASS",
			CompletedAt: time.Now(),
		})
	if !errors.Is(err, sqlite.ErrStale) {
		t.Fatalf("stale prewarm evidence write must be rejected, got %v", err)
	}
}
