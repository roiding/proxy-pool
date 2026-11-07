package sqlite

import (
	"errors"
	"testing"
	"time"

	"proxypool/internal/domain"
)

func TestSourceFetchWindowAndCompletionAreVersionOwned(t *testing.T) {
	s, ctx := newSchedulingStore(t)
	now := time.Now().UTC()
	src := createSchedulingSource(t, s, ctx, "fetch-owner", now)
	if err := s.ClaimSourceFetch(ctx, src.ID, src.ConfigVersion, now, time.Minute); err != nil {
		t.Fatal(err)
	}
	var limited *domain.Error
	err := s.ClaimSourceFetch(ctx, src.ID, src.ConfigVersion, now.Add(time.Second), time.Minute)
	if !errors.As(err, &limited) || limited.Code != "SOURCE_RATE_LIMITED" || limited.RetryAfter != 59*time.Second {
		t.Fatalf("durable rate window was lost: %v", err)
	}
	current := updateSchedulingSource(t, s, ctx, src, now.Add(2*time.Second))
	err = s.FinishSourceFetch(ctx, SourceFetchOutcome{SourceID: src.ID, ConfigVersion: src.ConfigVersion,
		StartedAt: now, CompletedAt: now.Add(3 * time.Second), NextFetchAt: now.Add(time.Minute), Fetched: 100})
	if !errors.Is(err, ErrStale) {
		t.Fatalf("stale completion wrote new source state: %v", err)
	}
	err = s.ClaimSourceFetch(ctx, src.ID, current.ConfigVersion, now.Add(4*time.Second), time.Minute)
	if !errors.As(err, &limited) || limited.Code != "SOURCE_RATE_LIMITED" {
		t.Fatalf("config edit reset real upstream cadence: %v", err)
	}
	start := now.Add(61 * time.Second)
	if err := s.ClaimSourceFetch(ctx, src.ID, current.ConfigVersion, start, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := s.FinishSourceFetch(ctx, SourceFetchOutcome{SourceID: src.ID, ConfigVersion: current.ConfigVersion,
		StartedAt: start, CompletedAt: start.Add(time.Second), NextFetchAt: start.Add(time.Minute), Fetched: 2, Retained: 2}); err != nil {
		t.Fatal(err)
	}
	state, err := s.SourceFetchState(ctx, src.ID)
	if err != nil || state.ConfigVersion != current.ConfigVersion || state.Fetched != 2 || state.ConsecutiveFailures != 0 {
		t.Fatalf("current completion was not persisted: %+v %v", state, err)
	}
}

func TestJobLimitsCoverRefreshAndPrewarmTogether(t *testing.T) {
	for _, mode := range []string{"project", "global"} {
		t.Run(mode, func(t *testing.T) {
			s, ctx := newSchedulingStore(t)
			now := time.Now().UTC()
			if err := s.EnsureProject(ctx, domain.Project{ID: "owner", Enabled: true}, now); err != nil {
				t.Fatal(err)
			}
			perProject, global := 2, 1
			if mode == "project" {
				perProject, global = 1, 4
			}
			if err := s.CreateJobWithLimits(ctx, domain.Job{ID: "warm", ProjectID: "owner", Kind: "prewarm"}, now, perProject, global); err != nil {
				t.Fatal(err)
			}
			err := s.CreateJobWithLimits(ctx, domain.Job{ID: "refresh", ProjectID: "owner", Kind: "source_refresh"}, now, perProject, global)
			var derr *domain.Error
			if !errors.As(err, &derr) || derr.Code != "SOURCE_FETCH_JOB_LIMIT" {
				t.Fatalf("changing kind bypassed %s job cap: %v", mode, err)
			}
		})
	}
}
