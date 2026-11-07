package application

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"proxypool/internal/config"
	"proxypool/internal/domain"
	"proxypool/internal/storage/sqlite"
	"proxypool/internal/validation"
)

func TestPrivateListProofIsNotAnotherProjectsReadySupply(t *testing.T) {
	s := newTestIngest(t)
	ctx := context.Background()
	for _, id := range []string{"owner", "outsider"} {
		if err := s.store.EnsureProject(ctx, domain.Project{ID: id, Enabled: true}, s.Now()); err != nil {
			t.Fatal(err)
		}
	}
	var hits atomic.Int64
	list := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = fmt.Fprint(w, "http://127.0.0.1:3128\n")
	}))
	defer list.Close()
	if _, derr := s.CreateSource(ctx, "private-ready", SourceConfig{Static: &StaticConfig{URL: list.URL},
		Access: &SourceAccessConfig{ProjectIDs: []string{"owner"}}}, true); derr != nil {
		t.Fatal(derr)
	}
	if _, derr := s.RefreshStaticSource(ctx, "private-ready"); derr != nil {
		t.Fatal(derr)
	}
	rows, _, fresh := s.CachedStaticCandidates("private-ready")
	if !fresh || len(rows) != 1 {
		t.Fatal("missing controlled list fixture")
	}
	scope := domain.ValidationScope{Mode: "basic", Profile: "https_basic_v1", SecurityHash: "synthetic-fixture"}
	hash := domain.EvidenceScopeHash(scope)
	if err := s.RecordStaticEvidence(ctx, rows[0], hash,
		domain.Evidence{Outcome: "PASS", CompletedAt: s.Now().Add(-time.Second)}, nil); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.IdentitySecret = s.secret
	p := NewPrewarmService(s.store, cfg, "", nil, make(chan struct{}, 4), nil)
	p.StaticSupply = s
	params := prewarmParams{Validation: scope}
	effective := prewarmScope{SourceUnrestricted: true, Validation: scope, ScopeHash: hash}
	ownerReady, err := p.currentReady(ctx, "owner", params, effective, hash)
	if err != nil || ownerReady != 1 {
		t.Fatalf("owner could not see its scoped proof: ready=%d err=%v", ownerReady, err)
	}
	outsiderReady, err := p.currentReady(ctx, "outsider", params, effective, hash)
	if err != nil || outsiderReady != 0 || hits.Load() != 1 {
		t.Fatalf("private proof leaked into another project's Ready or triggered I/O: ready=%d hits=%d err=%v", outsiderReady, hits.Load(), err)
	}
}

func TestPrewarmBackgroundAdmissionIsSharedAndCancellable(t *testing.T) {
	s := newTestIngest(t)
	slots := make(chan struct{}, 4)
	p := NewPrewarmService(s.store, config.Default(), "", nil, slots, nil)
	if cap(p.backgroundSlots) != 3 {
		t.Fatal("prewarm must share three background slots and leave one online slot")
	}
	for i := 0; i < cap(p.backgroundSlots); i++ {
		p.backgroundSlots <- struct{}{}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	start := time.Now()
	attempted, pass := p.probeOne(ctx, "never-started", domain.ValidationScope{}, "scope", domain.Candidate{})
	<-p.backgroundSlots
	if attempted || pass || len(slots) != 0 || time.Since(start) > time.Second {
		t.Fatalf("blocked prewarm did not honor admission/budget: attempted=%v pass=%v slots=%d", attempted, pass, len(slots))
	}
}

func TestListReadyProofSurvivesOtherProxyFailures(t *testing.T) {
	s := newTestIngest(t)
	ctx := context.Background()
	list := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintln(w, "http://127.0.0.1:3128")
	}))
	defer list.Close()
	src, derr := s.CreateSource(ctx, "mixed-quality", SourceConfig{Static: &StaticConfig{URL: list.URL}}, true)
	if derr != nil {
		t.Fatal(derr)
	}
	if _, derr := s.RefreshStaticSource(ctx, src.ID); derr != nil {
		t.Fatal(derr)
	}
	rows, _, _ := s.CachedStaticCandidates(src.ID)
	scope := domain.ValidationScope{Mode: "basic", Profile: validation.ProfileHTTPSBasicV1, SecurityHash: "fixture"}
	hash := domain.EvidenceScopeHash(scope)
	if err := s.RecordStaticEvidence(ctx, rows[0], hash, domain.Evidence{Outcome: "PASS", CompletedAt: s.Now()}, nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := s.store.RecordSourceOutcomeOwned(ctx, src.ID, src.ConfigVersion, "FAIL", s.Now(), sqlite.SourceCircuitPolicy{FailureThreshold: 3, Base: time.Minute}); err != nil {
			t.Fatal(err)
		}
	}
	cfg := config.Default()
	cfg.IdentitySecret = s.secret
	p := NewPrewarmService(s.store, cfg, "", nil, make(chan struct{}, 4), nil)
	p.StaticSupply = s
	ready, err := p.currentReady(ctx, DefaultAcquireProject, prewarmParams{Validation: scope}, prewarmScope{SourceUnrestricted: true}, hash)
	if err != nil || ready != 1 {
		t.Fatalf("three unrelated failures hid a fresh list PASS: ready=%d err=%v", ready, err)
	}
	res, err := s.store.ReserveSlot(ctx, rows[0], sqlite.ReserveArgs{Owner: "regression", ProjectID: DefaultAcquireProject,
		IdentitySecret: s.secret, Profile: scope.Profile, Now: s.Now(), ReservationDeadline: s.Now().Add(time.Second)})
	if err != nil {
		t.Fatalf("fresh list PASS could not be reserved: %v", err)
	}
	_ = s.store.ReleaseReservation(ctx, res.ReservationID, s.Now())
}

func TestPrewarmSettlesPendingJobWhenContextEndsBeforeStart(t *testing.T) {
	for _, timedOut := range []bool{false, true} {
		t.Run(fmt.Sprintf("deadline=%v", timedOut), func(t *testing.T) {
			s := newTestIngest(t)
			p := NewPrewarmService(s.store, config.Default(), "", nil, make(chan struct{}, 4), nil)
			job := domain.Job{ID: "never-started", ProjectID: DefaultAcquireProject, Kind: PrewarmJobKind}
			if err := s.store.CreateJob(context.Background(), job, p.Now()); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			wantState, wantReason := sqlite.JobCancelled, sqlite.JobStopCanceled
			if timedOut {
				cancel()
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				wantState, wantReason = sqlite.JobPartial, "deadline"
			}
			cancel()
			p.execute(ctx, job.ID, job.ProjectID, prewarmParams{DeadlineMS: 1000})
			got, err := s.store.GetJob(context.Background(), job.ID)
			if err != nil || got.State != wantState || got.StopReason != wantReason || got.FinishedAt == nil {
				t.Fatalf("queued job was left active: %+v %v", got, err)
			}
		})
	}
}

func TestPrewarmCancellationReleasesReservationImmediately(t *testing.T) {
	s := newTestIngest(t)
	started := make(chan struct{})
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	}))
	t.Cleanup(proxy.Close)
	list := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintln(w, proxy.URL)
	}))
	t.Cleanup(list.Close)
	// Proxy rows require an explicit port. httptest supplies one in its URL.
	u, err := url.Parse(proxy.URL)
	if err != nil {
		t.Fatal(err)
	}
	port, _ := strconv.Atoi(u.Port())
	if port == 0 {
		t.Fatal("controlled proxy has no port")
	}
	ctx := context.Background()
	src, derr := s.CreateSource(ctx, "cancel-prewarm-list", SourceConfig{Static: &StaticConfig{URL: list.URL}}, true)
	if derr != nil {
		t.Fatal(derr)
	}
	if _, derr := s.RefreshStaticSource(ctx, src.ID); derr != nil {
		t.Fatal(derr)
	}
	rows, _, fresh := s.CachedStaticCandidates(src.ID)
	if !fresh || len(rows) != 1 {
		t.Fatal("missing controlled candidate")
	}
	cfg := config.Default()
	cfg.IdentitySecret, cfg.JudgeSecret, cfg.AllowPrivateAddresses = s.secret, "controlled-only-judge-secret", true
	p := NewPrewarmService(s.store, cfg, "http://judge.invalid/v1/judge", nil, make(chan struct{}, 4), nil)
	p.StaticSupply = s
	job := domain.Job{ID: "cancel-live-probe", ProjectID: DefaultAcquireProject, Kind: PrewarmJobKind}
	if err := s.store.CreateJob(ctx, job, p.Now()); err != nil {
		t.Fatal(err)
	}
	probeCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	done := make(chan bool, 1)
	go func() {
		scope := domain.ValidationScope{Mode: "basic", Profile: validation.ProfileHTTPSBasicV1}
		attempted, _ := p.probeOne(probeCtx, job.ID, scope, domain.EvidenceScopeHash(scope), rows[0])
		done <- attempted
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("controlled probe did not start")
	}
	cancel()
	select {
	case attempted := <-done:
		if attempted {
			t.Fatal("request cancellation was recorded as a candidate failure")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled probe did not finish")
	}
	reservation, err := s.store.ReserveSlot(ctx, rows[0], sqlite.ReserveArgs{
		Owner: "after-cancel", ProjectID: job.ProjectID, IdentitySecret: s.secret,
		Profile: validation.ProfileHTTPSBasicV1, Now: p.Now(), ReservationDeadline: p.Now().Add(time.Second),
	})
	if err != nil {
		t.Fatalf("cancelled probe left a reservation until its TTL: %v", err)
	}
	if err := s.store.ReleaseReservation(ctx, reservation.ReservationID, p.Now()); err != nil {
		t.Fatal(err)
	}
}
