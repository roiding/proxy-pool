package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"proxypool/internal/application"
	"proxypool/internal/storage/sqlite"
)

func TestFastChannelIsProbedWhileAnotherDownloadIsPending(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	e.prewarm.StaticSupply = e.ingest
	proxy := startFakeProxy(t)
	started, cancelled := make(chan struct{}), make(chan struct{})
	var startOnce, cancelOnce sync.Once
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startOnce.Do(func() { close(started) })
		<-r.Context().Done()
		cancelOnce.Do(func() { close(cancelled) })
	}))
	t.Cleanup(slow.Close)
	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-started:
			fmt.Fprintln(w, proxy.URL)
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(fast.Close)
	for id, url := range map[string]string{"fast": fast.URL, "slow": slow.URL} {
		if _, err := e.ingest.CreateSource(context.Background(), id, application.SourceConfig{
			Static: &application.StaticConfig{URL: url},
		}, true); err != nil {
			t.Fatal(err)
		}
	}
	job, err := e.prewarm.StartJob(context.Background(), sqlite.DefaultProjectID,
		application.PrewarmRequest{Count: 1, DeadlineMS: 3000})
	if err != nil {
		t.Fatal(err)
	}
	job = waitLifecycleJob(t, e, job.ID)
	if job.State != sqlite.JobCompleted || job.Ready != 1 || job.Probed != 1 {
		t.Fatalf("slow download blocked a ready channel: %+v", job)
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("completed job left an unnecessary download running")
	}
}

func TestAutomaticPoolIncludesProviderAndRefillsAcrossEnabledChannels(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, startJudge(t), nil)
	e.prewarm.StaticSupply = e.ingest
	a, b, c := startFakeProxy(t), startFakeProxy(t), startFakeProxy(t)
	var fetches atomic.Int32
	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fetches.Add(1)
		if r.URL.Query().Get("count") != "20" {
			t.Errorf("configured batch size was replaced by the live deficit: %s", r.URL.RawQuery)
		}
		json.NewEncoder(w).Encode(map[string]any{"code": 200, "data": map[string]any{
			"proxies": []string{fmt.Sprintf("127.0.0.1:%d", portOf(a.URL)), fmt.Sprintf("127.0.0.1:%d", portOf(b.URL))}, "count": 2,
		}})
	}))
	t.Cleanup(vendor.Close)
	e.ingest.ScdnEndpoint, e.ingest.ScdnHTTPClient = vendor.URL, vendor.Client()
	if _, err := e.ingest.CreateSource(ctx, "provider", application.SourceConfig{
		Provider: &application.ProviderConfig{Protocol: "http", FetchCount: 20},
	}, true); err != nil {
		t.Fatal(err)
	}
	lifecycleList(t, e, "list", c.URL+"\n")
	p, err := e.store.GetProject(ctx, sqlite.DefaultProjectID)
	if err != nil {
		t.Fatal(err)
	}
	p.Defaults.AutoPrewarmTarget = 3
	if _, err := e.store.UpdateProject(ctx, p, p.ConfigVersion, time.Now()); err != nil {
		t.Fatal(err)
	}
	maint := application.NewPoolMaintenanceService(e.store, e.prewarm)
	clock := time.Now()
	maint.Now = func() time.Time { return clock }
	if err := maint.RunDue(ctx); err != nil {
		t.Fatal(err)
	}
	jobs, _ := e.store.ListJobs(ctx, p.ID, 10)
	if len(jobs) != 1 {
		t.Fatalf("automatic gap did not create exactly one task: %+v", jobs)
	}
	first := waitLifecycleJob(t, e, jobs[0].ID)
	if !first.Automatic || first.State != sqlite.JobCompleted || first.Ready != 3 || first.Probed != 3 || fetches.Load() != 1 {
		t.Fatalf("enabled provider did not join the pool: %+v fetches=%d", first, fetches.Load())
	}
	clock = clock.Add(31 * time.Second)
	if err := maint.RunDue(ctx); err != nil {
		t.Fatal(err)
	}
	jobs, _ = e.store.ListJobs(ctx, p.ID, 10)
	if len(jobs) != 1 {
		t.Fatal("already full pool created another prewarm task")
	}
	zero := int64(0)
	result, derr := e.acquire.Acquire(ctx, application.AcquireRequest{Count: 1, ProjectID: p.ID, CDSeconds: &zero,
		Filters: application.Filters{SourceIDs: []string{"provider"}}})
	if derr != nil || result.Delivered != 1 || !result.Items[0].Validation.CacheHit || e.ingest.CacheView("provider").Blacklisted != 1 {
		t.Fatalf("provider cached delivery did not enter shared blacklist lifecycle: %+v %v", result, derr)
	}
	lifecycleList(t, e, "newly-enabled", startFakeProxy(t).URL+"\n")
	clock = clock.Add(31 * time.Second)
	if err := maint.RunDue(ctx); err != nil {
		t.Fatal(err)
	}
	jobs, _ = e.store.ListJobs(ctx, p.ID, 10)
	if len(jobs) != 2 {
		t.Fatalf("delivery gap was not replenished: %+v", jobs)
	}
	second := waitLifecycleJob(t, e, jobs[0].ID)
	if second.Ready != 3 || second.Probed != 1 || second.Failed != 0 || fetches.Load() != 1 {
		t.Fatalf("replenishment retried used candidates or ignored enabled supply: %+v fetches=%d", second, fetches.Load())
	}
}
