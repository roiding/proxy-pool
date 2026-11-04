package integration

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"proxypool/internal/application"
	"proxypool/internal/config"
	"proxypool/internal/storage/sqlite"
)

func auditBlockingProxy(t *testing.T) (*httptest.Server, <-chan struct{}, func()) {
	t.Helper()
	arrived, resume := make(chan struct{}), make(chan struct{})
	var entered, released sync.Once
	unblock := func() { released.Do(func() { close(resume) }) }
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered.Do(func() { close(arrived) })
		select {
		case <-resume:
			http.Error(w, "audit fixture released", http.StatusBadGateway)
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(unblock)
	return srv, arrived, unblock
}

func TestAuditStartingPrewarmDoesNotInterruptAnotherLiveJob(t *testing.T) {
	e := newEnv(t, startJudge(t), func(c *config.Config) {
		c.JobMaxActivePerProject = 2
		c.JobMaxActiveGlobal = 4
		c.ConnectTimeoutMS = 2000
		c.ProbeTotalTimeoutMS = 2500
	})
	proxy, arrived, _ := auditBlockingProxy(t)
	e.importProxy(t, "audit-jobs", proxy.URL)
	first, derr := e.prewarm.StartJob(context.Background(), application.DefaultAcquireProject,
		application.PrewarmRequest{Count: 1, DeadlineMS: 3000})
	if derr != nil {
		t.Fatal(derr)
	}
	t.Cleanup(func() { _ = e.prewarm.CancelJob(context.Background(), first.ID) })
	select {
	case <-arrived:
	case <-time.After(time.Second):
		t.Fatal("first job never entered its probe")
	}
	second, derr := e.prewarm.StartJob(context.Background(), application.DefaultAcquireProject,
		application.PrewarmRequest{Count: 1, DeadlineMS: 1000,
			Filters: application.Filters{SourceIDs: []string{"no-such-source"}}})
	if derr != nil {
		t.Fatal(derr)
	}
	t.Cleanup(func() { _ = e.prewarm.CancelJob(context.Background(), second.ID) })
	current, err := e.store.GetJob(context.Background(), first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != sqlite.JobRunning {
		t.Fatalf("a new job rewrote another live runner as a restart: state=%s reason=%s", current.State, current.StopReason)
	}
}

func TestAuditPrewarmTotalBudgetCancelsInFlightProbe(t *testing.T) {
	e := newEnv(t, startJudge(t), func(c *config.Config) {
		c.ConnectTimeoutMS = 1000
		c.ProbeTotalTimeoutMS = 2000
	})
	proxy, _, _ := auditBlockingProxy(t)
	e.importProxy(t, "audit-job-budget", proxy.URL)
	started := time.Now()
	job, derr := e.prewarm.StartJob(context.Background(), application.DefaultAcquireProject,
		application.PrewarmRequest{Count: 1, DeadlineMS: 150})
	if derr != nil {
		t.Fatal(derr)
	}
	t.Cleanup(func() { _ = e.prewarm.CancelJob(context.Background(), job.ID) })
	limit := time.NewTimer(750 * time.Millisecond)
	defer limit.Stop()
	for {
		current, err := e.store.GetJob(context.Background(), job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.State != sqlite.JobPending && current.State != sqlite.JobRunning {
			return
		}
		select {
		case <-limit.C:
			t.Fatalf("150ms job still running after %s: state=%s probed=%d", time.Since(started), current.State, current.Probed)
		case <-time.After(10 * time.Millisecond):
		}
	}
}
