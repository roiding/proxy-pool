package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"proxypool/internal/application"
	"proxypool/internal/domain"
	"proxypool/internal/storage/sqlite"
)

func TestRefreshJobCancellationPrivacyAndRestart(t *testing.T) {
	e := newImportIdentityServices(t)
	ctx := context.Background()
	started, aborted := make(chan struct{}), make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
		close(aborted)
	}))
	t.Cleanup(upstream.Close)
	t.Cleanup(func() {
		stopCtx, stop := context.WithTimeout(ctx, time.Second)
		defer stop()
		_ = e.server.refresh.Shutdown(stopCtx)
	})
	if _, derr := e.ingest.CreateSource(ctx, "private-refresh", application.SourceConfig{
		Static: &application.StaticConfig{URL: upstream.URL},
		Access: &application.SourceAccessConfig{ProjectIDs: []string{"owner"}},
	}, true); derr != nil {
		t.Fatal(derr)
	}
	job, derr := e.server.refresh.StartJob(ctx, "private-refresh")
	if derr != nil {
		t.Fatal(derr)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh runner did not start bounded I/O")
	}
	read := func(admin bool) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/"+job.ID, nil)
		r.SetPathValue("id", job.ID)
		r = r.WithContext(withIdentity(r.Context(), authIdentity{Admin: admin, ProjectID: sqlite.DefaultProjectID}))
		rec := httptest.NewRecorder()
		e.server.handleGetJobV1(rec, r)
		return rec
	}
	if rec := read(false); rec.Code != http.StatusNotFound || strings.Contains(rec.Body.String(), "private-refresh") {
		t.Fatalf("default-project key could read admin source refresh job: %d %s", rec.Code, rec.Body.String())
	}
	if rec := read(true); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), application.SourceRefreshJobKind) {
		t.Fatalf("admin could not inspect refresh job: %d %s", rec.Code, rec.Body.String())
	}
	r := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/"+job.ID+"/cancel", nil)
	r.SetPathValue("id", job.ID)
	r = r.WithContext(withIdentity(r.Context(), authIdentity{ProjectID: sqlite.DefaultProjectID}))
	rec := httptest.NewRecorder()
	e.server.handleJobCancelV1(rec, r)
	if rec.Code != http.StatusNotFound || strings.Contains(rec.Body.String(), "private-refresh") {
		t.Fatalf("default-project key could discover/cancel admin refresh: %d %s", rec.Code, rec.Body.String())
	}
	if derr := e.server.refresh.CancelJob(ctx, job.ID); derr != nil {
		t.Fatal(derr)
	}
	select {
	case <-aborted:
	case <-time.After(2 * time.Second):
		t.Fatal("job cancellation did not abort its HTTP request")
	}
	current, err := e.store.GetJob(ctx, job.ID)
	if err != nil || current.State != sqlite.JobCancelled || current.Ready != 0 || current.Probed != 0 {
		t.Fatalf("refresh cancellation looked like validated supply: %+v %v", current, err)
	}
	// Startup recovery covers refresh jobs as well as prewarm. It does not
	// refetch a channel or replay a paid extraction on its own.
	if err := e.store.CreateJob(ctx, domain.Job{ID: "stale-refresh", ProjectID: sqlite.DefaultProjectID,
		Kind: application.SourceRefreshJobKind}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.RecoverInterruptedJobs(ctx, "service_restart", time.Now()); err != nil {
		t.Fatal(err)
	}
	stale, err := e.store.GetJob(ctx, "stale-refresh")
	if err != nil || stale.State != sqlite.JobInterrupted {
		t.Fatalf("restart left a nonexistent refresh runner active: %+v %v", stale, err)
	}
}
