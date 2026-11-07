package integration

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"proxypool/internal/application"
	"proxypool/internal/storage/sqlite"
)

func TestStaticPrewarmFeedsAcquireWithoutPersistentInventory(t *testing.T) {
	j := startJudge(t)
	proxy := startFakeProxy(t)
	e := newEnv(t, j, nil)
	e.prewarm.StaticSupply = e.ingest
	listBody := fmt.Sprintf("http://127.0.0.1:%d\n", portOf(proxy.URL))
	for i := 0; i < 3; i++ {
		bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusBadGateway)
		}))
		defer bad.Close()
		listBody = bad.URL + "\n" + listBody
	}
	list := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, listBody)
	}))
	defer list.Close()
	ctx := context.Background()
	if _, err := e.ingest.CreateSource(ctx, "prewarm-list", application.SourceConfig{Static: &application.StaticConfig{URL: list.URL}}, true); err != nil {
		t.Fatal(err)
	}
	job, err := e.prewarm.StartJob(ctx, sqlite.DefaultProjectID, application.PrewarmRequest{Count: 1, DeadlineMS: 3000, Filters: application.Filters{SourceIDs: []string{"prewarm-list"}}})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.After(4 * time.Second)
	for {
		current, err := e.store.GetJob(ctx, job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.State == sqlite.JobCompleted {
			if current.Ready != 1 || current.Probed != 4 || current.Failed != 3 {
				t.Fatalf("prewarm counts: %+v", current)
			}
			break
		}
		if current.State != sqlite.JobPending && current.State != sqlite.JobRunning {
			t.Fatalf("prewarm ended without supply: %+v", current)
		}
		select {
		case <-deadline:
			t.Fatal("prewarm did not finish")
		case <-time.After(10 * time.Millisecond):
		}
	}
	stats, err2 := e.store.Stats(ctx)
	rows, _, _ := e.ingest.CachedStaticCandidates("prewarm-list")
	if len(rows) != 4 || e.ingest.CacheView("prewarm-list").Blacklisted != 3 {
		t.Fatalf("failed network candidates were not blacklisted: %d cached", len(rows))
	}
	if err2 != nil || stats.Resources != 0 || stats.EnabledOffers != 0 || stats.ActiveLeases != 0 {
		t.Fatalf("prewarm persisted stock or lease: %+v %v", stats, err2)
	}
	var cds int
	if err2 := e.store.DB().QueryRow(`SELECT COUNT(*) FROM project_proxy_cd`).Scan(&cds); err2 != nil || cds != 0 {
		t.Fatalf("prewarm wrote CD: %d %v", cds, err2)
	}
	// A failed judge now distinguishes genuine proof reuse from a redundant
	// second probe through the same controlled proxy.
	j.healthy.Store(false)
	zero := int64(0)
	result, derr := e.acquire.Acquire(ctx, application.AcquireRequest{Count: 1, ProjectID: sqlite.DefaultProjectID, CDSeconds: &zero, Filters: application.Filters{SourceIDs: []string{"prewarm-list"}}})
	if derr != nil || result.Delivered != 1 || !result.Items[0].Validation.CacheHit {
		t.Fatalf("static prewarm was not reused: result=%+v err=%v", result, derr)
	}
	if ok, err := e.leases.Release(ctx, result.Items[0].LeaseID, "master", true); err != nil || !ok {
		t.Fatalf("release: %v %v", ok, err)
	}
	// The successful delivery consumes the candidate; release does not return
	// it to a network pool, even when the fetched list is still fresh.
	failed, derr := e.acquire.Acquire(ctx, application.AcquireRequest{Count: 1, ProjectID: sqlite.DefaultProjectID, CDSeconds: &zero, DeadlineMS: 2000, Filters: application.Filters{SourceIDs: []string{"prewarm-list"}}})
	if derr != nil || failed.Delivered != 0 {
		t.Fatalf("consumed network candidate was delivered twice: %+v %v", failed, derr)
	}
	stats, err2 = e.store.Stats(ctx)
	if err2 != nil || stats.Resources != 0 || stats.EnabledOffers != 0 {
		t.Fatalf("acquire leaked stock: %+v %v", stats, err2)
	}
}
