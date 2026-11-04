package integration

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"proxypool/internal/application"
)

func TestAuditLateAuthFailureCannotDeleteReimportedIncarnation(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	arrived, release := make(chan struct{}), make(chan struct{})
	var entered, released sync.Once
	unblock := func() { released.Do(func() { close(release) }) }
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered.Do(func() { close(arrived) })
		select {
		case <-release:
			w.WriteHeader(http.StatusProxyAuthRequired)
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(proxy.Close)
	t.Cleanup(unblock)
	e.importProxy(t, "audit-incarnation", proxy.URL)
	old := firstCandidate(t, e, time.Now())
	done := make(chan struct{})
	go func() {
		_, _ = e.acquire.Acquire(context.Background(), application.AcquireRequest{
			Count: 1, ProjectID: application.DefaultAcquireProject,
			ConsumerID: application.DefaultAcquireProject, DeadlineMS: 3000,
		})
		close(done)
	}()
	select {
	case <-arrived:
	case <-time.After(time.Second):
		t.Fatal("old candidate never entered its probe")
	}
	if _, err := e.store.RetireResource(context.Background(), old.Resource.ID, "audit explicit deletion", time.Now()); err != nil {
		t.Fatal(err)
	}
	if result, derr := e.ingest.ImportLines(context.Background(), "audit-incarnation", proxy.URL+"\n"); derr != nil || result.Added != 1 {
		t.Fatalf("re-import: result=%+v err=%v", result, derr)
	}
	replacement := firstCandidate(t, e, time.Now())
	if replacement.Resource.ID != old.Resource.ID || replacement.Incarnation == old.Incarnation {
		t.Fatal("fixture did not recreate the same connection with a new incarnation")
	}
	unblock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("late probe did not finish")
	}
	stats, err := e.store.Stats(context.Background())
	if err != nil || stats.Resources != 1 || stats.EnabledOffers != 1 {
		t.Fatalf("a late old AUTH result deleted the recreated object: stats=%+v err=%v", stats, err)
	}
}
