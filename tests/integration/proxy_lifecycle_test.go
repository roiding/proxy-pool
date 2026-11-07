package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"proxypool/internal/application"
	"proxypool/internal/domain"
	"proxypool/internal/storage/sqlite"
	"proxypool/internal/validation"
)

func lifecycleScope(e *env, j *judge) (domain.ValidationScope, validation.ProbeOptions) {
	digest := sha256.Sum256([]byte(e.cfg.JudgeCAPem))
	opts := validation.ProbeOptions{JudgeURL: e.cfg.JudgeURL, Limits: validation.ProbeLimits{
		DialTimeout: e.cfg.ConnectTimeout(), ProbeTotal: e.cfg.ProbeTotalTimeout(),
		AllowPrivate: true, RootCAs: j.certPool(), CAFingerprint: hex.EncodeToString(digest[:]),
		JudgeSecret: e.cfg.JudgeSecret,
	}}
	return domain.ValidationScope{Mode: "basic", Profile: validation.ProfileHTTPSBasicV1,
		SecurityHash: validation.ProbeSecurityHash(opts)}, opts
}

func lifecycleList(t *testing.T, e *env, id, body string) []domain.Candidate {
	t.Helper()
	list := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, body)
	}))
	t.Cleanup(list.Close)
	if _, err := e.ingest.CreateSource(context.Background(), id, application.SourceConfig{
		MaxPoolLeases: 20, Static: &application.StaticConfig{URL: list.URL},
		Refresh: &application.RefreshConfig{Mode: "demand", CacheTTLMS: 1000},
	}, true); err != nil {
		t.Fatal(err)
	}
	res, err := e.ingest.RefreshStaticSource(context.Background(), id)
	if err != nil || res.ExpiresAt == nil {
		t.Fatalf("controlled list failed: %+v %v", res, err)
	}
	view := e.ingest.CacheView(id)
	if view.ExpiresAt.Sub(*view.FetchedAt) != 30*time.Minute {
		t.Fatal("legacy source TTL changed the network cache lifetime")
	}
	rows, _, _ := e.ingest.CachedStaticCandidates(id)
	return rows
}

func waitLifecycleJob(t *testing.T, e *env, id string) domain.Job {
	t.Helper()
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		job, err := e.store.GetJob(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if job.State != sqlite.JobPending && job.State != sqlite.JobRunning {
			return job
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("controlled prewarm did not finish")
	return domain.Job{}
}

func TestNetworkVerifiedOnceIsConsumedExactlyOnceAcrossClientsAndChannels(t *testing.T) {
	j := startJudge(t)
	e := newEnv(t, j, nil)
	proxy := startFakeProxy(t)
	rows := lifecycleList(t, e, "network-one", proxy.URL+"\n")
	lifecycleList(t, e, "network-duplicate", proxy.URL+"\n")
	// A real controlled probe; shifting both timestamps models an old PASS
	// without sleeping and does not claim any vendor/network acceptance.
	scope, opts := lifecycleScope(e, j)
	ev, fail := validation.Probe(context.Background(), scope, rows[0].Resource.Conn, opts, "fixture")
	if fail != nil {
		t.Fatal(fail)
	}
	ev.StartedAt, ev.CompletedAt = ev.StartedAt.Add(-time.Minute), ev.CompletedAt.Add(-time.Minute)
	if err := e.ingest.RecordStaticEvidence(context.Background(), rows[0], domain.EvidenceScopeHash(scope), ev, nil); err != nil {
		t.Fatal(err)
	}
	j.healthy.Store(false) // Any redundant probe now fails.
	zero := int64(0)
	var wg sync.WaitGroup
	var delivered atomic.Int32
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := e.acquire.Acquire(context.Background(), application.AcquireRequest{
				Count: 1, ProjectID: sqlite.DefaultProjectID, CDSeconds: &zero,
				Filters: application.Filters{SourceIDs: []string{"network-one"}},
			})
			if err != nil && err.Code != "ACQUIRE_CAPACITY_EXHAUSTED" {
				t.Errorf("concurrent acquire failed: %v", err)
			}
			delivered.Add(int32(result.Delivered))
			if result.Delivered > 0 && !result.Items[0].Validation.CacheHit {
				t.Error("minute-old PASS was not reused")
			}
		}()
	}
	wg.Wait()
	if delivered.Load() != 1 {
		t.Fatalf("one cached connection was delivered %d times", delivered.Load())
	}
	for _, id := range []string{"network-one", "network-duplicate"} {
		rows, _, _ := e.ingest.CachedStaticCandidates(id)
		if len(rows) != 1 || e.ingest.CacheView(id).Proofs != 0 {
			t.Fatalf("cached row was deleted or served PASS remained in %s", id)
		}
		blocked, err := e.store.SourceProxyBlocked(context.Background(), id, rows[0].Resource.ID, time.Now())
		if err != nil || blocked != (id == "network-one") {
			t.Fatalf("blacklist was missing or leaked across channels: %s blocked=%v err=%v", id, blocked, err)
		}
	}

}

func TestUploadedVerifiedOnceSurvivesDeliveryAndClockButRealFailureDeletes(t *testing.T) {
	j := startJudge(t)
	e := newEnv(t, j, nil)
	proxy := startFakeProxy(t)
	e.importProxy(t, "my-upload", proxy.URL)
	ctx := context.Background()
	rows, err := e.store.SelectPeriodicCandidates(ctx, sqlite.PeriodicFilters{SourceID: "my-upload", Now: time.Now(), Limit: 1})
	if err != nil || len(rows) != 1 {
		t.Fatal(err)
	}
	c := rows[0]
	scope, opts := lifecycleScope(e, j)
	ev, fail := validation.Probe(ctx, scope, c.Resource.Conn, opts, "fixture")
	if fail != nil {
		t.Fatal(fail)
	}
	ev.StartedAt, ev.CompletedAt = ev.StartedAt.Add(-24*time.Hour), ev.CompletedAt.Add(-24*time.Hour)
	if err := e.store.PutEvidenceProduction(ctx, sqlite.EvidenceKey{ResourceID: c.Resource.ID,
		Fingerprint: e.acquire.FingerprintOf(c.Resource.Conn), SourceID: c.Offer.SourceID,
		SourceConfigVersion: c.SourceConfigVersion, Incarnation: c.Incarnation}, domain.EvidenceScopeHash(scope), ev); err != nil {
		t.Fatal(err)
	}
	j.healthy.Store(false)
	zero := int64(0)
	for i := 0; i < 2; i++ {
		result, err := e.acquire.Acquire(ctx, application.AcquireRequest{Count: 1,
			ProjectID: sqlite.DefaultProjectID, CDSeconds: &zero,
			Filters: application.Filters{SourceIDs: []string{"my-upload"}}})
		if err != nil || result.Delivered != 1 || !result.Items[0].Validation.CacheHit || result.Items[0].Validation.ValidUntil != nil {
			t.Fatalf("uploaded one-day-old PASS was lost: %+v %v", result, err)
		}
		if _, err := e.leases.Release(ctx, result.Items[0].LeaseID, "master", true); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.ingest.PurgeExpiredStaticCache(ctx); err != nil {
		t.Fatal(err)
	}
	stats, _ := e.store.Stats(ctx)
	if stats.Resources != 1 || stats.EnabledOffers != 1 {
		t.Fatal("delivery/network purge deleted uploaded inventory")
	}
	// Explicit revalidation actually fails: this is the only deletion path.
	if _, _, err := e.nightly.RunSourceOnce(ctx, "my-upload"); err != nil {
		t.Fatal(err)
	}
	stats, _ = e.store.Stats(ctx)
	if stats.Resources != 0 || stats.EnabledOffers != 0 {
		t.Fatal("actual failed upload was not deleted")
	}
}

func TestPrewarmProbesMultipleChannelsConcurrentlyAndCountsUniqueTotal(t *testing.T) {
	j := startJudge(t)
	e := newEnv(t, j, nil)
	e.prewarm.StaticSupply = e.ingest
	gate := make(chan struct{})
	var active, peak atomic.Int32
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(gate) }) })
	for i := 0; i < 3; i++ {
		inner := startFakeProxy(t)
		proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			n := active.Add(1)
			defer active.Add(-1)
			for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
			}
			if n >= 3 {
				once.Do(func() { close(gate) })
			}
			select {
			case <-gate:
				inner.Config.Handler.ServeHTTP(w, r)
			case <-r.Context().Done():
			}
		}))
		t.Cleanup(proxy.Close)
		lifecycleList(t, e, fmt.Sprintf("parallel-%d", i), proxy.URL+"\n")
	}
	job, err := e.prewarm.StartJob(context.Background(), sqlite.DefaultProjectID,
		application.PrewarmRequest{Count: 3, DeadlineMS: 5000})
	if err != nil {
		t.Fatal(err)
	}
	job = waitLifecycleJob(t, e, job.ID)
	if job.State != sqlite.JobCompleted || job.Ready != 3 || job.Probed != 3 || job.Failed != 0 || peak.Load() != 3 {
		t.Fatalf("channels were not concurrently aggregated: %+v peak=%d", job, peak.Load())
	}
}

func TestFailedNetworkIsNotFetchedOrProbedAgainInsideCooldown(t *testing.T) {
	j := startJudge(t)
	e := newEnv(t, j, nil)
	e.prewarm.StaticSupply = e.ingest
	var probes, downloads atomic.Int32
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { probes.Add(1); w.WriteHeader(http.StatusBadGateway) }))
	t.Cleanup(bad.Close)
	list := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { downloads.Add(1); _, _ = fmt.Fprintln(w, bad.URL) }))
	t.Cleanup(list.Close)
	if _, err := e.ingest.CreateSource(context.Background(), "bad-network", application.SourceConfig{Static: &application.StaticConfig{URL: list.URL}}, true); err != nil {
		t.Fatal(err)
	}
	for run := 0; run < 2; run++ {
		job, err := e.prewarm.StartJob(context.Background(), sqlite.DefaultProjectID, application.PrewarmRequest{Count: 1, DeadlineMS: 3000, Filters: application.Filters{SourceIDs: []string{"bad-network"}}})
		if err != nil {
			t.Fatal(err)
		}
		job = waitLifecycleJob(t, e, job.ID)
		want := 1
		if run == 1 {
			want = 0
		}
		if job.Probed != want || job.Ready != 0 {
			t.Fatalf("known bad proxy was retried: %+v", job)
		}
		if _, err := e.ingest.RefreshStaticSource(context.Background(), "bad-network"); err != nil {
			t.Fatal(err)
		}
	}
	if downloads.Load() != 1 || probes.Load() != 1 || e.ingest.CacheView("bad-network").Blacklisted != 1 {
		t.Fatalf("duplicate fetch/probe: downloads=%d probes=%d", downloads.Load(), probes.Load())
	}
}
