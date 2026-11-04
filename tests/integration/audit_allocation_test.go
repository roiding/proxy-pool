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

	sqlite3 "github.com/mattn/go-sqlite3"

	"proxypool/internal/application"
	"proxypool/internal/config"
	"proxypool/internal/domain"
	"proxypool/internal/storage/sqlite"
	"proxypool/internal/validation"
)

func TestAuditCooldownCannotBeShortenedAcrossSourcesOrAddressAliases(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	proxy := startFakeProxy(t)
	e.importProxy(t, "audit-first", proxy.URL)
	e.importProxy(t, "audit-alias", fmt.Sprintf("http://[::ffff:127.0.0.1]:%d", portOf(proxy.URL)))
	k := newKeyAPI(t, e)
	t.Cleanup(k.srv.Close)
	token := k.createKey(t, "audit-cd-identity", 0)
	status, data := auditPost(t, k.srv, token, "/api/v1/proxies/acquire",
		`{"count":1,"cd":600,"source_ids":["audit-first"]}`)
	var first application.AcquireResult
	if status != http.StatusOK || json.Unmarshal(data, &first) != nil || first.Delivered != 1 {
		t.Fatalf("initial allocation: status=%d body=%s", status, data)
	}
	// Remove the capacity lock independently of the downstream ownership
	// handler, so a handler regression cannot mask the fixed-CD checks.
	status, data = k.do(t, http.MethodPost, "/api/v1/leases/"+first.Items[0].LeaseID+"/release", e.cfg.APIToken, "")
	if status != http.StatusOK {
		t.Fatalf("release: status=%d body=%s", status, data)
	}
	for _, cd := range []int{0, 1} {
		status, data = auditPost(t, k.srv, token, "/api/v1/proxies/acquire",
			fmt.Sprintf(`{"count":1,"cd":%d,"source_ids":["audit-alias"]}`, cd))
		var blocked application.AcquireResult
		if status != http.StatusOK || json.Unmarshal(data, &blocked) != nil || blocked.Delivered != 0 {
			t.Fatalf("CD bypass via source/address/cd=%d: status=%d body=%s", cd, status, data)
		}
	}
}

func TestAuditOwningKeyCanReleaseItsDeliveredLease(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	e.importProxy(t, "audit-release-owner", startFakeProxy(t).URL)
	srv := auditServer(t, e)
	project := auditProject(t, srv, e.cfg.APIToken, "release-owner-project", "")
	_, ownerToken := auditProjectKey(t, srv, e.cfg.APIToken, project.ID, "owner")
	_, otherToken := auditProjectKey(t, srv, e.cfg.APIToken, project.ID, "other")
	status, data := auditPost(t, srv, ownerToken, "/api/v1/proxies/acquire", `{"count":1,"cd":600}`)
	var first application.AcquireResult
	if status != http.StatusOK || json.Unmarshal(data, &first) != nil || first.Delivered != 1 {
		t.Fatalf("initial delivery: status=%d body=%s", status, data)
	}
	url := "/api/v1/leases/" + first.Items[0].LeaseID + "/release"
	status, data = auditPost(t, srv, otherToken, url, "")
	if status != http.StatusNotFound {
		t.Fatalf("another key changed the owner's lease: status=%d body=%s", status, data)
	}
	status, data = auditPost(t, srv, ownerToken, url, "")
	if status != http.StatusOK {
		t.Fatalf("authenticated owner cannot release its lease: status=%d body=%s", status, data)
	}
	status, data = auditPost(t, srv, otherToken, "/api/v1/proxies/acquire", `{"count":1,"cd":0}`)
	var next application.AcquireResult
	if status != http.StatusOK || json.Unmarshal(data, &next) != nil || next.Delivered != 0 {
		t.Fatalf("release cleared the project cooldown: status=%d body=%s", status, data)
	}
}

func TestAuditDifferentConnectionsWithSameObservedExitRemainDistinct(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	firstProxy, secondProxy := startFakeProxy(t), startFakeProxy(t)
	if err := e.ingest.EnsureSource(context.Background(), "audit-same-exit", application.SourceConfig{
		DefaultTransport: domain.TransportHTTP, MaxPoolLeases: 1,
	}, true); err != nil {
		t.Fatal(err)
	}
	if _, derr := e.ingest.ImportLines(context.Background(), "audit-same-exit", firstProxy.URL+"\n"+secondProxy.URL+"\n"); derr != nil {
		t.Fatal(derr)
	}
	srv := auditServer(t, e)
	status, data := auditPost(t, srv, e.cfg.APIToken, "/api/v1/proxies/acquire", `{"count":2,"cd":600}`)
	var result application.AcquireResult
	if status != http.StatusOK || json.Unmarshal(data, &result) != nil || result.Delivered != 2 || len(result.Items) != 2 {
		t.Fatalf("connection identity was incorrectly collapsed by exit IP: status=%d body=%s", status, data)
	}
	if result.Items[0].Conn.Port == result.Items[1].Conn.Port {
		t.Fatalf("request delivered one connection twice: %s", data)
	}
	if result.Items[0].Validation.ObservedExitIP != result.Items[1].Validation.ObservedExitIP {
		t.Fatalf("fixture must share its observed exit: %s", data)
	}
}

func TestAuditDifferentCredentialsAtSameEndpointRemainDistinct(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	proxy := startFakeProxy(t)
	if err := e.ingest.EnsureSource(context.Background(), "audit-credential-identities", application.SourceConfig{
		DefaultTransport: domain.TransportHTTP, MaxPoolLeases: 1,
	}, true); err != nil {
		t.Fatal(err)
	}
	lines := fmt.Sprintf("http://audit-a:fake-password-a@127.0.0.1:%d\nhttp://audit-b:fake-password-b@127.0.0.1:%d\n",
		portOf(proxy.URL), portOf(proxy.URL))
	imported, err := e.ingest.ImportLines(context.Background(), "audit-credential-identities", lines)
	if err != nil || imported.Added != 2 {
		t.Fatalf("distinct credential import: result=%+v err=%v", imported, err)
	}
	cd := int64(600)
	result, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{
		Count: 2, ProjectID: application.DefaultAcquireProject, ConsumerID: application.DefaultAcquireProject,
		CDSeconds: &cd, DeadlineMS: 3000,
	})
	if derr != nil || result.Delivered != 2 || len(result.Items) != 2 {
		t.Fatalf("response dedup merged different real connection credentials: result=%+v err=%v", result, derr)
	}
	first := e.acquire.FingerprintOf(result.Items[0].Conn)
	second := e.acquire.FingerprintOf(result.Items[1].Conn)
	if first == second {
		t.Fatal("different upstream credentials share a connection fingerprint")
	}
}

func TestAuditOnDemandSourceDisabledDuringProbeCannotCommit(t *testing.T) {
	j := startJudge(t)
	e := newEnv(t, j, nil)
	inner := startFakeProxy(t)
	arrived, resume := make(chan struct{}), make(chan struct{})
	var started, released sync.Once
	unblock := func() { released.Do(func() { close(resume) }) }
	blockedProxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started.Do(func() { close(arrived) })
		<-resume
		inner.Config.Handler.ServeHTTP(w, r)
	}))
	t.Cleanup(blockedProxy.Close)
	t.Cleanup(unblock)
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 200,
			"data": map[string]any{"count": 1, "proxies": []string{fmt.Sprintf("127.0.0.1:%d", portOf(blockedProxy.URL))}},
		})
	}))
	t.Cleanup(provider.Close)
	e.ingest.ScdnEndpoint = provider.URL
	e.ingest.ScdnHTTPClient = provider.Client()
	sourceCfg := application.SourceConfig{MaxPoolLeases: 1, Provider: &application.ProviderConfig{
		Protocol: "https", FetchCount: 1,
	}}
	if err := e.ingest.EnsureSource(context.Background(), "audit-api", sourceCfg, true); err != nil {
		t.Fatal(err)
	}
	type answer struct {
		result application.AcquireResult
		err    *domain.Error
	}
	done := make(chan answer, 1)
	go func() {
		result, err := e.acquire.Acquire(context.Background(), application.AcquireRequest{
			Count: 1, ConsumerID: "audit-disable", CooldownMS: 60000,
		})
		done <- answer{result: result, err: err}
	}()
	select {
	case <-arrived:
	case <-time.After(3 * time.Second):
		t.Fatal("on-demand candidate never entered its probe")
	}
	if err := e.ingest.EnsureSource(context.Background(), "audit-api", sourceCfg, false); err != nil {
		t.Fatal(err)
	}
	unblock()
	select {
	case out := <-done:
		if out.err != nil || out.result.Delivered != 0 {
			t.Fatalf("old ephemeral candidate committed after source disable: err=%v result=%+v", out.err, out.result)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("disabled candidate did not stop within its request budget")
	}
	stats, err := e.store.Stats(context.Background())
	if err != nil || stats.Resources != 0 || stats.EnabledOffers != 0 || stats.ActiveLeases != 0 {
		t.Fatalf("on-demand result leaked inventory or a committed lease: stats=%+v err=%v", stats, err)
	}
}

func TestAuditOnDemandDeletedSourceCannotCommitAsRecreatedInstance(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	inner := startFakeProxy(t)
	arrived, resume := make(chan struct{}), make(chan struct{})
	var started, released sync.Once
	unblock := func() { released.Do(func() { close(resume) }) }
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started.Do(func() { close(arrived) })
		select {
		case <-resume:
			inner.Config.Handler.ServeHTTP(w, r)
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(proxy.Close)
	t.Cleanup(unblock)
	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 200, "data": map[string]any{"count": 1,
				"proxies": []string{fmt.Sprintf("127.0.0.1:%d", portOf(proxy.URL))}},
		})
	}))
	t.Cleanup(vendor.Close)
	e.ingest.ScdnEndpoint, e.ingest.ScdnHTTPClient = vendor.URL, vendor.Client()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const sourceID = "audit-recreated-api"
	if _, derr := e.ingest.CreateSource(ctx, sourceID, application.SourceConfig{
		MaxPoolLeases: 1, Provider: &application.ProviderConfig{Protocol: "http", FetchCount: 1},
	}, true); derr != nil {
		t.Fatal(derr)
	}
	type answer struct {
		result application.AcquireResult
		err    *domain.Error
	}
	done := make(chan answer, 1)
	go func() {
		result, derr := e.acquire.Acquire(ctx, application.AcquireRequest{
			Count: 1, DeadlineMS: 3000, ProjectID: application.DefaultAcquireProject,
			ConsumerID: application.DefaultAcquireProject,
			Filters: application.Filters{
				Transports: []domain.Transport{domain.TransportHTTP}, SourceIDs: []string{sourceID},
			},
		})
		done <- answer{result: result, err: derr}
	}()
	select {
	case <-arrived:
	case <-time.After(3 * time.Second):
		t.Fatal("old API candidate did not enter the controlled probe")
	}
	if _, err := e.store.DeleteSource(ctx, sourceID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, derr := e.ingest.CreateSource(ctx, sourceID, application.SourceConfig{
		MaxPoolLeases: 1, Provider: &application.ProviderConfig{Protocol: "socks5", FetchCount: 1},
	}, true); derr != nil {
		t.Fatal(derr)
	}
	unblock()
	select {
	case out := <-done:
		if out.err != nil || out.result.Delivered != 0 || len(out.result.Items) != 0 {
			t.Fatalf("old HTTP candidate committed under a recreated socks5 source: result=%+v err=%v", out.result, out.err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("source replacement did not stop the obsolete candidate within budget")
	}
	stats, err := e.store.Stats(ctx)
	if err != nil || stats.Resources != 0 || stats.EnabledOffers != 0 || stats.ActiveLeases != 0 {
		t.Fatalf("obsolete source candidate left public inventory or a delivery: stats=%+v err=%v", stats, err)
	}
}

func TestAuditOldSourcePassCannotCloseRecreatedCircuit(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	inner := startFakeProxy(t)
	arrived, release := make(chan struct{}), make(chan struct{})
	var entered, released sync.Once
	unblock := func() { released.Do(func() { close(release) }) }
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered.Do(func() { close(arrived) })
		select {
		case <-release:
			inner.Config.Handler.ServeHTTP(w, r)
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(proxy.Close)
	t.Cleanup(unblock)
	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 200, "data": map[string]any{"count": 1,
				"proxies": []string{fmt.Sprintf("127.0.0.1:%d", portOf(proxy.URL))}},
		})
	}))
	t.Cleanup(vendor.Close)
	e.ingest.ScdnEndpoint, e.ingest.ScdnHTTPClient = vendor.URL, vendor.Client()
	ctx := context.Background()
	const sourceID = "audit-recreated-circuit"
	sourceConfig := application.SourceConfig{
		MaxPoolLeases: 1, Provider: &application.ProviderConfig{Protocol: "http", FetchCount: 1},
	}
	if _, err := e.ingest.CreateSource(ctx, sourceID, sourceConfig, true); err != nil {
		t.Fatal(err)
	}
	type answer struct {
		result application.AcquireResult
		err    *domain.Error
	}
	done := make(chan answer, 1)
	go func() {
		result, err := e.acquire.Acquire(ctx, application.AcquireRequest{
			Count: 1, DeadlineMS: 3000, ProjectID: application.DefaultAcquireProject,
			ConsumerID: application.DefaultAcquireProject,
			Filters:    application.Filters{SourceIDs: []string{sourceID}},
		})
		done <- answer{result, err}
	}()
	select {
	case <-arrived:
	case <-time.After(3 * time.Second):
		t.Fatal("old channel did not enter the controlled probe")
	}
	if _, err := e.store.DeleteSource(ctx, sourceID, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := e.ingest.CreateSource(ctx, sourceID, sourceConfig, true); err != nil {
		t.Fatal(err)
	}
	current, err := e.store.RecordSourceOutcome(ctx, sourceID, "FAIL", time.Now(),
		sqlite.SourceCircuitPolicy{FailureThreshold: 1, Base: time.Hour, Max: time.Hour})
	if err != nil || current.State != sqlite.CircuitOpen || current.CooldownUntil == nil {
		t.Fatalf("fixture did not open the recreated channel circuit: circuit=%+v err=%v", current, err)
	}
	unblock()
	select {
	case out := <-done:
		if out.err != nil || out.result.Delivered != 0 {
			t.Fatalf("obsolete channel candidate unexpectedly delivered: result=%+v err=%v", out.result, out.err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("old channel probe did not finish within request budget")
	}
	var state, until string
	if err := e.store.DB().QueryRow(`SELECT state, COALESCE(cooldown_until, '') FROM source_circuits WHERE source_id = ?`, sourceID).
		Scan(&state, &until); err != nil {
		t.Fatal(err)
	}
	deadline, err := time.Parse(time.RFC3339Nano, until)
	if err != nil || state != sqlite.CircuitOpen || !deadline.Equal(*current.CooldownUntil) {
		t.Fatalf("old PASS reset the recreated channel's circuit: state=%s until=%s expected=%s err=%v", state, until, current.CooldownUntil, err)
	}
}

func TestAuditUndispatchedExpiredProofDoesNotStartCooldown(t *testing.T) {
	e := newEnv(t, startJudge(t), func(c *config.Config) {
		c.ParallelPerRequest = 2
		c.ConnectTimeoutMS = 2500
		c.ProbeTotalTimeoutMS = 3000
		c.MaxProofAgeMS = 100
		c.EvidenceCacheTTLMS = 100
	})
	good := startFakeProxy(t)
	slow, _, _ := auditBlockingProxy(t)
	if err := e.ingest.EnsureSource(context.Background(), "audit-dispatch", application.SourceConfig{
		DefaultTransport: domain.TransportHTTP, MaxPoolLeases: 1,
	}, true); err != nil {
		t.Fatal(err)
	}
	if _, derr := e.ingest.ImportLines(context.Background(), "audit-dispatch", good.URL+"\n"+slow.URL+"\n"); derr != nil {
		t.Fatal(derr)
	}
	cd := int64(600)
	result, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{
		Count: 2, ProjectID: application.DefaultAcquireProject, ConsumerID: application.DefaultAcquireProject,
		DeadlineMS: 1000, CDSeconds: &cd,
	})
	if derr != nil {
		t.Fatal(derr)
	}
	returned := make(map[string]bool)
	for _, item := range result.Items {
		returned[e.acquire.FingerprintOf(item.Conn)] = true
		if time.Since(item.Validation.CompletedAt) > e.cfg.MaxProofAge()+50*time.Millisecond {
			t.Error("dispatch relaxed the configured proof-age bound")
		}
	}
	var fingerprints []string
	rows, err := e.store.DB().Query(`SELECT connection_fingerprint FROM project_proxy_cd WHERE project_id=?`, application.DefaultAcquireProject)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var fingerprint string
		if err := rows.Scan(&fingerprint); err != nil {
			t.Fatal(err)
		}
		fingerprints = append(fingerprints, fingerprint)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for _, fingerprint := range fingerprints {
		if !returned[fingerprint] {
			t.Error("a candidate removed from the final response left an irreversible project CD window")
		}
	}
}

func TestAuditDispatchCleanupCannotClearAnotherRequestsCooldown(t *testing.T) {
	e := newEnv(t, startJudge(t), func(c *config.Config) {
		c.ParallelPerRequest = 2
		c.ConnectTimeoutMS = 2500
		c.ProbeTotalTimeoutMS = 3000
		c.MaxProofAgeMS = 100
		c.EvidenceCacheTTLMS = 100
	})
	inner := startFakeProxy(t)
	validated := make(chan struct{})
	var ready sync.Once
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inner.Config.Handler.ServeHTTP(w, r)
		ready.Do(func() { close(validated) })
	}))
	t.Cleanup(good.Close)
	slow, _, _ := auditBlockingProxy(t)
	for _, source := range []string{"audit-cd-mixed", "audit-cd-good"} {
		if err := e.ingest.EnsureSource(context.Background(), source, application.SourceConfig{
			DefaultTransport: domain.TransportHTTP, MaxPoolLeases: 2,
		}, true); err != nil {
			t.Fatal(err)
		}
		lines := good.URL + "\n"
		if source == "audit-cd-mixed" {
			lines += slow.URL + "\n"
		}
		if _, derr := e.ingest.ImportLines(context.Background(), source, lines); derr != nil {
			t.Fatal(derr)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	firstDone := make(chan *domain.Error, 1)
	zero := int64(0)
	go func() {
		_, derr := e.acquire.Acquire(ctx, application.AcquireRequest{
			Count: 2, ProjectID: application.DefaultAcquireProject, ConsumerID: application.DefaultAcquireProject,
			DeadlineMS: 1000, CDSeconds: &zero, Filters: application.Filters{SourceIDs: []string{"audit-cd-mixed"}},
		})
		firstDone <- derr
	}()
	select {
	case <-validated:
	case <-time.After(time.Second):
		t.Fatal("first request never completed its good probe")
	}
	cd := int64(600)
	second, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{
		Count: 1, ProjectID: application.DefaultAcquireProject, ConsumerID: application.DefaultAcquireProject,
		CDSeconds: &cd, Filters: application.Filters{SourceIDs: []string{"audit-cd-good"}},
	})
	if derr != nil || second.Delivered != 1 {
		t.Fatalf("second request could not make the independent CD delivery: result=%+v err=%v", second, derr)
	}
	fp := e.acquire.FingerprintOf(second.Items[0].Conn)
	before, err := e.store.ProjectCooldownBlocked(context.Background(), application.DefaultAcquireProject, time.Now(), []string{fp})
	if err != nil || len(before) != 1 {
		t.Fatalf("second delivery did not open its window: windows=%+v err=%v", before, err)
	}
	select {
	case derr := <-firstDone:
		if derr != nil {
			t.Fatal(derr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first request did not finalize within its budget")
	}
	after, err := e.store.ProjectCooldownBlocked(context.Background(), application.DefaultAcquireProject, time.Now(), []string{fp})
	if err != nil || len(after) != 1 || !after[fp].Equal(before[fp]) {
		t.Fatalf("cd=0 request cleanup erased or changed another request's fixed window: before=%+v after=%+v err=%v", before, after, err)
	}
}

func TestAuditFinalEligibilityRecheckKeepsQualifiedPartial(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	good := startFakeProxy(t)
	inner := startFakeProxy(t)
	arrived, release := make(chan struct{}), make(chan struct{})
	var started, released sync.Once
	unblock := func() { released.Do(func() { close(release) }) }
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started.Do(func() { close(arrived) })
		select {
		case <-release:
			inner.Config.Handler.ServeHTTP(w, r)
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(bad.Close)
	t.Cleanup(unblock)
	e.importProxy(t, "audit-final-good", good.URL)
	e.importProxy(t, "audit-final-bad", bad.URL)
	type answer struct {
		result application.AcquireResult
		err    *domain.Error
	}
	done := make(chan answer, 1)
	go func() {
		cd := int64(600)
		result, err := e.acquire.Acquire(context.Background(), application.AcquireRequest{
			Count: 2, ProjectID: application.DefaultAcquireProject, ConsumerID: application.DefaultAcquireProject,
			CDSeconds: &cd, DeadlineMS: 3000,
		})
		done <- answer{result, err}
	}()
	select {
	case <-arrived:
	case <-time.After(time.Second):
		t.Fatal("candidate did not enter its pre-disable probe")
	}
	if err := e.ingest.EnsureSource(context.Background(), "audit-final-bad", application.SourceConfig{
		DefaultTransport: domain.TransportHTTP, MaxPoolLeases: 1,
	}, false); err != nil {
		t.Fatal(err)
	}
	unblock()
	select {
	case out := <-done:
		if out.err != nil || out.result.Delivered != 1 || out.result.Missing != 1 || !out.result.Partial ||
			len(out.result.Items) != 1 || out.result.Items[0].SourceID != "audit-final-good" {
			t.Fatalf("one ineligible candidate discarded an independent qualified partial: result=%+v err=%v", out.result, out.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("eligibility recheck did not finish within the request budget")
	}
}

func TestAuditFinalDeliveryRespectsShortenedRetention(t *testing.T) {
	e := newEnv(t, startJudge(t), func(c *config.Config) {
		c.MinUsableMS = 100
		c.ExpirySafetyMarginMS = 0
	})
	inner := startFakeProxy(t)
	arrived, release := make(chan struct{}), make(chan struct{})
	var started, released sync.Once
	unblock := func() { released.Do(func() { close(release) }) }
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started.Do(func() { close(arrived) })
		select {
		case <-release:
			inner.Config.Handler.ServeHTTP(w, r)
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(proxy.Close)
	t.Cleanup(unblock)
	e.importProxy(t, "audit-shortened-retention", proxy.URL)
	type answer struct {
		result application.AcquireResult
		err    *domain.Error
	}
	done := make(chan answer, 1)
	go func() {
		zero := int64(0)
		result, err := e.acquire.Acquire(context.Background(), application.AcquireRequest{
			Count: 1, ProjectID: application.DefaultAcquireProject, ConsumerID: application.DefaultAcquireProject,
			CDSeconds: &zero, DeadlineMS: 3000,
		})
		done <- answer{result, err}
	}()
	select {
	case <-arrived:
	case <-time.After(time.Second):
		t.Fatal("candidate did not enter its pre-update probe")
	}
	retention := time.Now().UTC().Add(time.Second)
	if _, err := e.store.DB().Exec(`UPDATE source_offers SET local_retention_deadline = ?
		WHERE source_id = 'audit-shortened-retention'`, retention.Format("2006-01-02T15:04:05.000000000Z07:00")); err != nil {
		t.Fatal(err)
	}
	unblock()
	select {
	case out := <-done:
		if out.err != nil || out.result.Delivered != 1 || len(out.result.Items) != 1 {
			t.Fatalf("still-usable candidate was not delivered: result=%+v err=%v", out.result, out.err)
		}
		if out.result.Items[0].ExpiresAt.After(retention) {
			t.Fatalf("final delivery overstated updated retention: deadline=%s current_retention=%s", out.result.Items[0].ExpiresAt, retention)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("updated-retention delivery exceeded the request budget")
	}
}

func TestAuditCanceledAcquireReleasesOnlyItsOwnReservations(t *testing.T) {
	e := newEnv(t, startJudge(t), func(c *config.Config) {
		c.ParallelPerRequest = 1
		c.ConnectTimeoutMS = 2500
		c.ProbeTotalTimeoutMS = 3000
	})
	proxy, arrived, _ := auditBlockingProxy(t)
	const source = "audit-cancel-owned-reservations"
	if err := e.ingest.EnsureSource(context.Background(), source, application.SourceConfig{
		DefaultTransport: domain.TransportHTTP, MaxPoolLeases: 2,
	}, true); err != nil {
		t.Fatal(err)
	}
	if _, derr := e.ingest.ImportLines(context.Background(), source, proxy.URL+"\n"); derr != nil {
		t.Fatal(derr)
	}
	now := time.Now()
	candidates, err := e.store.SelectCandidates(context.Background(), sqlite.SelectionFilters{
		Profile: validation.ProfileHTTPSBasicV1, Now: now,
		UsefulCutoff: now.Add(-time.Hour), SourceIDs: []string{source}, Limit: 1,
	})
	if err != nil || len(candidates) != 1 {
		t.Fatalf("fixture selection: candidates=%d err=%v", len(candidates), err)
	}
	foreign, err := e.store.ReserveSlot(context.Background(), candidates[0], sqlite.ReserveArgs{
		Owner: "audit-other-live-request", ConsumerID: "audit-other-live-consumer",
		ProjectID: application.DefaultAcquireProject, IdentitySecret: e.cfg.IdentitySecret,
		Profile: validation.ProfileHTTPSBasicV1, Now: now,
		ReservationDeadline: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = e.store.ReleaseReservation(context.Background(), foreign.ReservationID, time.Now())
	})
	var foreignDeadline string
	if err := e.store.DB().QueryRow(`SELECT deadline FROM reservations WHERE reservation_id=?`, foreign.ReservationID).
		Scan(&foreignDeadline); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type answer struct {
		result application.AcquireResult
		err    *domain.Error
	}
	done := make(chan answer, 1)
	go func() {
		zero := int64(0)
		result, derr := e.acquire.Acquire(ctx, application.AcquireRequest{
			Count: 1, ProjectID: application.DefaultAcquireProject,
			ConsumerID: application.DefaultAcquireProject, CDSeconds: &zero, DeadlineMS: 3000,
			Filters: application.Filters{SourceIDs: []string{source}},
		})
		done <- answer{result, derr}
	}()
	select {
	case <-arrived:
	case <-time.After(2 * time.Second):
		t.Fatal("acquire did not enter the blocked probe after taking its reservation")
	}
	var held int
	if err := e.store.DB().QueryRow(`SELECT COUNT(*) FROM reservations WHERE state='RESERVED'`).Scan(&held); err != nil || held != 2 {
		t.Fatalf("fixture did not hold both independent reservations: held=%d err=%v", held, err)
	}
	canceledAt := time.Now()
	cancel()
	select {
	case out := <-done:
		if out.err != nil || out.result.Delivered != 0 || out.result.Missing != 1 || out.result.ShortfallReason == "" {
			t.Fatalf("cancellation did not return an explained business shortfall: result=%+v err=%v", out.result, out.err)
		}
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("request cancellation did not finish within its bounded cleanup budget")
	}
	if time.Since(canceledAt) > 1500*time.Millisecond {
		t.Fatal("reservation cleanup extended the canceled work without a bound")
	}
	var owned, leases, cd int
	if err := e.store.DB().QueryRow(`SELECT COUNT(*) FROM reservations
		WHERE owner <> ? AND state IN ('RESERVED','VALIDATING')`, "audit-other-live-request").Scan(&owned); err != nil {
		t.Fatal(err)
	}
	if owned != 0 {
		t.Errorf("canceled acquire left %d of its own active reservations after return", owned)
	}
	var state, deadline string
	if err := e.store.DB().QueryRow(`SELECT state, deadline FROM reservations WHERE reservation_id=?`, foreign.ReservationID).
		Scan(&state, &deadline); err != nil || state != "RESERVED" || deadline != foreignDeadline {
		t.Fatalf("cleanup rewrote another request's live reservation: state=%s deadline=%s err=%v", state, deadline, err)
	}
	if err := e.store.DB().QueryRow(`SELECT COUNT(*) FROM leases WHERE state='ACTIVE'`).Scan(&leases); err != nil {
		t.Fatal(err)
	}
	if err := e.store.DB().QueryRow(`SELECT COUNT(*) FROM project_proxy_cd`).Scan(&cd); err != nil {
		t.Fatal(err)
	}
	if leases != 0 || cd != 0 {
		t.Fatalf("canceled work created a lease or project CD: leases=%d cd=%d", leases, cd)
	}
}

func TestAuditZeroCooldownConcurrentDeliveryUsesTwoSlotsWithoutReservationLeak(t *testing.T) {
	e := newEnv(t, startJudge(t), func(c *config.Config) {
		c.MaxActiveAcquires = 6
		c.ParallelPerRequest = 1
		c.ProbeSlots = 2
		c.ConnectTimeoutMS = 2500
		c.ProbeTotalTimeoutMS = 3000
	})
	inner := startFakeProxy(t)
	bothArrived, release := make(chan struct{}), make(chan struct{})
	var hits atomic.Int32
	var released sync.Once
	unblock := func() { released.Do(func() { close(release) }) }
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 2 {
			close(bothArrived)
		}
		select {
		case <-release:
			inner.Config.Handler.ServeHTTP(w, r)
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(proxy.Close)
	t.Cleanup(unblock)
	const source = "audit-two-slots-zero-cd"
	if err := e.ingest.EnsureSource(context.Background(), source, application.SourceConfig{
		DefaultTransport: domain.TransportHTTP, MaxPoolLeases: 2,
	}, true); err != nil {
		t.Fatal(err)
	}
	if _, derr := e.ingest.ImportLines(context.Background(), source, proxy.URL+"\n"); derr != nil {
		t.Fatal(derr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	type answer struct {
		result  application.AcquireResult
		err     *domain.Error
		elapsed time.Duration
	}
	begin, done := make(chan struct{}), make(chan answer, 6)
	for i := 0; i < 6; i++ {
		go func() {
			<-begin
			began := time.Now()
			zero := int64(0)
			result, derr := e.acquire.Acquire(ctx, application.AcquireRequest{
				Count: 1, ProjectID: application.DefaultAcquireProject,
				ConsumerID: application.DefaultAcquireProject, CDSeconds: &zero, DeadlineMS: 3000,
				Filters: application.Filters{SourceIDs: []string{source}},
			})
			done <- answer{result, derr, time.Since(began)}
		}()
	}
	close(begin)
	select {
	case <-bothArrived:
	case <-time.After(2 * time.Second):
		t.Fatal("two legitimate cd=0 requests could not occupy the two probe slots")
	}
	unblock()
	var items []application.AcquiredItem
	for i := 0; i < 6; i++ {
		select {
		case out := <-done:
			if out.err != nil || out.result.Delivered+out.result.Missing != 1 ||
				(out.result.Delivered == 0 && out.result.ShortfallReason == "") {
				t.Fatalf("capacity contention did not return a business result: result=%+v err=%v", out.result, out.err)
			}
			if out.result.Delivered == 0 && out.elapsed < 2*time.Second && ctx.Err() == nil &&
				out.result.ShortfallReason == "VALIDATION_BUDGET_EXHAUSTED" {
				t.Errorf("fast capacity rejection was misreported as exhausted request budget: elapsed=%s", out.elapsed)
			}
			items = append(items, out.result.Items...)
		case <-ctx.Done():
			t.Fatal("bounded capacity contention did not finish")
		}
	}
	if len(items) != 2 {
		t.Errorf("one connection with two slots delivered %d times, want 2", len(items))
	}
	for _, item := range items {
		if e.acquire.FingerprintOf(item.Conn) != e.acquire.FingerprintOf(domain.Connection{
			Transport: domain.TransportHTTP, Host: "127.0.0.1", Port: portOf(proxy.URL),
		}) {
			t.Fatal("capacity workload returned a different connection")
		}
	}
	stats, err := e.store.Stats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var cd int
	if err := e.store.DB().QueryRow(`SELECT COUNT(*) FROM project_proxy_cd`).Scan(&cd); err != nil {
		t.Fatal(err)
	}
	if stats.ActiveLeases != 2 || stats.ActiveReservations != 0 || cd != 0 {
		t.Errorf("two-slot delivery leaked reservations or invented CD: leases=%d reservations=%d cd=%d",
			stats.ActiveLeases, stats.ActiveReservations, cd)
	}
	for _, item := range items {
		if ok, derr := e.leases.Release(context.Background(), item.LeaseID, application.DefaultAcquireProject, false); derr != nil || !ok {
			t.Fatalf("release: ok=%v err=%v", ok, derr)
		}
	}
	stats, err = e.store.Stats(context.Background())
	if err != nil || stats.ActiveLeases != 0 || stats.ActiveReservations != 0 {
		t.Fatalf("completed workload did not leave both slots free: stats=%+v err=%v", stats, err)
	}
}

func TestAuditAcquisitionReasonsSeparateInventoryExhaustionAndRequestDeadline(t *testing.T) {
	for _, deadlineCase := range []bool{false, true} {
		name := "inventory_exhaustion"
		if deadlineCase {
			name = "request_deadline"
		}
		t.Run(name, func(t *testing.T) {
			e := newEnv(t, startJudge(t), func(c *config.Config) {
				c.ParallelPerRequest = 1
				c.ConnectTimeoutMS = 1500
				c.ProbeTotalTimeoutMS = 2000
			})
			proxyURL := ""
			count, deadlineMS := 2, int64(3000)
			wantReason := "NO_ELIGIBLE_CANDIDATES"
			if deadlineCase {
				proxy, _, _ := auditBlockingProxy(t)
				proxyURL = proxy.URL
				count, deadlineMS = 1, 250
				wantReason = "VALIDATION_BUDGET_EXHAUSTED"
			} else {
				proxyURL = startFakeProxy(t).URL
			}
			e.importProxy(t, "audit-budget-reason", proxyURL)
			zero := int64(0)
			began := time.Now()
			result, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{
				Count: count, ProjectID: application.DefaultAcquireProject,
				ConsumerID: application.DefaultAcquireProject, CDSeconds: &zero, DeadlineMS: deadlineMS,
				Filters: application.Filters{SourceIDs: []string{"audit-budget-reason"}},
			})
			elapsed := time.Since(began)
			if derr != nil || result.ShortfallReason != wantReason || result.Missing != 1 {
				t.Errorf("shortfall cause does not describe the observed path: elapsed=%s delivered=%d missing=%d reason=%s err=%v",
					elapsed, result.Delivered, result.Missing, result.ShortfallReason, derr)
			}
			if deadlineCase {
				if result.Delivered != 0 || result.Partial || elapsed > 1500*time.Millisecond {
					t.Errorf("expired work was not bounded and empty: elapsed=%s result=%+v", elapsed, result)
				}
			} else if result.Delivered != 1 || !result.Partial || elapsed > 2*time.Second {
				t.Errorf("known exhausted inventory did not return its valid partial promptly: elapsed=%s result=%+v", elapsed, result)
			}
			stats, err := e.store.Stats(context.Background())
			if err != nil || stats.ActiveReservations != 0 || stats.Resources != 1 || stats.EnabledOffers != 1 || stats.HealthFail != 0 {
				t.Fatalf("shortfall cleanup or request-timeout attribution damaged the inventory: stats=%+v err=%v", stats, err)
			}
			for _, item := range result.Items {
				if ok, derr := e.leases.Release(context.Background(), item.LeaseID, application.DefaultAcquireProject, false); derr != nil || !ok {
					t.Fatalf("fixture delivery release: ok=%v err=%v", ok, derr)
				}
			}
		})
	}
}

func TestAuditCancellationDuringCandidateSQLIsNotStorageOutage(t *testing.T) {
	for _, phase := range []string{"selection", "reservation"} {
		t.Run(phase, func(t *testing.T) {
			e := newEnv(t, startJudge(t), nil)
			const source = "audit-canceled-candidate-sql"
			e.importProxy(t, source, startFakeProxy(t).URL)
			arrived, release := make(chan struct{}), make(chan struct{})
			var started, released sync.Once
			unblock := func() { released.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			gate := func() {
				started.Do(func() {
					close(arrived)
					<-release
				})
			}
			// The gate permits the SQL operation; only caller cancellation interrupts it.
			connection, err := e.store.DB().Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			err = connection.Raw(func(driverConnection any) error {
				conn, ok := driverConnection.(*sqlite3.SQLiteConn)
				if !ok {
					return fmt.Errorf("unexpected SQLite connection %T", driverConnection)
				}
				if phase == "selection" {
					conn.RegisterAuthorizer(func(operation int, table, _, _ string) int {
						if operation == sqlite3.SQLITE_READ && table == "proxy_resources" {
							gate()
						}
						return sqlite3.SQLITE_OK
					})
					return nil
				}
				return conn.RegisterFunc("audit_candidate_sql_gate", func() int {
					gate()
					return 0
				}, false)
			})
			_ = connection.Close()
			if err != nil {
				t.Fatal(err)
			}
			if phase == "reservation" {
				if _, err := e.store.DB().Exec(`CREATE TRIGGER audit_wait_for_candidate_cancel
					BEFORE INSERT ON reservations BEGIN SELECT audit_candidate_sql_gate(); END`); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			type answer struct {
				result application.AcquireResult
				err    *domain.Error
			}
			done := make(chan answer, 1)
			go func() {
				zero := int64(0)
				result, derr := e.acquire.Acquire(ctx, application.AcquireRequest{
					Count: 1, ProjectID: application.DefaultAcquireProject,
					ConsumerID: application.DefaultAcquireProject, CDSeconds: &zero,
					DeadlineMS: 3000, Filters: application.Filters{SourceIDs: []string{source}},
				})
				done <- answer{result, derr}
			}()
			select {
			case <-arrived:
			case <-time.After(2 * time.Second):
				t.Fatalf("acquire did not enter the gated %s SQL", phase)
			}
			cancel()
			unblock()
			select {
			case out := <-done:
				if out.err != nil || out.result.Delivered != 0 || out.result.Missing != 1 ||
					out.result.ShortfallReason != "VALIDATION_BUDGET_EXHAUSTED" {
					t.Errorf("caller cancellation during %s became a storage outage: delivered=%d missing=%d reason=%s err=%v",
						phase, out.result.Delivered, out.result.Missing, out.result.ShortfallReason, out.err)
				}
			case <-time.After(1500 * time.Millisecond):
				t.Fatal("canceled SQL did not finish within the cleanup bound")
			}
			stats, err := e.store.Stats(context.Background())
			if err != nil || stats.ActiveReservations != 0 || stats.ActiveLeases != 0 ||
				stats.Resources != 1 || stats.EnabledOffers != 1 || stats.HealthFail != 0 {
				t.Fatalf("canceled SQL polluted inventory or left active ownership: stats=%+v err=%v", stats, err)
			}
			var cd int
			if err := e.store.DB().QueryRow(`SELECT COUNT(*) FROM project_proxy_cd`).Scan(&cd); err != nil || cd != 0 {
				t.Fatalf("canceled SQL started a project CD: cd=%d err=%v", cd, err)
			}
		})
	}
}
