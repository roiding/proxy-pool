package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"proxypool/internal/application"
	"proxypool/internal/config"
	"proxypool/internal/domain"
	"proxypool/internal/geoip"
	"proxypool/internal/storage/sqlite"
)

func TestAuditStickyBindingPersistenceIsAtomicWithDelivery(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	e.importProxy(t, "audit-sticky-atomic", startFakeProxy(t).URL)
	srv := auditServer(t, e)
	project := auditProject(t, srv, e.cfg.APIToken, "sticky-atomic-project", "")
	_, token := auditProjectKey(t, srv, e.cfg.APIToken, project.ID, "sticky-atomic-key")
	if _, err := e.store.DB().Exec(`CREATE TRIGGER audit_reject_binding BEFORE UPDATE ON sticky_bindings
		WHEN NEW.state='ACTIVE' BEGIN SELECT RAISE(ABORT, 'audit simulated binding failure'); END`); err != nil {
		t.Fatal(err)
	}
	status, data := auditPost(t, srv, token, "/api/v1/proxies/acquire",
		`{"count":1,"mode":"sticky","session_id":"audit-atomic","cd":600}`)
	if status < 500 {
		t.Errorf("sticky binding failure still delivered a connection: status=%d body=%s", status, data)
	}
	var active, cooldowns int
	if err := e.store.DB().QueryRow(`SELECT COUNT(*) FROM leases WHERE state='ACTIVE'`).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if err := e.store.DB().QueryRow(`SELECT COUNT(*) FROM project_proxy_cd WHERE project_id=?`, project.ID).Scan(&cooldowns); err != nil {
		t.Fatal(err)
	}
	if active != 0 || cooldowns != 0 {
		t.Errorf("binding failure did not roll back delivery/CD: active=%d cooldowns=%d", active, cooldowns)
	}
}

func TestAuditStickyReuseHonorsCurrentEligibility(t *testing.T) {
	cases := []struct {
		name string
		body string
		edit func(*env) error
	}{
		{name: "disabled_source", body: `{"count":1,"mode":"sticky","session_id":"audit-reuse","cd":0}`,
			edit: func(e *env) error {
				return e.ingest.EnsureSource(context.Background(), "audit-sticky-scope", application.SourceConfig{
					DefaultTransport: domain.TransportHTTP, MaxPoolLeases: 1,
				}, false)
			}},
		{name: "project_scope_revoked", body: `{"count":1,"mode":"sticky","session_id":"audit-reuse","cd":0}`,
			edit: func(e *env) error {
				p, err := e.store.GetProject(context.Background(), application.DefaultAcquireProject)
				if err != nil {
					return err
				}
				p.AllowedSourceIDs = []string{"a-different-source"}
				_, err = e.store.UpdateProject(context.Background(), p, p.ConfigVersion, time.Now())
				return err
			}},
		{name: "different_source_filter", body: `{"count":1,"mode":"sticky","session_id":"audit-reuse","source_ids":["a-different-source"]}`},
		{name: "different_protocol", body: `{"count":1,"mode":"sticky","session_id":"audit-reuse","protocols":["socks5"]}`},
		{name: "different_country", body: `{"count":1,"mode":"sticky","session_id":"audit-reuse","country":["CN"]}`},
		{name: "stronger_target_validation", body: `{"count":1,"mode":"sticky","session_id":"audit-reuse","validation":{"mode":"tls","target":"127.0.0.1:1"}}`},
		{name: "retired_resource", body: `{"count":1,"mode":"sticky","session_id":"audit-reuse","cd":0}`,
			edit: func(e *env) error {
				b, err := e.store.GetBinding(context.Background(), application.DefaultAcquireProject, "audit-reuse")
				if err != nil {
					return err
				}
				if _, err := e.store.DB().Exec(`UPDATE leases SET state = 'RELEASED'`); err != nil {
					return err
				}
				retired, err := e.store.RetireResourceOwned(context.Background(), b.ResourceID, b.Incarnation,
					b.SourceID, b.SourceConfigVersion, "AUTH_FAILED", time.Now())
				if err != nil || retired == 0 {
					return fmt.Errorf("fixture could not retire the bound resource: offers=%d err=%v", retired, err)
				}
				return nil
			}},
		{name: "source_cooling", body: `{"count":1,"mode":"sticky","session_id":"audit-reuse","cd":0}`,
			edit: func(e *env) error {
				_, err := e.store.RecordSourceOutcome(context.Background(), "audit-sticky-scope", "FAIL", time.Now(),
					sqlite.SourceCircuitPolicy{FailureThreshold: 1, Base: time.Hour, Max: time.Hour})
				return err
			}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, startJudge(t), nil)
			e.importProxy(t, "audit-sticky-scope", startFakeProxy(t).URL)
			_, token, err := e.keys.Create(context.Background(), "audit-sticky-scope-key", 0)
			if err != nil {
				t.Fatal(err)
			}
			srv := auditServer(t, e)
			status, data := auditPost(t, srv, token, "/api/v1/proxies/acquire",
				`{"count":1,"mode":"sticky","session_id":"audit-reuse","cd":600}`)
			var first application.AcquireResult
			if status != http.StatusOK || json.Unmarshal(data, &first) != nil || first.Delivered != 1 {
				t.Fatalf("initial binding: status=%d body=%s", status, data)
			}
			if tc.edit != nil {
				if err := tc.edit(e); err != nil {
					t.Fatal(err)
				}
			}
			status, data = auditPost(t, srv, token, "/api/v1/proxies/acquire", tc.body)
			var next application.AcquireResult
			if json.Unmarshal(data, &next) != nil {
				t.Fatalf("invalid response: status=%d body=%s", status, data)
			}
			if next.Delivered != 0 || len(next.Items) != 0 {
				t.Fatalf("sticky CD exception bypassed current eligibility: status=%d body=%s", status, data)
			}
		})
	}
}

func TestAuditStickyReuseNeverServesSupersededProof(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	e.importProxy(t, "audit-sticky-evidence", startFakeProxy(t).URL)
	_, token, err := e.keys.Create(context.Background(), "audit-sticky-evidence-key", 0)
	if err != nil {
		t.Fatal(err)
	}
	srv := auditServer(t, e)
	status, data := auditPost(t, srv, token, "/api/v1/proxies/acquire",
		`{"count":1,"mode":"sticky","session_id":"audit-proof","cd":600}`)
	var first application.AcquireResult
	if status != http.StatusOK || json.Unmarshal(data, &first) != nil || first.Delivered != 1 {
		t.Fatalf("initial sticky binding: status=%d body=%s", status, data)
	}
	b, err := e.store.GetBinding(context.Background(), application.DefaultAcquireProject, "audit-proof")
	if err != nil {
		t.Fatal(err)
	}
	failure := b.Validation
	failure.AttemptID = "audit-new-negative-proof"
	failure.Outcome = "FAIL"
	failure.StartedAt = time.Now().UTC()
	failure.CompletedAt = failure.StartedAt
	scope := domain.ValidationScope{
		Mode: failure.Mode, TargetHostPort: failure.Target, Profile: failure.Profile, SecurityHash: failure.SecurityHash,
	}
	if err := e.store.PutEvidence(context.Background(), sqlite.EvidenceKey{
		ResourceID: b.ResourceID, Fingerprint: b.Fingerprint, SourceID: b.SourceID,
		SourceConfigVersion: b.SourceConfigVersion, Incarnation: b.Incarnation,
	}, domain.EvidenceScopeHash(scope), failure); err != nil {
		t.Fatal(err)
	}
	status, data = auditPost(t, srv, token, "/api/v1/proxies/acquire",
		`{"count":1,"mode":"sticky","session_id":"audit-proof","cd":0}`)
	var next application.AcquireResult
	if json.Unmarshal(data, &next) != nil {
		t.Fatalf("invalid sticky response: status=%d body=%s", status, data)
	}
	for _, item := range next.Items {
		row, err := e.store.GetLease(context.Background(), item.LeaseID)
		if err != nil {
			t.Fatal(err)
		}
		var committedProof domain.Evidence
		if err := json.Unmarshal([]byte(row.ValidationJSON), &committedProof); err != nil {
			t.Fatal(err)
		}
		if committedProof.Outcome != "PASS" || !committedProof.CompletedAt.After(failure.CompletedAt) ||
			!committedProof.CompletedAt.Equal(item.Validation.CompletedAt) ||
			committedProof.Mode != item.Validation.Mode || committedProof.SecurityHash != failure.SecurityHash {
			t.Fatalf("sticky reuse served an old PASS after newer FAIL: status=%d body=%s", status, data)
		}
		if item.Observed == nil || item.Observed.ExitIP != committedProof.ObservedExitIP {
			t.Fatal("canonical exit observation did not match the committed fresh proof")
		}
	}
}

func auditStickyLeaseIdentity(t *testing.T, e *env, result application.AcquireResult, sessionID string) sqlite.LeaseRow {
	t.Helper()
	item := result.Items[0]
	row, err := e.store.GetLease(context.Background(), item.LeaseID)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := e.store.GetBinding(context.Background(), application.DefaultAcquireProject, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	var inventoryConn, leaseConn domain.Connection
	if err := e.store.DB().QueryRow(`
		SELECT r.transport, r.host, r.port, COALESCE(c.username, ''), COALESCE(c.password, '')
		FROM proxy_resources r LEFT JOIN proxy_credentials c
		  ON c.credential_version = r.credential_version WHERE r.resource_id = ?`, binding.ResourceID).
		Scan(&inventoryConn.Transport, &inventoryConn.Host, &inventoryConn.Port, &inventoryConn.Username, &inventoryConn.Password); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(row.ConnectionJSON), &leaseConn); err != nil {
		t.Fatal(err)
	}
	// Internal inventory IDs are not part of the new public response. Verify
	// them against the durable lease, binding, and actual inventory instead.
	if row.Lease.Tenant != "default" || row.Lease.RequestID != result.RequestID ||
		row.Lease.ResourceID != binding.ResourceID || row.Lease.ResourceID == binding.Fingerprint ||
		item.SourceID != binding.SourceID || leaseConn != item.Conn ||
		e.acquire.FingerprintOf(inventoryConn) != e.acquire.FingerprintOf(item.Conn) ||
		e.acquire.FingerprintOf(item.Conn) != binding.Fingerprint {
		t.Fatalf("sticky delivery identity disagrees with its committed lease and inventory: lease=%+v binding=%+v response=%+v",
			row.Lease, binding, result)
	}
	return row
}

func TestAuditStickyReusePersistsCorrectLeaseIdentity(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	e.importProxy(t, "audit-sticky-lease-metadata", startFakeProxy(t).URL)
	srv := auditServer(t, e)
	body := `{"count":1,"mode":"sticky","session_id":"audit-lease-metadata","cd":600}`
	for call := 0; call < 2; call++ {
		status, data := auditPost(t, srv, e.cfg.APIToken, "/api/v1/proxies/acquire", body)
		var result application.AcquireResult
		if status != http.StatusOK || json.Unmarshal(data, &result) != nil || result.Delivered != 1 || len(result.Items) != 1 {
			t.Fatalf("sticky call %d did not deliver: status=%d body=%s", call, status, data)
		}
		row := auditStickyLeaseIdentity(t, e, result, "audit-lease-metadata")
		if call == 1 && row.Lease.ContextKey != "sticky|audit-lease-metadata" {
			t.Fatalf("sticky reuse stored the binding context in the wrong column: lease=%+v", row.Lease)
		}
	}
}

func TestAuditStickyRequestBudgetDoesNotBecomeLeaseTTL(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	e.importProxy(t, "audit-sticky-budget", startFakeProxy(t).URL)
	srv := auditServer(t, e)
	status, data := auditPost(t, srv, e.cfg.APIToken, "/api/v1/proxies/acquire",
		`{"count":1,"mode":"sticky","session_id":"audit-short-budget","timeout":1,"cd":0}`)
	var result application.AcquireResult
	if status != http.StatusOK || json.Unmarshal(data, &result) != nil || result.Delivered != 1 || len(result.Items) != 1 {
		t.Fatalf("a short wait budget shortened an otherwise usable connection lease: status=%d body=%s", status, data)
	}
	if result.Items[0].ExpiresAt.Sub(time.Now()) < e.cfg.MinUsable()-time.Second {
		t.Fatalf("request timeout became the connection's usable lifetime: %+v", result)
	}
}

func TestAuditStickyBindingUsesConfiguredTTL(t *testing.T) {
	for _, ttl := range []time.Duration{15 * time.Second, 90 * time.Second} {
		t.Run(ttl.String(), func(t *testing.T) {
			e := newEnv(t, startJudge(t), func(c *config.Config) {
				c.StickyBindingTTLMS = ttl.Milliseconds()
			})
			e.importProxy(t, "audit-binding-ttl", startFakeProxy(t).URL)
			srv := auditServer(t, e)
			started := time.Now().UTC()
			status, data := auditPost(t, srv, e.cfg.APIToken, "/api/v1/proxies/acquire",
				`{"count":1,"mode":"sticky","session_id":"audit-configured-ttl","cd":600}`)
			finished := time.Now().UTC()
			var result application.AcquireResult
			if status != http.StatusOK || json.Unmarshal(data, &result) != nil || result.Delivered != 1 || len(result.Items) != 1 {
				t.Fatalf("configured binding TTL prevented initial delivery: status=%d body=%s", status, data)
			}
			binding, err := e.store.GetBinding(context.Background(), application.DefaultAcquireProject, "audit-configured-ttl")
			if err != nil {
				t.Fatal(err)
			}
			// This upload has neither provider expiry nor local retention, so
			// the configured binding TTL is its only session lifetime bound.
			if binding.Deadline.Before(started.Add(ttl)) || binding.Deadline.After(finished.Add(ttl)) {
				t.Fatalf("binding ignored its configured TTL or substituted lease/proof TTL: configured=%s binding=%s lease=%s start=%s end=%s",
					ttl, binding.Deadline, result.Items[0].ExpiresAt, started, finished)
			}
		})
	}
}

func TestAuditStickyInitialBindingRespectsChangedOfferDeadline(t *testing.T) {
	for _, column := range []string{"local_retention_deadline", "provider_expires_at"} {
		t.Run(column, func(t *testing.T) {
			e := newEnv(t, startJudge(t), func(c *config.Config) {
				c.MinUsableMS = 1000
				c.ExpirySafetyMarginMS = 200
				c.StickyBindingTTLMS = 90000
			})
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
			e.importProxy(t, "audit-initial-sticky-cutoff", proxy.URL)
			type answer struct {
				result application.AcquireResult
				err    *domain.Error
			}
			done := make(chan answer, 1)
			go func() {
				zero := int64(0)
				result, err := e.acquire.Acquire(context.Background(), application.AcquireRequest{
					Count: 1, ProjectID: application.DefaultAcquireProject,
					ConsumerID: application.DefaultAcquireProject, CDSeconds: &zero,
					DeadlineMS: 5000, Lease: application.LeasePolicy{Mode: "sticky"},
					SessionID: "audit-initial-cutoff",
				})
				done <- answer{result, err}
			}()
			select {
			case <-arrived:
			case <-time.After(3 * time.Second):
				t.Fatal("sticky candidate did not enter the controlled probe")
			}
			deadline := time.Now().UTC().Add(2 * time.Second)
			if _, err := e.store.DB().Exec(`UPDATE source_offers SET `+column+` = ?
				WHERE source_id = 'audit-initial-sticky-cutoff'`,
				deadline.Format("2006-01-02T15:04:05.000000000Z07:00")); err != nil {
				t.Fatal(err)
			}
			cutoff := deadline
			if column == "provider_expires_at" {
				cutoff = deadline.Add(-e.cfg.ExpirySafetyMargin())
			}
			unblock()
			select {
			case out := <-done:
				if out.err != nil || out.result.Delivered != 1 || len(out.result.Items) != 1 {
					t.Fatalf("still-usable sticky candidate was not delivered: result=%+v err=%v", out.result, out.err)
				}
				if out.result.Items[0].ExpiresAt.After(cutoff) {
					t.Fatalf("initial sticky lease ignored the current offer cutoff: lease=%s cutoff=%s", out.result.Items[0].ExpiresAt, cutoff)
				}
				binding, err := e.store.GetBinding(context.Background(), application.DefaultAcquireProject, "audit-initial-cutoff")
				if err != nil || binding.Deadline.After(cutoff) {
					t.Fatalf("initial binding kept the pre-probe deadline after final delivery tightened it: binding=%s cutoff=%s err=%v", binding.Deadline, cutoff, err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("sticky final-delivery guard exceeded the request budget")
			}
		})
	}
}

// The historical audit name is retained for frozen aggregate references.
// Reuse keeps the binding and fingerprint, then freshly revalidates a consumed
// network proof without fetching another vendor batch.
func TestAuditStickyPrivateAPIBindingReusesWithoutAnotherFetch(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	proxy := startFakeProxy(t)
	var fetches atomic.Int32
	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fetches.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 200, "data": map[string]any{"count": 1,
				"proxies": []string{fmt.Sprintf("127.0.0.1:%d", portOf(proxy.URL))}},
		})
	}))
	t.Cleanup(vendor.Close)
	e.ingest.ScdnEndpoint, e.ingest.ScdnHTTPClient = vendor.URL, vendor.Client()
	if err := e.ingest.EnsureSource(context.Background(), "audit-sticky-private-api", application.SourceConfig{
		MaxPoolLeases: 1, Provider: &application.ProviderConfig{Protocol: "http", FetchCount: 1},
	}, true); err != nil {
		t.Fatal(err)
	}
	srv := auditServer(t, e)
	body := `{"count":1,"mode":"sticky","session_id":"audit-private-api","source_ids":["audit-sticky-private-api"],"cd":600}`
	var first application.AcquireResult
	for call := 0; call < 2; call++ {
		status, data := auditPost(t, srv, e.cfg.APIToken, "/api/v1/proxies/acquire", body)
		var result application.AcquireResult
		if status != http.StatusOK || json.Unmarshal(data, &result) != nil || result.Delivered != 1 || len(result.Items) != 1 {
			t.Fatalf("private API sticky call %d did not deliver: status=%d body=%s fetches=%d", call, status, data, fetches.Load())
		}
		if call == 0 {
			first = result
		} else if result.BindingVersion != first.BindingVersion ||
			e.acquire.FingerprintOf(result.Items[0].Conn) != e.acquire.FingerprintOf(first.Items[0].Conn) ||
			result.SessionChanged || result.Items[0].SessionChanged ||
			result.Items[0].Validation.CacheHit ||
			!result.Items[0].Validation.CompletedAt.After(first.Items[0].Validation.CompletedAt) {
			t.Fatalf("same-session private reuse changed binding or did not freshly revalidate: first=%+v next=%+v", first, result)
		}
	}
	if fetches.Load() != 1 {
		t.Fatalf("same-session reuse bought another vendor batch: fetches=%d", fetches.Load())
	}
	stats, err := e.store.Stats(context.Background())
	if err != nil || stats.Resources != 0 || stats.EnabledOffers != 0 {
		t.Fatalf("private API reuse created public inventory: stats=%+v err=%v", stats, err)
	}
}

func TestAuditStickyExpiredProofRevalidatesPrivateBindingWithinCD(t *testing.T) {
	e := newEnv(t, startJudge(t), func(c *config.Config) {
		c.MaxProofAgeMS = 500
		c.EvidenceCacheTTLMS = 500
	})
	proxy := startFakeProxy(t)
	var fetches atomic.Int32
	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fetches.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 200, "data": map[string]any{"count": 1,
				"proxies": []string{fmt.Sprintf("127.0.0.1:%d", portOf(proxy.URL))}},
		})
	}))
	t.Cleanup(vendor.Close)
	e.ingest.ScdnEndpoint, e.ingest.ScdnHTTPClient = vendor.URL, vendor.Client()
	const sourceID = "audit-private-revalidation-api"
	if err := e.ingest.EnsureSource(context.Background(), sourceID, application.SourceConfig{
		MaxPoolLeases: 1, Provider: &application.ProviderConfig{Protocol: "http", FetchCount: 1},
	}, true); err != nil {
		t.Fatal(err)
	}
	srv := auditServer(t, e)
	body := `{"count":1,"mode":"sticky","session_id":"audit-proof-refresh","source_ids":["audit-private-revalidation-api"],"timeout":2,"cd":600}`
	status, data := auditPost(t, srv, e.cfg.APIToken, "/api/v1/proxies/acquire", body)
	var first application.AcquireResult
	if status != http.StatusOK || json.Unmarshal(data, &first) != nil || first.Delivered != 1 || len(first.Items) != 1 {
		t.Fatalf("initial private binding did not deliver: status=%d body=%s", status, data)
	}
	ctx := context.Background()
	originalBinding, err := e.store.GetBinding(ctx, application.DefaultAcquireProject, "audit-proof-refresh")
	if err != nil {
		t.Fatal(err)
	}
	fingerprint := e.acquire.FingerprintOf(first.Items[0].Conn)
	before, err := e.store.ProjectCooldownBlocked(ctx, application.DefaultAcquireProject,
		time.Now(), []string{fingerprint})
	if err != nil || before[fingerprint].IsZero() {
		t.Fatalf("initial delivery did not establish project CD: windows=%+v err=%v", before, err)
	}
	time.Sleep(time.Until(first.Items[0].Validation.CompletedAt.Add(e.cfg.MaxProofAge() + 100*time.Millisecond)))
	status, data = auditPost(t, srv, e.cfg.APIToken, "/api/v1/proxies/acquire", body)
	var refreshed application.AcquireResult
	if status != http.StatusOK || json.Unmarshal(data, &refreshed) != nil || refreshed.Delivered != 1 || len(refreshed.Items) != 1 {
		t.Fatalf("proof expiry prevented fresh validation of the healthy existing binding within CD: status=%d body=%s fetches=%d", status, data, fetches.Load())
	}
	if e.acquire.FingerprintOf(refreshed.Items[0].Conn) != fingerprint ||
		refreshed.Items[0].Validation.CacheHit ||
		refreshed.SessionChanged || refreshed.Items[0].SessionChanged ||
		!refreshed.Items[0].Validation.CompletedAt.After(first.Items[0].Validation.CompletedAt) {
		t.Fatalf("expired proof was not replaced by a fresh probe of the same connection: first=%+v refreshed=%+v", first, refreshed)
	}
	if fetches.Load() != 1 {
		t.Fatalf("proof refresh fetched a new API batch instead of probing the private binding: fetches=%d", fetches.Load())
	}
	after, err := e.store.ProjectCooldownBlocked(ctx, application.DefaultAcquireProject,
		time.Now(), []string{fingerprint})
	if err != nil || !after[fingerprint].Equal(before[fingerprint]) {
		t.Fatalf("revalidation extended or erased the original project CD: before=%+v after=%+v err=%v", before, after, err)
	}
	currentBinding, err := e.store.GetBinding(ctx, application.DefaultAcquireProject, "audit-proof-refresh")
	if err != nil || !currentBinding.Deadline.Equal(originalBinding.Deadline) {
		t.Fatalf("proof refresh extended the original binding lifetime: before=%s after=%s err=%v", originalBinding.Deadline, currentBinding.Deadline, err)
	}
	stats, err := e.store.Stats(ctx)
	if err != nil || stats.Resources != 0 || stats.EnabledOffers != 0 {
		t.Fatalf("private proof refresh created public inventory: stats=%+v err=%v", stats, err)
	}
}

func TestAuditStickyProbeSaturationDoesNotDestroyPrivateBinding(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		name := "probe_budget"
		if canceled {
			name = "caller_cancellation"
		}
		t.Run(name, func(t *testing.T) {
			j := startJudge(t)
			e := newEnv(t, j, func(c *config.Config) {
				c.MaxProofAgeMS = 200
				c.EvidenceCacheTTLMS = 200
				c.ProbeTotalTimeoutMS = 1000
			})
			proxy := startFakeProxy(t)
			var fetches atomic.Int32
			vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				fetches.Add(1)
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"code": 200, "data": map[string]any{"count": 1,
						"proxies": []string{fmt.Sprintf("127.0.0.1:%d", portOf(proxy.URL))}},
				})
			}))
			t.Cleanup(vendor.Close)
			e.ingest.ScdnEndpoint, e.ingest.ScdnHTTPClient = vendor.URL, vendor.Client()
			const sourceID = "audit-saturated-private-api"
			if err := e.ingest.EnsureSource(context.Background(), sourceID, application.SourceConfig{
				MaxPoolLeases: 1, Provider: &application.ProviderConfig{Protocol: "http", FetchCount: 1},
			}, true); err != nil {
				t.Fatal(err)
			}
			slots := make(chan struct{}, 1)
			e.acquire = application.NewAcquireService(e.store, e.cfg, j.url(), j.certPool(), slots, nil)
			e.acquire.Refiller = e.ingest
			cd := int64(600)
			req := application.AcquireRequest{
				Count: 1, ProjectID: application.DefaultAcquireProject,
				ConsumerID: application.DefaultAcquireProject, CDSeconds: &cd,
				DeadlineMS: 2000, SessionID: "audit-saturated-private-session",
				Lease:   application.LeasePolicy{Mode: "sticky"},
				Filters: application.Filters{SourceIDs: []string{sourceID}},
			}
			first, derr := e.acquire.Acquire(context.Background(), req)
			if derr != nil || first.Delivered != 1 || len(first.Items) != 1 {
				t.Fatalf("initial private binding did not deliver: result=%+v err=%v", first, derr)
			}
			ctx := context.Background()
			before, err := e.store.GetBinding(ctx, req.ProjectID, req.SessionID)
			if err != nil || before.State != sqlite.BindingActive {
				t.Fatalf("initial private binding was not ACTIVE: binding=%+v err=%v", before, err)
			}
			cdBefore, err := e.store.ProjectCooldownBlocked(ctx, req.ProjectID, time.Now(), []string{before.Fingerprint})
			if err != nil || cdBefore[before.Fingerprint].IsZero() {
				t.Fatalf("initial project CD was missing: windows=%+v err=%v", cdBefore, err)
			}
			type circuitSnapshot struct {
				State, Outcome, Cooldown, Success, Failure, Updated string
				Failures, Opens                                     int
				Exists                                              bool
			}
			readCircuit := func() circuitSnapshot {
				t.Helper()
				var snapshot circuitSnapshot
				if err := e.store.DB().QueryRowContext(ctx, `
					SELECT state, consecutive_failures, open_count, last_outcome,
					       COALESCE(cooldown_until, ''), COALESCE(last_success_at, ''),
					       COALESCE(last_failure_at, ''), updated_at
					FROM source_circuits WHERE source_id = ?`, sourceID).Scan(
					&snapshot.State, &snapshot.Failures, &snapshot.Opens, &snapshot.Outcome,
					&snapshot.Cooldown, &snapshot.Success, &snapshot.Failure, &snapshot.Updated); err != nil {
					if errors.Is(err, sql.ErrNoRows) {
						return snapshot
					}
					t.Fatal(err)
				}
				snapshot.Exists = true
				return snapshot
			}
			circuitBefore := readCircuit()
			time.Sleep(time.Until(first.Items[0].Validation.CompletedAt.Add(e.cfg.MaxProofAge() + 100*time.Millisecond)))
			slots <- struct{}{}
			probeCtx := ctx
			cancel := func() {}
			if canceled {
				probeCtx, cancel = context.WithTimeout(ctx, 300*time.Millisecond)
			}
			started := time.Now()
			result, derr := e.acquire.Acquire(probeCtx, req)
			elapsed := time.Since(started)
			cancel()
			<-slots
			if derr != nil || result.Delivered != 0 || result.Missing != 1 || result.ShortfallReason == "" {
				t.Errorf("probe saturation must return an explained empty result, not a storage failure: result=%+v err=%v", result, derr)
			}
			if elapsed > 2500*time.Millisecond {
				t.Errorf("saturated proof refresh exceeded the caller budget: elapsed=%s", elapsed)
			}
			after, err := e.store.GetBinding(ctx, req.ProjectID, req.SessionID)
			if err != nil || after.State != sqlite.BindingActive || after.Version != before.Version ||
				after.Fingerprint != before.Fingerprint || after.ConnectionCiphertext != before.ConnectionCiphertext ||
				!after.Deadline.Equal(before.Deadline) || !after.Validation.CompletedAt.Equal(before.Validation.CompletedAt) {
				t.Fatalf("inconclusive refresh destroyed the recoverable private binding: before=%+v after=%+v err=%v", before, after, err)
			}
			circuitAfter := readCircuit()
			if circuitAfter != circuitBefore {
				t.Fatalf("inconclusive refresh mutated source health: before=%+v after=%+v", circuitBefore, circuitAfter)
			}
			cdAfter, err := e.store.ProjectCooldownBlocked(ctx, req.ProjectID, time.Now(), []string{before.Fingerprint})
			if err != nil || !cdAfter[before.Fingerprint].Equal(cdBefore[before.Fingerprint]) {
				t.Fatalf("inconclusive refresh changed the committed project CD: before=%+v after=%+v err=%v", cdBefore, cdAfter, err)
			}
			recovered, derr := e.acquire.Acquire(ctx, req)
			if derr != nil || recovered.Delivered != 1 || len(recovered.Items) != 1 ||
				recovered.BindingVersion != first.BindingVersion || recovered.SessionChanged ||
				e.acquire.FingerprintOf(recovered.Items[0].Conn) != before.Fingerprint ||
				!recovered.Items[0].Validation.CompletedAt.After(before.Validation.CompletedAt) {
				t.Fatalf("private binding did not recover after probe slots became available: result=%+v err=%v", recovered, derr)
			}
			if fetches.Load() != 1 {
				t.Fatalf("saturation or recovery fetched another vendor batch: calls=%d", fetches.Load())
			}
			stats, err := e.store.Stats(ctx)
			if err != nil || stats.Resources != 0 || stats.EnabledOffers != 0 || stats.ActiveReservations != 0 {
				t.Fatalf("private saturation/recovery leaked public inventory or reservations: stats=%+v err=%v", stats, err)
			}
		})
	}
}

type auditStickyRefreshGeoGate struct {
	armed   atomic.Bool
	lookups atomic.Int32
	arrived chan struct{}
	release chan struct{}
}

func (g *auditStickyRefreshGeoGate) Lookup(netip.Addr) (geoip.Location, bool) {
	if g.armed.Load() && g.lookups.Add(1) == 1 {
		close(g.arrived)
		<-g.release
	}
	return geoip.Location{CountryCode: "US"}, true
}

func TestAuditStickyLateRefreshCannotReplaceNewerPrivateProof(t *testing.T) {
	j := startJudge(t)
	e := newEnv(t, j, func(c *config.Config) {
		c.MaxProofAgeMS = 500
		c.EvidenceCacheTTLMS = 500
	})
	gate := &auditStickyRefreshGeoGate{arrived: make(chan struct{}), release: make(chan struct{})}
	var released sync.Once
	unblock := func() { released.Do(func() { close(gate.release) }) }
	t.Cleanup(unblock)
	e.acquire = application.NewAcquireService(e.store, e.cfg, e.cfg.JudgeURL,
		j.certPool(), make(chan struct{}, e.cfg.ProbeSlots), gate)
	e.acquire.Refiller = e.ingest
	proxy := startFakeProxy(t)
	var fetches atomic.Int32
	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fetches.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 200, "data": map[string]any{"count": 1,
				"proxies": []string{fmt.Sprintf("127.0.0.1:%d", portOf(proxy.URL))}},
		})
	}))
	t.Cleanup(vendor.Close)
	e.ingest.ScdnEndpoint, e.ingest.ScdnHTTPClient = vendor.URL, vendor.Client()
	const sourceID = "audit-private-refresh-cas"
	if _, err := e.ingest.CreateSource(context.Background(), sourceID, application.SourceConfig{
		MaxPoolLeases: 1, Provider: &application.ProviderConfig{Protocol: "http", FetchCount: 1},
	}, true); err != nil {
		t.Fatal(err)
	}
	cd := int64(600)
	req := application.AcquireRequest{
		Count: 1, ProjectID: application.DefaultAcquireProject, ConsumerID: application.DefaultAcquireProject,
		DeadlineMS: 4000, CDSeconds: &cd, SessionID: "audit-refresh-cas",
		Lease:   application.LeasePolicy{Mode: "sticky"},
		Filters: application.Filters{SourceIDs: []string{sourceID}, Countries: []string{"US"}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	first, err := e.acquire.Acquire(ctx, req)
	if err != nil || first.Delivered != 1 || len(first.Items) != 1 {
		t.Fatalf("initial private country binding failed: result=%+v err=%v", first, err)
	}
	time.Sleep(time.Until(first.Items[0].Validation.CompletedAt.Add(e.cfg.MaxProofAge() + 100*time.Millisecond)))
	gate.armed.Store(true)
	type answer struct {
		result application.AcquireResult
		err    *domain.Error
	}
	done := make(chan answer, 1)
	go func() {
		result, err := e.acquire.Acquire(ctx, req)
		done <- answer{result, err}
	}()
	select {
	case <-gate.arrived:
	case <-time.After(3 * time.Second):
		t.Fatal("older refresh did not reach its post-probe country lookup")
	}
	// The first proof is complete but not yet committed. Let a genuinely
	// newer successful refresh commit before the old continuation resumes.
	newer, err := e.acquire.Acquire(ctx, req)
	if err != nil || newer.Delivered != 1 || len(newer.Items) != 1 {
		t.Fatalf("newer private refresh failed: result=%+v err=%v", newer, err)
	}
	unblock()
	select {
	case older := <-done:
		if older.err != nil || older.result.Delivered != 1 || len(older.result.Items) != 1 || older.result.SessionChanged {
			t.Fatalf("late refresh destroyed a newer usable binding instead of reusing it: result=%+v err=%v fetches=%d", older.result, older.err, fetches.Load())
		}
	case <-time.After(4 * time.Second):
		t.Fatal("late refresh did not finish within its request budget")
	}
	binding, bindingErr := e.store.GetBinding(context.Background(), application.DefaultAcquireProject, req.SessionID)
	if bindingErr != nil || binding.State != sqlite.BindingActive ||
		binding.Validation.CompletedAt.Before(newer.Items[0].Validation.CompletedAt) ||
		binding.Fingerprint != e.acquire.FingerprintOf(first.Items[0].Conn) || fetches.Load() != 1 {
		t.Fatalf("old refresh replaced or expired the newer private proof: binding=%+v newer=%+v fetches=%d err=%v", binding, newer, fetches.Load(), bindingErr)
	}
}

func auditStickyFinalGuardFixture(t *testing.T) (*env, *httptest.Server, domain.StickyBinding, application.AcquiredItem) {
	t.Helper()
	e := newEnv(t, startJudge(t), nil)
	e.importProxy(t, "audit-sticky-final-guard", startFakeProxy(t).URL)
	srv := auditServer(t, e)
	status, data := auditPost(t, srv, e.cfg.APIToken, "/api/v1/proxies/acquire",
		`{"count":1,"mode":"sticky","session_id":"audit-final-guard","cd":600}`)
	var first application.AcquireResult
	if status != http.StatusOK || json.Unmarshal(data, &first) != nil || first.Delivered != 1 || len(first.Items) != 1 {
		t.Fatalf("final-guard initial binding: status=%d body=%s", status, data)
	}
	b, err := e.store.GetBinding(context.Background(), application.DefaultAcquireProject, "audit-final-guard")
	if err != nil {
		t.Fatal(err)
	}
	lease := auditStickyLeaseIdentity(t, e, first, "audit-final-guard")
	first.Items[0].ResourceID = lease.Lease.ResourceID
	proofJSON, err := json.Marshal(b.Validation)
	if err != nil {
		t.Fatal(err)
	}
	scope := domain.ValidationScope{Mode: b.Validation.Mode, TargetHostPort: b.Validation.Target,
		Profile: b.Validation.Profile, SecurityHash: b.Validation.SecurityHash}
	// Seed the actual probe result to isolate reuse guards from the separate
	// evidence-write regression; no synthetic PASS or altered proof is used.
	if _, err := e.store.DB().Exec(`INSERT OR REPLACE INTO validation_evidence
		(connection_fingerprint, scope_hash, source_id, source_config_version,
		 incarnation, evidence_json, outcome, completed_at) VALUES(?,?,?,?,?,?,?,?)`,
		b.Fingerprint, domain.EvidenceScopeHash(scope), b.SourceID, b.SourceConfigVersion,
		b.Incarnation, string(proofJSON), b.Validation.Outcome,
		b.Validation.CompletedAt.UTC().Format("2006-01-02T15:04:05.000000000Z07:00")); err != nil {
		t.Fatal(err)
	}
	return e, srv, b, first.Items[0]
}

func TestAuditStickyReuseRechecksCurrentRetention(t *testing.T) {
	for _, expired := range []bool{false, true} {
		t.Run(fmt.Sprintf("expired-%t", expired), func(t *testing.T) {
			e, srv, b, _ := auditStickyFinalGuardFixture(t)
			retention := time.Now().UTC().Add(15 * time.Second)
			if expired {
				retention = time.Now().UTC().Add(-time.Second)
			}
			if _, err := e.store.DB().Exec(`UPDATE source_offers SET local_retention_deadline = ?
				WHERE resource_id = ? AND source_id = ?`,
				retention.Format("2006-01-02T15:04:05.000000000Z07:00"), b.ResourceID, b.SourceID); err != nil {
				t.Fatal(err)
			}
			status, data := auditPost(t, srv, e.cfg.APIToken, "/api/v1/proxies/acquire",
				`{"count":1,"mode":"sticky","session_id":"audit-final-guard","cd":600}`)
			var result application.AcquireResult
			if status != http.StatusOK || json.Unmarshal(data, &result) != nil {
				t.Fatalf("retention eligibility change became a storage outage: status=%d body=%s", status, data)
			}
			if expired {
				if result.Delivered != 0 || len(result.Items) != 0 {
					t.Fatalf("sticky reuse delivered inventory past its current retention: %s", data)
				}
				return
			}
			if result.Delivered != 1 || len(result.Items) != 1 || result.Items[0].ExpiresAt.After(retention) {
				t.Fatalf("sticky reuse ignored a shorter but usable current retention: retention=%s body=%s", retention, data)
			}
		})
	}
}

func TestAuditStickyReuseUsesActualCommitTime(t *testing.T) {
	e, _, b, item := auditStickyFinalGuardFixture(t)
	beforeWait := time.Now().UTC()
	deadline := beforeWait.Add(50 * time.Millisecond)
	if _, err := e.store.DB().Exec(`UPDATE sticky_bindings SET deadline = ?
		WHERE project_id = ? AND session_id = ?`,
		deadline.Format("2006-01-02T15:04:05.000000000Z07:00"), b.ProjectID, b.SessionID); err != nil {
		t.Fatal(err)
	}
	connJSON, err := json.Marshal(item.Conn)
	if err != nil {
		t.Fatal(err)
	}
	scope := domain.ValidationScope{Mode: b.Validation.Mode, TargetHostPort: b.Validation.Target,
		Profile: b.Validation.Profile, SecurityHash: b.Validation.SecurityHash}
	// A transaction can begin after the application's precheck clock snapshot.
	time.Sleep(80 * time.Millisecond)
	_, err = e.store.CommitStickyReuse(context.Background(), sqlite.StickyReuseArgs{
		ProjectID: b.ProjectID, SessionID: b.SessionID, ExpectedVersion: b.Version,
		ConsumerID: application.DefaultAcquireProject, Fingerprint: b.Fingerprint,
		SourceID: b.SourceID, SourceConfigVersion: b.SourceConfigVersion,
		ResourceID: b.ResourceID, Incarnation: b.Incarnation,
		ScopeHash: domain.EvidenceScopeHash(scope), Proof: b.Validation,
		MaxProofAge: e.cfg.MaxProofAge(), RequestID: "req_audit_stale_commit_clock",
		LeaseID: "lease_audit_stale_commit_clock", Mode: "sticky",
		LeaseDeadline: deadline, ConnJSON: string(connJSON), Now: beforeWait,
		BindingCiphertext: b.ConnectionCiphertext, DeliverySafety: e.cfg.ExpirySafetyMargin(),
	})
	if err == nil {
		t.Fatal("sticky reuse committed an already-expired binding using the pre-wait clock snapshot")
	}
	var leases int
	if err := e.store.DB().QueryRow(`SELECT COUNT(*) FROM leases WHERE lease_id = ?`,
		"lease_audit_stale_commit_clock").Scan(&leases); err != nil || leases != 0 {
		t.Fatalf("rejected sticky reuse left a lease behind: count=%d err=%v", leases, err)
	}
}

func TestAuditStickyReuseKeepsInventoryResourceIdentity(t *testing.T) {
	e, srv, b, original := auditStickyFinalGuardFixture(t)
	status, data := auditPost(t, srv, e.cfg.APIToken, "/api/v1/proxies/acquire",
		`{"count":1,"mode":"sticky","session_id":"audit-final-guard","cd":600}`)
	var result application.AcquireResult
	if status != http.StatusOK || json.Unmarshal(data, &result) != nil || result.Delivered != 1 || len(result.Items) != 1 {
		t.Fatalf("inventory sticky reuse did not deliver: status=%d body=%s", status, data)
	}
	row := auditStickyLeaseIdentity(t, e, result, "audit-final-guard")
	if original.ResourceID != b.ResourceID || row.Lease.ResourceID != original.ResourceID {
		t.Fatalf("reuse replaced the real inventory identity with the CD fingerprint: inventory=%s original=%s lease=%s fingerprint=%s",
			b.ResourceID, original.ResourceID, row.Lease.ResourceID, b.Fingerprint)
	}
}

func TestAuditStickyCommitCannotOutliveCurrentRetention(t *testing.T) {
	e, _, b, item := auditStickyFinalGuardFixture(t)
	retention := time.Now().UTC().Add(15 * time.Second)
	if _, err := e.store.DB().Exec(`UPDATE source_offers SET local_retention_deadline = ?
		WHERE resource_id = ? AND source_id = ?`,
		retention.Format("2006-01-02T15:04:05.000000000Z07:00"), b.ResourceID, b.SourceID); err != nil {
		t.Fatal(err)
	}
	connJSON, err := json.Marshal(item.Conn)
	if err != nil {
		t.Fatal(err)
	}
	scope := domain.ValidationScope{Mode: b.Validation.Mode, TargetHostPort: b.Validation.Target,
		Profile: b.Validation.Profile, SecurityHash: b.Validation.SecurityHash}
	committedDeadline, err := e.store.CommitStickyReuse(context.Background(), sqlite.StickyReuseArgs{
		ProjectID: b.ProjectID, SessionID: b.SessionID, ExpectedVersion: b.Version,
		ConsumerID: application.DefaultAcquireProject, Fingerprint: b.Fingerprint,
		SourceID: b.SourceID, SourceConfigVersion: b.SourceConfigVersion,
		ResourceID: b.ResourceID, Incarnation: b.Incarnation,
		ScopeHash: domain.EvidenceScopeHash(scope), Proof: b.Validation,
		MaxProofAge: e.cfg.MaxProofAge(), RequestID: "req_audit_retention_commit",
		LeaseID: "lease_audit_retention_commit", Mode: "sticky",
		LeaseDeadline: b.Deadline, ConnJSON: string(connJSON), Now: time.Now().UTC(),
		MinUsable: e.cfg.MinUsable(), BindingCiphertext: b.ConnectionCiphertext,
		DeliverySafety: e.cfg.ExpirySafetyMargin(),
	})
	if err != nil {
		if !errors.Is(err, sqlite.ErrNotEligible) && !errors.Is(err, sqlite.ErrStale) {
			t.Fatalf("retention guard returned a storage error: %v", err)
		}
		return
	}
	row, err := e.store.GetLease(context.Background(), "lease_audit_retention_commit")
	if err != nil || row.Lease.Deadline.After(retention) || !committedDeadline.Equal(row.Lease.Deadline) {
		t.Fatalf("commit ignored retention or returned another deadline: retention=%s returned=%s lease=%+v err=%v", retention, committedDeadline, row.Lease, err)
	}
}

func TestAuditStickyMatchingVerifiedCountryCanReuseOwnBinding(t *testing.T) {
	for _, kind := range []string{"inventory", "api"} {
		t.Run(kind, func(t *testing.T) {
			j := startJudge(t)
			e := newEnv(t, j, nil)
			proxy := startFakeProxy(t)
			var fetches atomic.Int32
			if kind == "inventory" {
				e.importProxy(t, "audit-sticky-country", proxy.URL)
			} else {
				vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					fetches.Add(1)
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(map[string]any{
						"code": 200, "data": map[string]any{"count": 1,
							"proxies": []string{fmt.Sprintf("127.0.0.1:%d", portOf(proxy.URL))}},
					})
				}))
				t.Cleanup(vendor.Close)
				e.ingest.ScdnEndpoint, e.ingest.ScdnHTTPClient = vendor.URL, vendor.Client()
				if err := e.ingest.EnsureSource(context.Background(), "audit-sticky-country", application.SourceConfig{
					MaxPoolLeases: 1, Provider: &application.ProviderConfig{Protocol: "http", FetchCount: 1},
				}, true); err != nil {
					t.Fatal(err)
				}
			}
			e.acquire = application.NewAcquireService(e.store, e.cfg, j.url(), j.certPool(),
				make(chan struct{}, e.cfg.ProbeSlots), fakeGeo{loc: geoip.Location{CountryCode: "US"}})
			e.acquire.Refiller = e.ingest
			srv := auditServer(t, e)
			body := `{"count":1,"mode":"sticky","session_id":"audit-country-reuse","country":["US"],"cd":600}`
			var first application.AcquireResult
			for call := 0; call < 2; call++ {
				status, data := auditPost(t, srv, e.cfg.APIToken, "/api/v1/proxies/acquire", body)
				var result application.AcquireResult
				if status != http.StatusOK || json.Unmarshal(data, &result) != nil || result.Delivered != 1 || len(result.Items) != 1 {
					t.Fatalf("matching verified country blocked same-session call %d: status=%d body=%s", call, status, data)
				}
				if call == 0 {
					if result.Items[0].Observed == nil || result.Items[0].Observed.CountryCode != "US" {
						t.Fatalf("fixture did not freshly verify the requested country: %+v", result)
					}
					first = result
				} else {
					if result.BindingVersion != first.BindingVersion ||
						e.acquire.FingerprintOf(result.Items[0].Conn) != e.acquire.FingerprintOf(first.Items[0].Conn) ||
						result.SessionChanged || result.Items[0].SessionChanged {
						t.Fatalf("matching fresh country forced a new allocation instead of own-binding reuse: first=%+v next=%+v", first, result)
					}
					if kind == "inventory" {
						if !result.Items[0].Validation.CacheHit {
							t.Fatalf("inventory binding did not reuse its current proof: first=%+v next=%+v", first, result)
						}
					} else if result.Items[0].Validation.CacheHit ||
						!result.Items[0].Validation.CompletedAt.After(first.Items[0].Validation.CompletedAt) {
						t.Fatalf("consumed network proof was not freshly revalidated: first=%+v next=%+v", first, result)
					}
				}
			}
			if kind == "api" {
				stats, err := e.store.Stats(context.Background())
				if err != nil || stats.Resources != 0 || stats.EnabledOffers != 0 || fetches.Load() != 1 {
					t.Fatalf("country reuse bought new API supply or created public inventory: stats=%+v fetches=%d err=%v", stats, fetches.Load(), err)
				}
			}
		})
	}
}

func TestAuditStickyReuseRechecksProviderExpiry(t *testing.T) {
	for _, expired := range []bool{false, true} {
		t.Run(fmt.Sprintf("expired-%t", expired), func(t *testing.T) {
			e, srv, b, _ := auditStickyFinalGuardFixture(t)
			expiry := time.Now().UTC().Add(15 * time.Second)
			if expired {
				expiry = time.Now().UTC().Add(-time.Second)
			}
			if _, err := e.store.DB().Exec(`UPDATE source_offers SET provider_expires_at = ?
				WHERE resource_id = ? AND source_id = ?`,
				expiry.Format("2006-01-02T15:04:05.000000000Z07:00"), b.ResourceID, b.SourceID); err != nil {
				t.Fatal(err)
			}
			status, data := auditPost(t, srv, e.cfg.APIToken, "/api/v1/proxies/acquire",
				`{"count":1,"mode":"sticky","session_id":"audit-final-guard","cd":600}`)
			var result application.AcquireResult
			if status != http.StatusOK || json.Unmarshal(data, &result) != nil {
				t.Fatalf("provider expiry change became a storage outage: status=%d body=%s", status, data)
			}
			if expired {
				if result.Delivered != 0 || len(result.Items) != 0 {
					t.Fatalf("sticky reuse delivered an explicitly expired provider offer: %s", data)
				}
				return
			}
			cutoff := expiry.Add(-e.cfg.ExpirySafetyMargin())
			if result.Delivered != 1 || len(result.Items) != 1 || result.Items[0].ExpiresAt.After(cutoff) {
				t.Fatalf("sticky reuse ignored current provider expiry and safety margin: cutoff=%s body=%s", cutoff, data)
			}
		})
	}
}

func TestAuditStickyCommitCannotOutliveProviderExpiry(t *testing.T) {
	e, _, b, item := auditStickyFinalGuardFixture(t)
	expiry := time.Now().UTC().Add(15 * time.Second)
	if _, err := e.store.DB().Exec(`UPDATE source_offers SET provider_expires_at = ?
		WHERE resource_id = ? AND source_id = ?`,
		expiry.Format("2006-01-02T15:04:05.000000000Z07:00"), b.ResourceID, b.SourceID); err != nil {
		t.Fatal(err)
	}
	connJSON, err := json.Marshal(item.Conn)
	if err != nil {
		t.Fatal(err)
	}
	scope := domain.ValidationScope{Mode: b.Validation.Mode, TargetHostPort: b.Validation.Target,
		Profile: b.Validation.Profile, SecurityHash: b.Validation.SecurityHash}
	committedDeadline, err := e.store.CommitStickyReuse(context.Background(), sqlite.StickyReuseArgs{
		ProjectID: b.ProjectID, SessionID: b.SessionID, ExpectedVersion: b.Version,
		ConsumerID: application.DefaultAcquireProject, Fingerprint: b.Fingerprint,
		SourceID: b.SourceID, SourceConfigVersion: b.SourceConfigVersion,
		ResourceID: b.ResourceID, Incarnation: b.Incarnation,
		ScopeHash: domain.EvidenceScopeHash(scope), Proof: b.Validation,
		MaxProofAge: e.cfg.MaxProofAge(), RequestID: "req_audit_provider_expiry_commit",
		LeaseID: "lease_audit_provider_expiry_commit", Mode: "sticky",
		LeaseDeadline: b.Deadline, ConnJSON: string(connJSON), Now: time.Now().UTC(),
		MinUsable: e.cfg.MinUsable(), BindingCiphertext: b.ConnectionCiphertext,
		DeliverySafety: e.cfg.ExpirySafetyMargin(),
	})
	if err != nil {
		if !errors.Is(err, sqlite.ErrNotEligible) && !errors.Is(err, sqlite.ErrStale) {
			t.Fatalf("provider expiry guard returned a storage error: %v", err)
		}
		return
	}
	row, err := e.store.GetLease(context.Background(), "lease_audit_provider_expiry_commit")
	cutoff := expiry.Add(-e.cfg.ExpirySafetyMargin())
	if err != nil || row.Lease.Deadline.After(cutoff) || !committedDeadline.Equal(row.Lease.Deadline) {
		t.Fatalf("commit ignored provider expiry/safety or returned another deadline: cutoff=%s returned=%s lease=%+v err=%v", cutoff, committedDeadline, row.Lease, err)
	}
}
