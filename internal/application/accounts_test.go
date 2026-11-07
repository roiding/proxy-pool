package application

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"proxypool/internal/domain"
)

const sourceSecretTestKey = "test-account-encryption-master-key-32-bytes"

func accountTestConfig() SourceConfig {
	return SourceConfig{Account: &AccountConfig{VendorID: "webshare", Protocol: "http", Mode: "direct"}, Access: &SourceAccessConfig{ProjectIDs: []string{"owner"}}}
}

func createAccountFixture(t *testing.T, s *IngestService, id, token string) domain.Source {
	t.Helper()
	if err := s.ConfigureSourceSecrets(sourceSecretTestKey); err != nil {
		t.Fatal(err)
	}
	src, derr := s.CreateSource(context.Background(), id, accountTestConfig(), false)
	if derr != nil {
		t.Fatal(derr)
	}
	status, derr := s.SetSourceSecret(context.Background(), id, src.ConfigVersion, token)
	if derr != nil {
		t.Fatal(derr)
	}
	src, derr = s.SetSourceEnabled(context.Background(), id, true, status.ConfigVersion)
	if derr != nil {
		t.Fatal(derr)
	}
	return src
}

func TestAccountRequiresPrivateAccessAndSeparateEncryptedSecret(t *testing.T) {
	s := newTestIngest(t)
	cfg := accountTestConfig()
	cfg.Access = nil
	if _, err := s.CreateSource(context.Background(), "account-public", cfg, false); err == nil {
		t.Fatal("public account source accepted")
	}
	cfg.Access = &SourceAccessConfig{}
	if _, err := s.CreateSource(context.Background(), "account-null", cfg, false); err == nil {
		t.Fatal("null account access accepted")
	}
	if _, err := s.CreateSource(context.Background(), "account-enabled", accountTestConfig(), true); err == nil {
		t.Fatal("unconfigured account enabled at create")
	}
	src, err := s.CreateSource(context.Background(), "account-private", accountTestConfig(), false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetSourceSecret(context.Background(), src.ID, src.ConfigVersion, "account-api-key"); err == nil || err.Code != "SECRET_ENCRYPTION_NOT_CONFIGURED" {
		t.Fatalf("plaintext persisted without master key: %v", err)
	}
	if err := s.ConfigureSourceSecrets(sourceSecretTestKey); err != nil {
		t.Fatal(err)
	}
	status, err := s.SetSourceSecret(context.Background(), src.ID, src.ConfigVersion, "account-api-key")
	if err != nil {
		t.Fatal(err)
	}
	if !status.Configured || status.SecretVersion != 1 || status.ConfigVersion <= src.ConfigVersion {
		t.Fatalf("rotation metadata: %+v", status)
	}
	record, err2 := s.store.SourceAccount(context.Background(), src.ID)
	if err2 != nil {
		t.Fatal(err2)
	}
	if strings.Contains(record.Ciphertext, "account-api-key") || !strings.HasPrefix(record.Ciphertext, "v1:") {
		t.Fatalf("secret was not sealed: %q", record.Ciphertext)
	}
	got, err := s.GetSource(context.Background(), src.ID)
	if err != nil || strings.Contains(got.ConfigJSON, "account-api-key") {
		t.Fatalf("source config leaked secret: %+v %v", got, err)
	}
	box := s.accountSecretBox()
	if _, err := box.open("other-source", record.SecretVersion, record.Ciphertext); err == nil {
		t.Fatal("ciphertext swap across sources was accepted")
	}
	if _, err := box.open(src.ID, record.SecretVersion+1, record.Ciphertext); err == nil {
		t.Fatal("ciphertext replay across versions was accepted")
	}
	if _, err := s.SetSourceSecret(context.Background(), src.ID, src.ConfigVersion, "replacement-api-key"); err == nil || err.Code != "SOURCE_VERSION_CONFLICT" {
		t.Fatalf("stale secret rotation accepted: %v", err)
	}
}

func TestAccountListIsDisposableAndRotationInvalidatesBatch(t *testing.T) {
	s := newTestIngest(t)
	src := createAccountFixture(t, s, "account-list", "account-api-key")
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.Header.Get("Authorization") != "Token account-api-key" {
			t.Error("wrong account API key")
		}
		_, _ = fmt.Fprint(w, `{"count":1,"next":null,"results":[{"id":"allocated-one","proxy_address":"127.0.0.1","port":12345,"username":"proxy-user","password":"proxy-pass","valid":true}]}`)
	}))
	defer server.Close()
	s.WebshareEndpoint, s.WebshareHTTPClient = server.URL, server.Client()
	if _, _, err := s.ScopedStaticBatch(context.Background(), src.ID, 1, RefillScope{ProjectID: "outsider"}); err == nil || hits.Load() != 0 {
		t.Fatalf("unauthorized project triggered account I/O: %v hits=%d", err, hits.Load())
	}
	result, derr := s.RefreshStaticSource(context.Background(), src.ID)
	if derr != nil {
		t.Fatal(derr)
	}
	if result.Retained != 1 || result.Fetched != 1 || !result.Complete {
		t.Fatalf("account batch: %+v", result)
	}
	candidates, batch, fresh := s.CachedStaticCandidates(src.ID)
	if !fresh || len(candidates) != 1 {
		t.Fatal("no account cache")
	}
	c := candidates[0]
	if !c.Ephemeral || c.Resource.Conn.Username != "proxy-user" || c.Offer.SourceRecordID != "allocated-one" || c.Offer.ProviderExpiresAt != nil || c.Offer.LocalRetentionDeadline != nil || c.SourceConfigVersion != src.ConfigVersion {
		t.Fatalf("account lifecycle corrupted: %+v", c)
	}
	stats, err := s.store.Stats(context.Background())
	if err != nil || stats.Resources != 0 || stats.EnabledOffers != 0 {
		t.Fatalf("account list persisted inventory: %+v %v", stats, err)
	}
	pass := domain.Evidence{Outcome: "PASS", CompletedAt: s.Now().Add(-time.Second)}
	if err := s.RecordStaticEvidence(context.Background(), c, "account-scope", pass, nil); err != nil {
		t.Fatal(err)
	}
	status, derr := s.SetSourceSecret(context.Background(), src.ID, src.ConfigVersion, "replacement-api-key")
	if derr != nil {
		t.Fatal(derr)
	}
	if s.StaticBatchCurrent(src.ID, batch) || status.SecretVersion != 2 || status.ConfigVersion <= src.ConfigVersion {
		t.Fatalf("rotation failed to retire old batch: %+v", status)
	}
	if _, _, ok := s.StaticRecentEvidence(c, "account-scope", s.Now().Add(-time.Minute)); ok {
		t.Fatal("rotated account proof was reused")
	}
	cleared, derr := s.SetSourceSecret(context.Background(), src.ID, status.ConfigVersion, "")
	if derr != nil || cleared.Configured {
		t.Fatalf("clear: %+v %v", cleared, derr)
	}
	restarted := NewIngestService(s.store, "test-secret", true)
	if err := restarted.ConfigureSourceSecrets(sourceSecretTestKey); err != nil {
		t.Fatal(err)
	}
	view, derr := restarted.SourceSecretStatus(context.Background(), src.ID)
	if derr != nil || view.Configured || view.SecretVersion != 3 {
		t.Fatalf("clear sentinel lost on restart: %+v %v", view, derr)
	}
	if _, derr := restarted.RefreshStaticSource(context.Background(), src.ID); derr == nil || derr.Code != "SOURCE_AUTH_FAILED" || hits.Load() != 1 {
		t.Fatalf("cleared account made I/O: %v hits=%d", derr, hits.Load())
	}
}

func TestAccountSharedCredentialRateLimitAndVendorRetryAfter(t *testing.T) {
	s := newTestIngest(t)
	first := createAccountFixture(t, s, "account-a", "same-account-api-key")
	second := createAccountFixture(t, s, "account-b", "same-account-api-key")
	now := time.Now()
	s.Now = func() time.Time { return now }
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(429)
		_, _ = fmt.Fprint(w, `{"detail":"private vendor text"}`)
	}))
	defer server.Close()
	// Keep the production controlled client so real non-200 headers traverse
	// the shared downloader before the account limiter persists them.
	s.WebshareEndpoint = server.URL
	if _, derr := s.RefreshStaticSource(context.Background(), first.ID); derr == nil || derr.Code != "SOURCE_RATE_LIMITED" {
		t.Fatalf("rate error: %v", derr)
	}
	state, err := s.store.SourceFetchState(context.Background(), first.ID)
	if err != nil || state.NextFetchAt == nil || state.NextFetchAt.Sub(now) < 120*time.Second {
		t.Fatalf("vendor retry-after not persisted: %+v %v", state, err)
	}
	if _, derr := s.RefreshStaticSource(context.Background(), second.ID); derr == nil || derr.Code != "SOURCE_RATE_LIMITED" || hits.Load() != 1 {
		t.Fatalf("same credential bypassed shared rate floor: %v hits=%d", derr, hits.Load())
	}
}

func TestAccountPageFailureKeepsOldSnapshotOnlyUntilItsHardTTL(t *testing.T) {
	s := newTestIngest(t)
	src := createAccountFixture(t, s, "account-pages", "account-api-key")
	cfg := accountTestConfig()
	cfg.Account.PageSize, cfg.Account.MaxPages = 1, 2
	if _, derr := s.UpdateSource(context.Background(), src.ID, cfg, true, src.ConfigVersion); derr != nil {
		t.Fatal(derr)
	}
	now := time.Now().UTC()
	s.Now = func() time.Time { return now }
	var failPage atomic.Bool
	var endpoint string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("page") == "1" {
			_, _ = fmt.Fprintf(w, `{"count":2,"next":%q,"results":[{"id":"one","proxy_address":"127.0.0.1","port":12345,"username":"u","password":"p","valid":true}]}`,
				endpoint+"/?mode=direct&page=2&page_size=1")
			return
		}
		if failPage.Load() {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		_, _ = fmt.Fprint(w, `{"count":2,"next":null,"results":[{"id":"two","proxy_address":"127.0.0.1","port":12346,"username":"u","password":"p","valid":true}]}`)
	}))
	defer server.Close()
	endpoint = server.URL
	s.WebshareEndpoint, s.WebshareHTTPClient = endpoint+"/", server.Client()
	first, derr := s.RefreshStaticSource(context.Background(), src.ID)
	if derr != nil || first.Retained != 2 || !first.Complete {
		t.Fatalf("full snapshot was not read: %+v %v", first, derr)
	}
	failPage.Store(true)
	now = now.Add(31 * time.Second)
	if res, derr := s.RefreshStaticSource(context.Background(), src.ID); derr != nil || !res.CacheHit {
		t.Fatal("a fresh account snapshot triggered a duplicate fetch")
	}
	old, batch, fresh := s.CachedStaticCandidates(src.ID)
	if !fresh || batch != first.BatchID || len(old) != 2 {
		t.Fatalf("page failure replaced the still-valid old snapshot: fresh=%v batch=%s count=%d", fresh, batch, len(old))
	}
	now = first.ExpiresAt.Add(time.Second)
	if _, derr := s.RefreshStaticSource(context.Background(), src.ID); derr == nil {
		t.Fatal("failed second page installed a partial account snapshot after expiry")
	}
	if _, _, fresh := s.CachedStaticCandidates(src.ID); fresh {
		t.Fatal("page failure extended the old snapshot's hard TTL")
	}
}

func TestAccountSecretRotationFencesAnInFlightRefresh(t *testing.T) {
	s := newTestIngest(t)
	src := createAccountFixture(t, s, "account-rotation-flight", "original-account-key")
	started, released := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(released) }) }
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-released
		_, _ = fmt.Fprint(w, `{"count":1,"next":null,"results":[{"id":"old-assignment","proxy_address":"127.0.0.1","port":12345,"username":"old-user","password":"old-pass","valid":true}]}`)
	}))
	defer server.Close()
	defer release()
	s.WebshareEndpoint, s.WebshareHTTPClient = server.URL, server.Client()
	done := make(chan *domain.Error, 1)
	go func() {
		_, derr := s.RefreshStaticSource(context.Background(), src.ID)
		done <- derr
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("account refresh did not begin")
	}
	if _, derr := s.SetSourceSecret(context.Background(), src.ID, src.ConfigVersion, "replacement-account-key"); derr != nil {
		t.Fatal(derr)
	}
	release()
	select {
	case derr := <-done:
		if derr == nil || derr.Code != "SOURCE_CONFIG_CHANGED" {
			t.Fatalf("old authenticated refresh survived credential rotation: %v", derr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("obsolete refresh did not complete")
	}
	if _, _, fresh := s.CachedStaticCandidates(src.ID); fresh {
		t.Fatal("old credentials repopulated the rotated account's cache")
	}
}
