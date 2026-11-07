package application

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"proxypool/internal/config"
	"proxypool/internal/domain"
	"proxypool/internal/validation"
)

func TestNetworkBatchHardExpiryPurgesProofCredentialsAndValidators(t *testing.T) {
	s, c := cachedTestCandidate(t)
	ctx := context.Background()
	if err := s.RecordStaticEvidence(ctx, c, "scope", domain.Evidence{Outcome: "PASS", CompletedAt: s.Now()}, nil); err != nil {
		t.Fatal(err)
	}
	s.static.mu.Lock()
	e := s.static.entries[c.Offer.SourceID]
	before := staticEntryBytes(e)
	e.etag = "validator"
	s.static.bytes += staticEntryBytes(e) - before
	expires := s.static.expiresAt(e)
	s.static.mu.Unlock()
	s.Now = func() time.Time { return expires }
	if err := s.PurgeExpiredStaticCache(ctx); err != nil {
		t.Fatal(err)
	}
	if rows, _, fresh := s.CachedStaticCandidates(c.Offer.SourceID); fresh || len(rows) > 0 {
		t.Fatal("expired network candidate was retained")
	}
	s.static.mu.Lock()
	defer s.static.mu.Unlock()
	if len(s.static.entries) != 0 || s.static.candidates != 0 || s.static.bytes != 0 {
		t.Fatal("expired credentials/proofs/accounting were not cleared")
	}
}

func TestConditionalCheckDoesNotExtendNetworkBatchLifetime(t *testing.T) {
	s, c := cachedTestCandidate(t)
	src, err := s.store.GetSource(context.Background(), c.Offer.SourceID)
	if err != nil {
		t.Fatal(err)
	}
	old := s.CacheView(src.ID)
	res, entry, derr := s.refreshedEntry(src.ID, src, s.Now().Add(time.Minute), "new-validator", "")
	if derr != nil || entry.cachedAt != *old.FetchedAt || !res.ExpiresAt.Equal(*old.ExpiresAt) {
		t.Fatalf("304 extended retained supply: %+v %v", res, derr)
	}
}

func TestPrewarmSmallDeficitRotatesAwayFromLargeFirstChannel(t *testing.T) {
	s := newTestIngest(t)
	ctx := context.Background()
	for i, count := range []int{40, 1} {
		body := ""
		for n := 0; n < count; n++ {
			body += fmt.Sprintf("http://127.0.0.1:%d\n", 30000+i*100+n)
		}
		list := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = fmt.Fprint(w, body) }))
		t.Cleanup(list.Close)
		id := fmt.Sprintf("channel-%d", i)
		if _, err := s.CreateSource(ctx, id, SourceConfig{Static: &StaticConfig{URL: list.URL}}, true); err != nil {
			t.Fatal(err)
		}
		if _, err := s.RefreshStaticSource(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	cfg := config.Default()
	cfg.IdentitySecret = s.secret
	p := NewPrewarmService(s.store, cfg, "", nil, make(chan struct{}, 4), nil)
	p.StaticSupply = s
	rows, err := p.probePage(ctx, DefaultAcquireProject, prewarmParams{},
		prewarmScope{SourceUnrestricted: true}, "scope", nil, map[string]int{"channel-0": 1})
	if err != nil || len(rows) == 0 || rows[0].Offer.SourceID != "channel-1" {
		t.Fatalf("large first channel monopolized the final one-proxy deficit: %v", err)
	}
}

func TestUploadIgnoresLocalCacheRetention(t *testing.T) {
	s := newTestIngest(t)
	c := s.buildCandidate(domain.Source{ID: "upload", AdapterType: AdapterCustom},
		SourceConfig{LocalRetentionMS: 1000}, domain.Connection{Transport: domain.TransportHTTP, Host: "127.0.0.1", Port: 8080}, s.Now())
	if c.LocalRetentionDeadline != nil {
		t.Fatal("uploaded supply was given an automatic cache deadline")
	}
}

func TestCandidateTimeoutIsVerdictButParentCancellationIsNot(t *testing.T) {
	fail := &validation.Failure{Kind: validation.FailBudget}
	if !probeHasVerdict(context.Background(), fail) {
		t.Fatal("candidate timeout escaped the blacklist")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if probeHasVerdict(ctx, fail) {
		t.Fatal("parent cancellation was treated as a proxy failure")
	}
}
