package application

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"proxypool/internal/domain"
)

func TestStaticRefillKeepsFallbackAndRanksMatchingProofUntilBatchExpiry(t *testing.T) {
	s := newTestIngest(t)
	ctx := context.Background()
	list := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		for i := 0; i < 40; i++ {
			_, _ = fmt.Fprintf(w, "http://127.0.0.1:%d\n", 31000+i)
		}
	}))
	defer list.Close()
	src, derr := s.CreateSource(ctx, "selection-list", SourceConfig{Static: &StaticConfig{URL: list.URL}}, true)
	if derr != nil {
		t.Fatal(derr)
	}
	if _, derr := s.RefreshStaticSource(ctx, src.ID); derr != nil {
		t.Fatal(derr)
	}
	rows, _, fresh := s.CachedStaticCandidates(src.ID)
	if !fresh || len(rows) != 40 {
		t.Fatal("missing controlled list")
	}
	now := s.Now()
	// This is explicit synthetic evidence, outside the first fallback page.
	good := rows[35]
	proof := domain.Evidence{Outcome: "PASS", CompletedAt: now.Add(-time.Second)}
	if err := s.RecordStaticEvidence(ctx, good, "scope-a", proof, nil); err != nil {
		t.Fatal(err)
	}
	scope := RefillScope{ProjectID: DefaultAcquireProject, EvidenceScopeHash: "scope-a", EvidenceNotBefore: now.Add(-30 * time.Second)}
	batch, err := s.Batches(ctx, []string{src.ID}, 1, scope)
	if err != nil || len(batch) != minStaticRefillCandidates || batch[0].Resource.ID != good.Resource.ID {
		t.Fatalf("wanted=1 hid a matching PASS or fallback rows: size=%d err=%v", len(batch), err)
	}
	scope.EvidenceScopeHash = "scope-b"
	foreign, err := s.Batches(ctx, []string{src.ID}, 1, scope)
	if err != nil || len(foreign) == 0 || foreign[0].Resource.ID != rows[32].Resource.ID {
		t.Fatalf("different scope borrowed PASS priority or cursor did not advance: %v", err)
	}
	proof.Outcome = "FAIL"
	proof.CompletedAt = now
	if err := s.RecordStaticEvidence(ctx, good, "scope-a", proof, nil); err != nil {
		t.Fatal(err)
	}
	scope.EvidenceScopeHash = "scope-a"
	negative, err := s.Batches(ctx, []string{src.ID}, 1, scope)
	if err != nil || len(negative) == 0 || negative[0].Resource.ID == good.Resource.ID {
		t.Fatal("newer FAIL kept old PASS priority")
	}
	if err := s.RecordStaticEvidence(ctx, rows[38], "scope-a", domain.Evidence{Outcome: "PASS", CompletedAt: now.Add(-time.Minute)}, nil); err != nil {
		t.Fatal(err)
	}
	expired, err := s.Batches(ctx, []string{src.ID}, 1, scope)
	if err != nil || len(expired) == 0 || expired[0].Resource.ID != rows[38].Resource.ID {
		t.Fatal("verified PASS aged out inside the live list batch")
	}
}
