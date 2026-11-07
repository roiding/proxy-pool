package sqlite

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"proxypool/internal/domain"
)

// These are synthetic database fixtures, not addresses that were probed.
func seedAccessSource(t *testing.T, s *Store, id, adapter, config string, n int, now time.Time) {
	t.Helper()
	ctx := context.Background()
	if err := s.CreateSource(ctx, domain.Source{ID: id, Tenant: "default", AdapterType: adapter,
		ParserType: "lines_v1", Enabled: true, ConfigJSON: config}, now); err != nil {
		t.Fatal(err)
	}
	if adapter != "custom" {
		return
	}
	for i := 0; i < n; i++ {
		conn := domain.Connection{Transport: domain.TransportHTTP, Host: "8.8.8.8", Port: 10000 + i, Username: id}
		cred := domain.CredentialVersion("access-test", conn.Username, conn.Password)
		c := domain.NormalizedCandidate{SourceID: id, RecordID: fmt.Sprint(i), Generation: "pool", Conn: conn,
			Kind: domain.KindStatic, Session: domain.SessionStatic, MaxPoolLeases: 1, CredentialVersion: cred}
		c.ResourceID = domain.ResourceID("access-test", domain.IdentityKey{Tenant: "default", Transport: conn.Transport,
			Host: conn.Host, Port: conn.Port, CredentialVersion: cred, Session: domain.SessionStatic})
		c.OfferID = domain.OfferID("access-test", id, c.RecordID, c.Generation)
		if _, err := s.ImportCandidates(ctx, []domain.NormalizedCandidate{c}, now); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSourceProjectAccessFiltersBeforeInventoryPageLimit(t *testing.T) {
	s, ctx := newSchedulingStore(t)
	now := time.Now().UTC()
	seedAccessSource(t, s, "private", "custom", `{"access":{"project_ids":["owner"]}}`, 8, now)
	seedAccessSource(t, s, "deny-all", "custom", `{"access":{"project_ids":[]}}`, 4, now)
	seedAccessSource(t, s, "public", "custom", `{}`, 1, now)
	all, err := s.SelectCandidates(ctx, SelectionFilters{Now: now, UsefulCutoff: now, Profile: "select", Limit: 32})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range all {
		profile := "select"
		if c.Offer.SourceID == "public" {
			// Prewarm orders unknown rows ahead of passing ones. Without the
			// source ceiling before LIMIT, private rows would consume its page.
			profile = "warm"
		}
		if err := s.UpdateHealth(ctx, HealthUpdate{ResourceID: c.Resource.ID,
			ContextKey: domain.ContextKey(c.Resource.CredentialVersion, c.Resource.Session),
			Profile:    profile, Outcome: "PASS", Incarnation: c.Incarnation, Now: now}); err != nil {
			t.Fatal(err)
		}
	}
	selected, err := s.SelectCandidates(ctx, SelectionFilters{ProjectID: "outsider", Now: now,
		UsefulCutoff: now, Profile: "select", Limit: 1})
	if err != nil || len(selected) != 1 || selected[0].Offer.SourceID != "public" {
		t.Fatalf("private passing rows consumed public acquire page: %+v %v", selected, err)
	}
	warm, err := s.SelectPrewarmCandidates(ctx, PrewarmQuery{ProjectID: "outsider", Now: now,
		Profile: "warm", ScopeUnrestricted: true, Limit: 1})
	if err != nil || len(warm) != 1 || warm[0].Offer.SourceID != "public" {
		t.Fatalf("private unknown rows consumed public prewarm page: %+v %v", warm, err)
	}
	owner, err := s.SelectCandidates(ctx, SelectionFilters{ProjectID: "owner", Now: now,
		UsefulCutoff: now, Profile: "select", Limit: 32})
	if err != nil || len(owner) != 9 {
		t.Fatalf("owner lost access or deny-all became public: count=%d err=%v", len(owner), err)
	}
}

func TestSourceProjectAccessFiltersBeforeRefillLimit(t *testing.T) {
	for _, adapter := range []string{"provider_api", "static_url", "account_list"} {
		t.Run(adapter, func(t *testing.T) {
			s, ctx := newSchedulingStore(t)
			now := time.Now().UTC()
			for i := 0; i < 16; i++ {
				seedAccessSource(t, s, fmt.Sprintf("private-%02d", i), adapter,
					`{"access":{"project_ids":["owner"]}}`, 0, now)
			}
			seedAccessSource(t, s, "public", adapter, `{}`, 0, now)
			for i := 0; i < 8; i++ {
				var ids []string
				var err error
				if adapter == "provider_api" {
					ids, err = s.ProviderSourcesForRefill(ctx, now, nil, 1, "outsider")
				} else {
					ids, err = s.StaticSourcesForRefill(ctx, now, nil, 1, "outsider")
				}
				if err != nil || len(ids) != 1 || ids[0] != "public" {
					t.Fatalf("private sources consumed another project's refill page: %v %v", ids, err)
				}
			}
		})
	}
}

func TestSourceAccessIsRecheckedInsideTransaction(t *testing.T) {
	s, ctx := newSchedulingStore(t)
	now := time.Now().UTC()
	seedAccessSource(t, s, "private", "custom", `{"access":{"project_ids":["owner"]}}`, 1, now)
	rows, err := s.SelectCandidates(ctx, SelectionFilters{ProjectID: "owner", Now: now,
		UsefulCutoff: now, Limit: 1})
	if err != nil || len(rows) != 1 {
		t.Fatalf("owner selection failed: %v %v", rows, err)
	}
	_, err = s.ReserveSlot(ctx, rows[0], ReserveArgs{ProjectID: "outsider", Owner: "req",
		Profile: "test", Now: now, ReservationDeadline: now.Add(time.Minute)})
	if !errors.Is(err, ErrNotEligible) {
		t.Fatalf("selection snapshot bypassed source access at reserve: %v", err)
	}
	src, err := s.GetSource(ctx, "private")
	if err != nil {
		t.Fatal(err)
	}
	src.ConfigJSON = `{"access":{"project_ids":[]}}`
	if err := s.UpdateSource(ctx, src, src.ConfigVersion, now); err != nil {
		t.Fatal(err)
	}
	_, err = s.ReserveSlot(ctx, rows[0], ReserveArgs{ProjectID: "owner", Owner: "req",
		Profile: "test", Now: now, ReservationDeadline: now.Add(time.Minute)})
	if !errors.Is(err, ErrNotEligible) {
		t.Fatalf("revoked source access survived until reserve: %v", err)
	}
}
