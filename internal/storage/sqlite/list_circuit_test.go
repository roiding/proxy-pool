package sqlite

import (
	"testing"
	"time"

	"proxypool/internal/domain"
)

func TestListProxyFailuresDoNotTripChannelCircuit(t *testing.T) {
	for _, adapter := range []string{"site_fetch", "static_url", "account_list", "custom", "provider_api"} {
		t.Run(adapter, func(t *testing.T) {
			s, ctx := newSchedulingStore(t)
			now := time.Now().UTC()
			if err := s.CreateSource(ctx, domain.Source{ID: "channel", AdapterType: adapter, Enabled: true, ConfigJSON: "{}"}, now); err != nil {
				t.Fatal(err)
			}
			src, err := s.GetSource(ctx, "channel")
			if err != nil {
				t.Fatal(err)
			}
			policy := SourceCircuitPolicy{FailureThreshold: 3, Base: time.Minute, Max: time.Hour}
			for i := 0; i < 4; i++ {
				if _, err := s.RecordSourceOutcomeOwned(ctx, src.ID, src.ConfigVersion, "FAIL", now, policy); err != nil {
					t.Fatal(err)
				}
			}
			open, err := s.SourceCircuitOpen(ctx, src.ID, now)
			want := adapter == "custom"
			if err != nil || open != want {
				t.Fatalf("channel circuit open=%v want=%v err=%v", open, want, err)
			}
			if !want {
				var circuitRows int
				if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM source_circuits
						WHERE source_id = ?`, src.ID).Scan(&circuitRows); err != nil {
					t.Fatal(err)
				}
				if circuitRows != 0 {
					t.Fatalf("network adapter %s retained a whole-source circuit row", adapter)
				}
				if _, err := s.RecordSourceOutcome(ctx, src.ID, "FAIL", now, SourceCircuitPolicy{FailureThreshold: 1, Base: time.Hour}); err != nil {
					t.Fatal(err)
				}
				if err := s.EnsureSupplySchema(ctx); err != nil {
					t.Fatal(err)
				}
				if open, err := s.SourceCircuitOpen(ctx, src.ID, now); err != nil || open {
					t.Fatalf("old list circuit survived upgrade: %v %v", open, err)
				}
			}
		})
	}
}

func TestListAttemptsVisibleWithoutPersistentInventory(t *testing.T) {
	s, ctx := newSchedulingStore(t)
	now := time.Now().UTC()
	if err := s.CreateSource(ctx, domain.Source{ID: "list", AdapterType: "site_fetch", Enabled: true, ConfigJSON: "{}"}, now); err != nil {
		t.Fatal(err)
	}
	src, _ := s.GetSource(ctx, "list")
	for _, outcome := range []string{"PASS", "FAIL", "PASS"} {
		ev := domain.Evidence{AttemptID: domain.NewID("att_"), Outcome: outcome, StartedAt: now, CompletedAt: now, Profile: "https_basic_v1"}
		if err := s.RecordAttemptAttributed(ctx, ev, "ephemeral", "ctx", "", "static_batch", src.ID, src.ConfigVersion); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := s.SourcesOverview(ctx)
	if err != nil || len(rows) != 1 || rows[0].AttemptPass != 2 || rows[0].AttemptFail != 1 || rows[0].LastValidated == nil {
		t.Fatalf("list attempts hidden: %+v %v", rows, err)
	}
	st, err := s.Stats(ctx)
	if err != nil || st.AttemptPass != 2 || st.AttemptFail != 1 || st.Resources != 0 {
		t.Fatalf("stats hid attempts or created inventory: %+v %v", st, err)
	}
	src.ConfigJSON = `{"changed":true}`
	if err := s.UpdateSource(ctx, src, src.ConfigVersion, now); err != nil {
		t.Fatal(err)
	}
	rows, err = s.SourcesOverview(ctx)
	if err != nil || len(rows) != 1 || rows[0].AttemptPass != 0 || rows[0].AttemptFail != 0 {
		t.Fatalf("new configuration inherited old attempt counts: %+v %v", rows, err)
	}
}
