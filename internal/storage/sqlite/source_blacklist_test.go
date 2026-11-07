package sqlite

import (
	"testing"
	"time"

	"proxypool/internal/domain"
)

func TestSourceBlacklistIsPersistentScopedAndDoesNotShrink(t *testing.T) {
	s, ctx := newSchedulingStore(t)
	now := time.Now().UTC()
	seedAccessSource(t, s, "network", "static_url", `{}`, 0, now)
	seedAccessSource(t, s, "other-network", "static_url", `{}`, 0, now)
	src, err := s.GetSource(ctx, "network")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureProject(ctx, domain.Project{ID: "consumer", Enabled: true,
		Defaults: domain.ProjectDefaults{NetworkReuseCooldownMS: 60000}}, now); err != nil {
		t.Fatal(err)
	}
	c := domain.Candidate{Ephemeral: true, SourceConfigVersion: src.ConfigVersion, Offer: domain.Offer{SourceID: src.ID}}
	if err := s.MarkSourceProxyUnavailable(ctx, c, "fingerprint", "consumer", "validation_failed", now); err != nil {
		t.Fatal(err)
	}
	p, err := s.GetProject(ctx, "consumer")
	if err != nil {
		t.Fatal(err)
	}
	p.Defaults.NetworkReuseCooldownMS = 1000
	if _, err := s.UpdateProject(ctx, p, p.ConfigVersion, now); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkSourceProxyUnavailable(ctx, c, "fingerprint", "consumer", "delivered", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	blocked, err := s.SourceProxyBlocked(ctx, "network", "fingerprint", now.Add(59*time.Second))
	if err != nil || !blocked {
		t.Fatal("new short setting erased an existing blacklist window")
	}
	blocked, err = s.SourceProxyBlocked(ctx, "other-network", "fingerprint", now)
	if err != nil || blocked {
		t.Fatal("blacklist leaked across independent channels")
	}
	var seq int
	var name, path string
	if err := s.db.QueryRowContext(ctx, `PRAGMA database_list`).Scan(&seq, &name, &path); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := Open(path, 2048)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	resumed := New(db)
	blocked, err = resumed.SourceProxyBlocked(ctx, "network", "fingerprint", now.Add(59*time.Second))
	if err != nil || !blocked {
		t.Fatal("restart lost persistent blacklist")
	}
	blocked, err = resumed.SourceProxyBlocked(ctx, "network", "fingerprint", now.Add(time.Minute))
	if err != nil || blocked {
		t.Fatal("temporary blacklist became permanent")
	}
}

func TestLegacyBlacklistBootstrapImportsFailureButNotUnusedPass(t *testing.T) {
	s, ctx := newSchedulingStore(t)
	now := time.Now().UTC()
	at := now.Add(-time.Minute)
	seedAccessSource(t, s, "network", "static_url", `{}`, 0, now)
	src, err := s.GetSource(ctx, "network")
	if err != nil {
		t.Fatal(err)
	}
	for _, outcome := range []string{"FAIL", "PASS"} {
		if _, err := s.db.ExecContext(ctx, `INSERT INTO validation_attempts
		 (attempt_id,request_id,resource_id,context_key,profile,outcome,started_at,completed_at)
		 VALUES(?,?,?,'','basic',?,?,?)`, outcome, "legacy", "resource_"+outcome, outcome, fmtTime(at), fmtTime(at)); err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.ExecContext(ctx, `INSERT INTO validation_attempt_sources
		 (attempt_id,source_id,source_config_version,completed_at,outcome) VALUES(?,?,?,?,?)`, outcome, src.ID, src.ConfigVersion, fmtTime(at), outcome); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.BootstrapSourceBlacklist(ctx, now); err != nil {
		t.Fatal(err)
	}
	for _, outcome := range []string{"FAIL", "PASS"} {
		blocked, err := s.SourceProxyBlocked(ctx, src.ID, "resource_"+outcome, now)
		if err != nil || blocked != (outcome == "FAIL") {
			t.Fatalf("legacy bootstrap confused unused PASS with failed supply: %s %v %v", outcome, blocked, err)
		}
	}
	if err := s.BootstrapSourceBlacklist(ctx, now.Add(29*time.Minute)); err != nil {
		t.Fatal(err)
	}
	blocked, err := s.SourceProxyBlocked(ctx, src.ID, "resource_FAIL", now.Add(29*time.Minute))
	if err != nil || blocked {
		t.Fatal("restarting bootstrap extended the historical window")
	}
}
