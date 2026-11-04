package application

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"proxypool/internal/domain"
	"proxypool/internal/storage/sqlite"
)

func newTestIngest(t *testing.T) *IngestService {
	t.Helper()
	db, err := sqlite.Open(filepath.Join(t.TempDir(), "sources.db"), 1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store := sqlite.New(db)
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	// allow_private on: static-download fixtures run on loopback httptest
	// servers; production stays restricted by config.
	return NewIngestService(store, "test-secret", true)
}

func TestImportLinesRejectsNonCustomAdapters(t *testing.T) {
	svc := newTestIngest(t)
	ctx := context.Background()
	if _, derr := svc.CreateSource(ctx, "provider-import", SourceConfig{
		Provider: &ProviderConfig{Protocol: "http"},
	}, true); derr != nil {
		t.Fatal(derr)
	}
	if _, derr := svc.CreateSource(ctx, "static-import", SourceConfig{
		Static: &StaticConfig{URL: "https://example.com/list.txt"},
	}, true); derr != nil {
		t.Fatal(derr)
	}
	for _, id := range []string{"provider-import", "static-import"} {
		if _, derr := svc.ImportLines(ctx, id, "127.0.0.1:8080\n"); derr == nil {
			t.Fatalf("non-custom source %q accepted persistent import", id)
		}
	}
	stats, err := svc.store.Stats(ctx)
	if err != nil || stats.Resources != 0 || stats.EnabledOffers != 0 {
		t.Fatalf("rejected imports changed inventory: stats=%+v err=%v", stats, err)
	}
}

func TestConfigureStaticCacheAppliesLimits(t *testing.T) {
	svc := newTestIngest(t)
	if err := svc.ConfigureStaticCache(time.Second, 2, 1024); err != nil {
		t.Fatal(err)
	}
	if svc.static.ttl != time.Second || svc.static.maxEntries != 2 ||
		svc.static.maxBytes != 1024 {
		t.Fatalf("static config not applied: %+v", svc.static)
	}
	if err := svc.ConfigureStaticCache(0, 2, 1024); err == nil {
		t.Fatal("zero ttl must be rejected")
	}
}

func TestAdapterDescriptorsClosedRegistry(t *testing.T) {
	descs := AdapterDescriptors()
	seen := map[string]bool{}
	for _, d := range descs {
		if d.ID == "" || seen[d.ID] || d.Version < 1 {
			t.Fatalf("bad descriptor: %+v", d)
		}
		seen[d.ID] = true
	}
	if len(descs) != 4 {
		t.Fatalf("registry must be exactly the four code-owned adapters, got %d", len(descs))
	}
}

func TestCreateSourcePersistsDefinitionWithoutFetching(t *testing.T) {
	svc := newTestIngest(t)
	ctx := context.Background()
	src, derr := svc.CreateSource(ctx, "upload-1", SourceConfig{
		MaxPoolLeases: 2,
	}, true)
	if derr != nil {
		t.Fatal(derr)
	}
	if src.ConfigVersion != 1 || src.AdapterType != AdapterCustom || !src.Enabled {
		t.Fatalf("unexpected created source: %+v", src)
	}
	got, derr := svc.GetSource(ctx, "upload-1")
	if derr != nil || got.ID != "upload-1" || !got.Enabled {
		t.Fatalf("get after create: %+v err=%v", got, derr)
	}
	var cfg SourceConfig
	if err := json.Unmarshal([]byte(got.ConfigJSON), &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.MaxPoolLeases != 2 {
		t.Fatalf("config not persisted as given: %+v", cfg)
	}
}

func TestCreateSourceNeverOverwrites(t *testing.T) {
	svc := newTestIngest(t)
	ctx := context.Background()
	if _, derr := svc.CreateSource(ctx, "dup", SourceConfig{}, true); derr != nil {
		t.Fatal(derr)
	}
	_, derr := svc.CreateSource(ctx, "dup", SourceConfig{MaxPoolLeases: 9}, false)
	if derr == nil || derr.Code != "SOURCE_EXISTS" || derr.Status != 409 {
		t.Fatalf("duplicate create must conflict, got %+v", derr)
	}
}

func TestCreateSourceValidation(t *testing.T) {
	svc := newTestIngest(t)
	ctx := context.Background()
	cases := []struct {
		name string
		id   string
		cfg  SourceConfig
	}{
		{"bad id", "Bad ID!", SourceConfig{}},
		{"unknown site", "site-x", SourceConfig{Site: &SiteConfig{SiteID: "not-a-site"}}},
		{"bad provider protocol", "prov-x", SourceConfig{Provider: &ProviderConfig{Protocol: "socks4"}}},
		{"static bad url", "static-x", SourceConfig{Static: &StaticConfig{URL: "ftp://x/y"}}},
		{"static relative url", "static-y", SourceConfig{Static: &StaticConfig{URL: "/etc/hosts"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, derr := svc.CreateSource(ctx, tc.id, tc.cfg, true); derr == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
	src, derr := svc.CreateSource(ctx, "static-ok", SourceConfig{
		Static: &StaticConfig{URL: "https://example.com/list.txt"},
	}, true)
	if derr != nil || src.AdapterType != AdapterStatic || src.ParserType != ParserLines {
		t.Fatalf("valid static channel rejected: %+v err=%v", src, derr)
	}
}

func TestUpdateSourceVersionGuard(t *testing.T) {
	svc := newTestIngest(t)
	ctx := context.Background()
	if _, derr := svc.CreateSource(ctx, "cfg-1", SourceConfig{MaxPoolLeases: 1}, true); derr != nil {
		t.Fatal(derr)
	}
	updated, derr := svc.UpdateSource(ctx, "cfg-1", SourceConfig{MaxPoolLeases: 5},
		true, 1)
	if derr != nil {
		t.Fatal(derr)
	}
	if updated.ConfigVersion != 2 {
		t.Fatalf("version must advance to 2, got %d", updated.ConfigVersion)
	}
	got, _ := svc.GetSource(ctx, "cfg-1")
	var cfg SourceConfig
	if err := json.Unmarshal([]byte(got.ConfigJSON), &cfg); err != nil || cfg.MaxPoolLeases != 5 {
		t.Fatalf("update not persisted: %+v err=%v", cfg, err)
	}
	if _, derr = svc.UpdateSource(ctx, "cfg-1", SourceConfig{MaxPoolLeases: 6}, true, 1); derr == nil ||
		derr.Code != "SOURCE_VERSION_CONFLICT" {
		t.Fatalf("stale version must conflict, got %+v", derr)
	}
	if _, derr = svc.UpdateSource(ctx, "missing", SourceConfig{}, true, 1); derr == nil ||
		derr.Code != "NOT_FOUND" {
		t.Fatalf("unknown source must 404, got %+v", derr)
	}
	if _, derr = svc.UpdateSource(ctx, "cfg-1", SourceConfig{
		Site: &SiteConfig{SiteID: "ip89"},
	}, true, 2); derr == nil {
		t.Fatal("adapter type switch must be rejected")
	}
}

func TestSourceDeleteRecreateAdvancesVersionAndDropsSnapshots(t *testing.T) {
	svc := newTestIngest(t)
	ctx := context.Background()
	first, derr := svc.CreateSource(ctx, "aba-source", SourceConfig{
		Provider: &ProviderConfig{Protocol: "http"},
	}, true)
	if derr != nil {
		t.Fatal(derr)
	}
	oldCandidate := domain.Candidate{
		Resource: domain.Resource{Conn: domain.Connection{
			Transport: domain.TransportHTTP, Host: "203.0.113.9", Port: 8080,
		}},
		Offer:               domain.Offer{SourceID: "aba-source"},
		SourceConfigVersion: first.ConfigVersion,
	}
	if err := svc.store.PutBinding(ctx, domain.StickyBinding{
		ProjectID: DefaultAcquireProject, SessionID: "aba-session",
		SourceID: "aba-source", SourceConfigVersion: first.ConfigVersion,
		ResourceID: "old-private-resource", Incarnation: "ondemand_old",
		Fingerprint: "old-fingerprint", ConnectionCiphertext: "retired-secret",
		State:    sqlite.BindingActive,
		Deadline: time.Now().Add(time.Hour),
	}, 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, derr = svc.DeleteSource(ctx, "aba-source"); derr != nil {
		t.Fatal(derr)
	}
	binding, err := svc.store.GetBinding(ctx, DefaultAcquireProject, "aba-session")
	if err != nil || binding.State != "INVALID" ||
		binding.ConnectionCiphertext != "" {
		t.Fatalf("deleted source left a reusable session snapshot: binding=%+v err=%v", binding, err)
	}
	second, derr := svc.CreateSource(ctx, "aba-source", SourceConfig{
		Provider: &ProviderConfig{Protocol: "socks5"},
	}, true)
	if derr != nil {
		t.Fatal(derr)
	}
	if second.ConfigVersion <= first.ConfigVersion {
		t.Fatalf("recreated source reused config version: first=%d second=%d",
			first.ConfigVersion, second.ConfigVersion)
	}
	eligible, err := svc.store.StaticBatchLive(ctx, "aba-source",
		oldCandidate.SourceConfigVersion, "unused", time.Now())
	if err != nil || eligible {
		t.Fatalf("old version remained eligible after recreate: eligible=%t err=%v", eligible, err)
	}
	if _, derr = svc.DeleteSource(ctx, "aba-source"); derr != nil {
		t.Fatal(derr)
	}
	third, derr := svc.CreateSource(ctx, "aba-source", SourceConfig{}, true)
	if derr != nil {
		t.Fatal(derr)
	}
	if third.ConfigVersion <= second.ConfigVersion {
		t.Fatalf("second recreate version went backwards: second=%d third=%d",
			second.ConfigVersion, third.ConfigVersion)
	}
}

func TestSetSourceEnabledAdvancesVersion(t *testing.T) {
	svc := newTestIngest(t)
	ctx := context.Background()
	if _, derr := svc.CreateSource(ctx, "toggle", SourceConfig{}, true); derr != nil {
		t.Fatal(derr)
	}
	src, derr := svc.SetSourceEnabled(ctx, "toggle", false, 1)
	if derr != nil || src.Enabled || src.ConfigVersion != 2 {
		t.Fatalf("disable failed: %+v err=%v", src, derr)
	}
	if _, derr = svc.SetSourceEnabled(ctx, "toggle", true, 1); derr == nil ||
		derr.Code != "SOURCE_VERSION_CONFLICT" {
		t.Fatalf("stale enable must conflict, got %+v", derr)
	}
}

func TestListSources(t *testing.T) {
	svc := newTestIngest(t)
	ctx := context.Background()
	for _, id := range []string{"b-src", "a-src"} {
		if _, derr := svc.CreateSource(ctx, id, SourceConfig{}, true); derr != nil {
			t.Fatal(derr)
		}
	}
	srcs, derr := svc.ListSources(ctx)
	if derr != nil || len(srcs) != 2 || srcs[0].ID != "a-src" || srcs[1].ID != "b-src" {
		t.Fatalf("list mismatch: %+v err=%v", srcs, derr)
	}
}

func TestEnsureSourceKeepsLegacyUpsertCompatibility(t *testing.T) {
	svc := newTestIngest(t)
	ctx := context.Background()
	if err := svc.EnsureSource(ctx, "legacy", SourceConfig{MaxPoolLeases: 1}, true); err != nil {
		t.Fatal(err)
	}
	if err := svc.EnsureSource(ctx, "legacy", SourceConfig{MaxPoolLeases: 3}, false); err != nil {
		t.Fatal(err)
	}
	got, derr := svc.GetSource(ctx, "legacy")
	if derr != nil || got.Enabled {
		t.Fatalf("legacy upsert must still overwrite: %+v err=%v", got, derr)
	}
	var cfg SourceConfig
	if err := json.Unmarshal([]byte(got.ConfigJSON), &cfg); err != nil || cfg.MaxPoolLeases != 3 {
		t.Fatalf("legacy config not overwritten: %+v", cfg)
	}
}
