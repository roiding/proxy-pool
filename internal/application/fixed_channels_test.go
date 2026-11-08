package application

import (
	"context"
	"encoding/json"
	"testing"

	"proxypool/internal/domain"
)

func fixedChannelForTest(t *testing.T, s *IngestService, id string) FixedChannel {
	t.Helper()
	channels, derr := s.FixedChannels(context.Background())
	if derr != nil {
		t.Fatal(derr)
	}
	for _, channel := range channels {
		if channel.ID == id {
			return channel
		}
	}
	t.Fatalf("missing channel %s", id)
	return FixedChannel{}
}

func TestFixedChannelSwitchIncludesBothProtocolsAndIsIdempotent(t *testing.T) {
	s := newTestIngest(t)
	ctx := context.Background()
	channel := fixedChannelForTest(t, s, "proxyscrape")
	if channel.Enabled || len(channel.SourceIDs) != 0 || len(channel.Transports) != 2 {
		t.Fatalf("unexpected unopened channel: %+v", channel)
	}
	if derr := s.SetFixedChannelEnabled(ctx, channel.ID, true, channel.Version); derr != nil {
		t.Fatal(derr)
	}
	channel = fixedChannelForTest(t, s, channel.ID)
	if !channel.Enabled || channel.PartiallyEnabled || len(channel.SourceIDs) != 2 {
		t.Fatalf("one switch did not enable both protocols: %+v", channel)
	}
	before, _ := s.ListSources(ctx)
	if err := s.ReconcileEnabledChannels(ctx); err != nil {
		t.Fatal(err)
	}
	after, _ := s.ListSources(ctx)
	for i := range before {
		if before[i] != after[i] {
			t.Fatal("restart changed an existing definition or version")
		}
	}
	if derr := s.SetFixedChannelEnabled(ctx, channel.ID, false, channel.Version); derr != nil {
		t.Fatal(derr)
	}
	for _, src := range channelSources(after, channel.ID) {
		current, _ := s.GetSource(ctx, src.ID)
		if current.Enabled {
			t.Fatal("channel off left a protocol enabled")
		}
	}
	if derr := s.SetFixedChannelEnabled(ctx, channel.ID, true, channel.Version); derr == nil || derr.Code != "CHANNEL_VERSION_CONFLICT" {
		t.Fatalf("stale switch accepted: %v", derr)
	}
}

func TestFixedChannelUpgradePreservesSourceAndAccessAndDisabledSuppliers(t *testing.T) {
	s := newTestIngest(t)
	ctx := context.Background()
	cfg := SourceConfig{Site: &SiteConfig{SiteID: "proxyscrape-elite"},
		Access: &SourceAccessConfig{ProjectIDs: []string{"private-project"}}}
	existing, err := s.CreateSource(ctx, "legacy-http", cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateSource(ctx, "disabled-monosans", SourceConfig{Site: &SiteConfig{SiteID: "monosans-http"}}, false); err != nil {
		t.Fatal(err)
	}
	if err := s.ReconcileEnabledChannels(ctx); err != nil {
		t.Fatal(err)
	}
	current, _ := s.GetSource(ctx, existing.ID)
	if current != existing {
		t.Fatal("upgrade changed the existing source definition")
	}
	channel := fixedChannelForTest(t, s, "proxyscrape")
	if len(channel.SourceIDs) != 2 {
		t.Fatal("upgrade did not supply missing SOCKS5")
	}
	for _, id := range channel.SourceIDs {
		src, _ := s.GetSource(ctx, id)
		var config SourceConfig
		_ = json.Unmarshal([]byte(src.ConfigJSON), &config)
		if sourceProjectAllowed(config, "unrelated-project") || !sourceProjectAllowed(config, "private-project") {
			t.Fatal("new protocol widened private source access")
		}
	}
	if fixedChannelForTest(t, s, "monosans").Enabled {
		t.Fatal("upgrade enabled a disabled supplier")
	}
}

func TestFixedChannelCollisionDoesNotPartiallyEnableExistingMembers(t *testing.T) {
	s := newTestIngest(t)
	ctx := context.Background()
	if _, err := s.CreateSource(ctx, "existing-http", SourceConfig{Site: &SiteConfig{SiteID: "proxyscrape-http"}}, false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateSource(ctx, "builtin-proxyscrape-socks5", SourceConfig{DefaultTransport: domain.TransportHTTP}, false); err != nil {
		t.Fatal(err)
	}
	channel := fixedChannelForTest(t, s, "proxyscrape")
	if derr := s.SetFixedChannelEnabled(ctx, channel.ID, true, channel.Version); derr == nil || derr.Code != "SOURCE_EXISTS" {
		t.Fatalf("collision accepted: %v", derr)
	}
	src, _ := s.GetSource(ctx, "existing-http")
	if src.Enabled {
		t.Fatal("failed switch partially enabled a channel")
	}
}

func TestFixedChannelCatalogReadDoesNotFetchOrCreateAndAccountNeedsCredentials(t *testing.T) {
	s := newTestIngest(t)
	ctx := context.Background()
	channels, err := s.FixedChannels(ctx)
	if err != nil || len(channels) != 14 {
		t.Fatalf("catalog: %v %+v", err, channels)
	}
	sources, _ := s.ListSources(ctx)
	if len(sources) != 0 {
		t.Fatal("catalog read created sources")
	}
	channel := fixedChannelForTest(t, s, "webshare")
	if channel.Available {
		t.Fatal("unconnected account advertised a working switch")
	}
	if derr := s.SetFixedChannelEnabled(ctx, channel.ID, true, channel.Version); derr == nil {
		t.Fatal("unconnected account enabled")
	}
	open := fixedChannelForTest(t, s, "openproxylist")
	if len(open.Transports) != 1 || open.Transports[0] != domain.TransportHTTP {
		t.Fatal("repository mirror silently used as fixed supplier supply")
	}
}

func TestFixedChannelSwitchDoesNotIncludeAnUploadNamedAfterSupplier(t *testing.T) {
	s := newTestIngest(t)
	ctx := context.Background()
	upload, err := s.CreateSource(ctx, "proxyscrape", SourceConfig{}, true)
	if err != nil {
		t.Fatal(err)
	}
	channel := fixedChannelForTest(t, s, "proxyscrape")
	if channel.Enabled || len(channel.SourceIDs) != 0 {
		t.Fatal("upload was folded into an unrelated supplier")
	}
	if derr := s.SetFixedChannelEnabled(ctx, channel.ID, true, channel.Version); derr != nil {
		t.Fatal(derr)
	}
	current, _ := s.GetSource(ctx, upload.ID)
	if current != upload {
		t.Fatal("supplier switch changed independent upload")
	}
	if len(fixedChannelForTest(t, s, "source:proxyscrape").SourceIDs) != 1 {
		t.Fatal("independent upload channel lost")
	}
}

func TestFixedChannelStorageRejectsConcurrentMembershipChange(t *testing.T) {
	s := newTestIngest(t)
	ctx := context.Background()
	existing, err := s.CreateSource(ctx, "http", SourceConfig{Site: &SiteConfig{SiteID: "proxyscrape-http"}}, false)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _ := s.ListSources(ctx)
	if _, err := s.CreateSource(ctx, "socks", SourceConfig{Site: &SiteConfig{SiteID: "proxyscrape-socks5"}}, false); err != nil {
		t.Fatal(err)
	}
	existing.Enabled = true
	if err := s.store.ApplyChannelSources(ctx, snapshot, []domain.Source{existing}, s.Now()); err == nil {
		t.Fatal("switch committed against changed membership")
	}
	current, _ := s.GetSource(ctx, existing.ID)
	if current.Enabled {
		t.Fatal("conflicting switch partially committed")
	}
}
