package application

import (
	"context"
	"encoding/json"
	"testing"

	"proxypool/internal/domain"
	"proxypool/internal/integrations/sites"
)

func TestChannelTemplatesAreCuratedOnePerSupplierWithValidVariants(t *testing.T) {
	svc := newTestIngest(t)
	channels := svc.ChannelTemplates()
	if len(channels) != 14 {
		t.Fatalf("unexpected admitted family count %d", len(channels))
	}
	ids, names, variants := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, channel := range channels {
		if ids[channel.ID] || names[channel.Name] {
			t.Fatalf("duplicate supplier %s %s", channel.ID, channel.Name)
		}
		ids[channel.ID] = true
		names[channel.Name] = true
		if !channel.Runnable || channel.Config == nil || len(channel.Variants) == 0 || channel.Reason == "" {
			t.Fatalf("unreviewed/noncreatable entry: %+v", channel)
		}
		protocols := map[domain.Transport]bool{}
		for _, v := range channel.Variants {
			if variants[channel.ID+"/"+v.ID] || v.Config == nil {
				t.Fatalf("duplicate/missing variant %s/%s", channel.ID, v.ID)
			}
			variants[channel.ID+"/"+v.ID] = true
			if _, derr := svc.buildSource("check-variant", *v.Config, false); derr != nil {
				t.Fatalf("invalid variant %s/%s: %v", channel.ID, v.ID, derr)
			}
			for _, protocol := range v.Transports {
				protocols[protocol] = true
			}
			if v.Config.Site != nil {
				family, ok := sites.ChannelForSite(v.Config.Site.SiteID)
				if !ok || family.ID != channel.ID {
					t.Fatalf("variant assigned to wrong family: %+v", v)
				}
			}
			if v.Config.Account != nil && (v.Config.Access == nil || v.Config.Access.ProjectIDs == nil || len(v.Config.Access.ProjectIDs) != 0) {
				t.Fatal("account variant is not explicitly private")
			}
		}
		if len(protocols) != len(channel.Transports) {
			t.Fatal("combined protocols inconsistent")
		}
		for _, p := range channel.Transports {
			if !protocols[p] {
				t.Fatal("channel advertises unknown protocol")
			}
			delete(protocols, p)
		}
	}
	for _, excluded := range []string{"thespeedx", "kuaidaili", "roosterkid", "sslproxies", "us-proxy", "socks-proxy", "proxy-list-download", "jetkai", "7yip", "ipzan-extract", "zhima-extract", "taiyang-extract", "jiguang-old", "scraperapi", "scrapingbee", "crawlbase"} {
		if ids[excluded] {
			t.Fatalf("not-worth-building supplier exposed: %s", excluded)
		}
	}
	if !ids["openproxylist"] || !variants["openproxylist/openproxylist-http"] || !variants["openproxylist/roosterkid-socks5"] {
		t.Fatal("OpenProxyList paths/protocols not grouped")
	}
	if !variants["webshare/http"] || !variants["webshare/socks5"] {
		t.Fatal("Webshare protocols not grouped")
	}
}

func TestTemplateConfigsAreDetachedAndPrivateEmptySurvivesWire(t *testing.T) {
	s := new(IngestService)
	first := s.ChannelTemplates()
	for i := range first {
		if first[i].ID == "webshare" {
			first[i].Config.Account.Protocol = "changed"
			if first[i].Variants[0].Config.Account.Protocol != "http" {
				t.Fatal("default aliases variant")
			}
			first[i].Variants[0].Config.Access.ProjectIDs = append(first[i].Variants[0].Config.Access.ProjectIDs, "outsider")
		}
	}
	raw, _ := json.Marshal(s.ChannelTemplates())
	var decoded []ChannelTemplate
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	for _, c := range decoded {
		if c.ID == "webshare" {
			for _, v := range c.Variants {
				if v.Config.Access.ProjectIDs == nil || len(v.Config.Access.ProjectIDs) != 0 {
					t.Fatal("account private empty array became null or caller mutation survived")
				}
			}
		}
	}
}

func TestNotAdmittedEndpointsCannotBecomeNewSourcesButLegacyRemainsEditable(t *testing.T) {
	s := newTestIngest(t)
	ctx := context.Background()
	for _, id := range []string{"thespeedx-http", "kuaidaili-free", "jetkai-proxy-list", "proxy-list-download-http", "spys-html"} {
		cfg := SourceConfig{Site: &SiteConfig{SiteID: id}}
		if _, derr := s.CreateSource(ctx, "new-"+id, cfg, false); derr == nil || derr.Code != "CHANNEL_NOT_ADMITTED" {
			t.Fatalf("not-admitted creation succeeded %s: %v", id, derr)
		}
		if err := s.EnsureSource(ctx, "new-"+id, cfg, false); err == nil {
			t.Fatalf("legacy API bypasses admission %s", id)
		}
	}
	cfg := SourceConfig{Site: &SiteConfig{SiteID: "thespeedx-http"}}
	src, derr := s.buildSource("legacy-speedx", cfg, false)
	if derr != nil {
		t.Fatal(derr)
	}
	if err := s.store.CreateSource(ctx, src, s.Now()); err != nil {
		t.Fatal(err)
	}
	updated, derr := s.UpdateSource(ctx, src.ID, cfg, false, 1)
	if derr != nil {
		t.Fatalf("unchanged legacy definition not editable: %v", derr)
	}
	if err := s.EnsureSource(ctx, src.ID, cfg, false); err != nil {
		t.Fatalf("existing bootstrap definition broken: %v", err)
	}
	if _, derr = s.UpdateSource(ctx, updated.ID, SourceConfig{Site: &SiteConfig{SiteID: "kuaidaili-free"}}, false, updated.ConfigVersion); derr == nil || derr.Code != "CHANNEL_NOT_ADMITTED" {
		t.Fatalf("legacy endpoint switched to new excluded source: %v", derr)
	}
}

func TestSourceSupplierGroupingDoesNotMergeInstancesOrPermissions(t *testing.T) {
	for _, id := range []string{"proxyscrape-http", "proxyscrape-socks5", "proxyscrape-anon", "proxyscrape-elite"} {
		raw, _ := json.Marshal(SourceConfig{Site: &SiteConfig{SiteID: id}})
		src := domain.Source{ID: "instance-" + id, ConfigJSON: string(raw)}
		family, name := SourceChannelIdentity(src)
		if family != "proxyscrape" || name != "ProxyScrape" || src.ID != "instance-"+id {
			t.Fatal("display grouping changed persisted source identity")
		}
	}
	src := domain.Source{ID: "private-custom", ConfigJSON: `{"static":{"url":"https://example.com/list"}}`}
	id, _ := SourceChannelIdentity(src)
	if id != src.ID {
		t.Fatal("unknown URL arbitrarily merged with a supplier")
	}
}
