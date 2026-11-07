package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"proxypool/internal/application"
)

func TestChannelTemplateAPIHasOneReviewedSupplierRow(t *testing.T) {
	e := newImportIdentityServices(t)
	rec := httptest.NewRecorder()
	e.server.handleChannelTemplates(rec, httptest.NewRequest("GET", "/", nil))
	var body struct {
		Templates []application.ChannelTemplate `json:"templates"`
	}
	if rec.Code != 200 || json.Unmarshal(rec.Body.Bytes(), &body) != nil || len(body.Templates) != 14 {
		t.Fatalf("channel catalog: %d %s", rec.Code, rec.Body.String())
	}
	for _, template := range body.Templates {
		if template.Config == nil || len(template.Variants) == 0 {
			t.Fatalf("not creatable entry: %+v", template)
		}
	}
	if strings.Contains(rec.Body.String(), `"id":"kuaidaili-free"`) || strings.Contains(rec.Body.String(), `"id":"scraperapi"`) {
		t.Fatal("rejected products exposed as channel templates")
	}
}

func TestSourceOverviewGroupsBrandWithoutCollapsingStoredInstances(t *testing.T) {
	e := newImportIdentityServices(t)
	for _, pair := range []struct{ id, site string }{{"my-http", "proxyscrape-http"}, {"my-socks", "proxyscrape-socks5"}} {
		if _, derr := e.ingest.CreateSource(context.Background(), pair.id, application.SourceConfig{Site: &application.SiteConfig{SiteID: pair.site}}, false); derr != nil {
			t.Fatal(derr)
		}
	}
	rec := httptest.NewRecorder()
	e.server.handleSourcesOverview(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	var rows []struct {
		SourceID    string `json:"source_id"`
		ChannelID   string `json:"channel_id"`
		ChannelName string `json:"channel_name"`
	}
	if rec.Code != 200 || json.Unmarshal(rec.Body.Bytes(), &rows) != nil || len(rows) != 2 {
		t.Fatalf("overview: %d %s", rec.Code, rec.Body.String())
	}
	ids := map[string]bool{}
	for _, row := range rows {
		ids[row.SourceID] = true
		if row.ChannelID != "proxyscrape" || row.ChannelName != "ProxyScrape" {
			t.Fatalf("missing grouping metadata: %+v", row)
		}
	}
	if !ids["my-http"] || !ids["my-socks"] {
		t.Fatal("grouping lost source IDs/operations")
	}
}
