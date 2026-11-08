package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"proxypool/internal/application"
)

func TestFixedChannelAPIUsesOneSwitchAndRequiresAdmin(t *testing.T) {
	e := newImportIdentityServices(t)
	handler := e.server.Handler()
	request := func(method, path, body string, authenticated bool) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		if authenticated {
			req.Header.Set("Authorization", "Bearer "+e.server.cfg.APIToken)
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}
	if rec := request(http.MethodGet, "/api/v1/admin/channels", "", false); rec.Code != 401 {
		t.Fatalf("anonymous catalog: %d", rec.Code)
	}
	rec := request(http.MethodGet, "/api/v1/admin/channels", "", true)
	var channels []application.FixedChannel
	if rec.Code != 200 || json.Unmarshal(rec.Body.Bytes(), &channels) != nil || len(channels) != 14 {
		t.Fatalf("fixed catalog: %d %s", rec.Code, rec.Body.String())
	}
	var channel application.FixedChannel
	for _, c := range channels {
		if c.ID == "scdn" {
			channel = c
		}
	}
	body, _ := json.Marshal(map[string]any{"enabled": true, "version": channel.Version})
	rec = request(http.MethodPatch, "/api/v1/admin/channels/scdn", string(body), true)
	if rec.Code != 200 || json.Unmarshal(rec.Body.Bytes(), &channels) != nil {
		t.Fatalf("switch: %d %s", rec.Code, rec.Body.String())
	}
	for _, c := range channels {
		if c.ID == "scdn" && (!c.Enabled || len(c.SourceIDs) != 2 || c.PartiallyEnabled) {
			t.Fatalf("protocols not controlled together: %+v", c)
		}
	}
	if rec := request(http.MethodPatch, "/api/v1/admin/channels/scdn", string(body), true); rec.Code != 409 {
		t.Fatalf("stale switch: %d %s", rec.Code, rec.Body.String())
	}
	if rec := request(http.MethodPatch, "/api/v1/admin/channels/scdn", `{"enabled":false}`, true); rec.Code != 400 {
		t.Fatalf("unguarded switch: %d", rec.Code)
	}
}
