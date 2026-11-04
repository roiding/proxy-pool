package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"proxypool/internal/application"
	"proxypool/internal/config"
	"proxypool/internal/domain"
)

func auditWireObject(t *testing.T, raw []byte) map[string]json.RawMessage {
	t.Helper()
	var out map[string]json.RawMessage
	if err := json.Unmarshal(raw, &out); err != nil || out == nil {
		t.Fatalf("expected a JSON object: body=%s err=%v", raw, err)
	}
	return out
}

func auditWireUTC(t *testing.T, fields map[string]json.RawMessage, name string) time.Time {
	t.Helper()
	var text string
	if err := json.Unmarshal(fields[name], &text); err != nil || text == "" {
		t.Errorf("missing RFC3339 timestamp %s: value=%s err=%v", name, fields[name], err)
		return time.Time{}
	}
	stamp, err := time.Parse(time.RFC3339Nano, text)
	if err != nil || !strings.HasSuffix(text, "Z") {
		t.Errorf("%s must be UTC RFC3339, not a local offset: value=%q err=%v", name, text, err)
	}
	return stamp
}

func TestAuditAcquireCanonicalJSONResponse(t *testing.T) {
	cases := []struct {
		name     string
		count    int
		user     string
		password string
		knownTTL bool
		empty    bool
	}{
		{name: "empty", count: 3, empty: true},
		{name: "partial_with_encoded_auth", count: 2, user: "u ser:@/%", password: "p@ss:/% ?#", knownTTL: true},
		{name: "full_with_empty_password", count: 1, user: "user", password: ""},
		{name: "full_without_auth", count: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, startJudge(t), nil)
			const sourceID = "audit-wire-custom"
			var expectedConn domain.Connection
			var providerExpiry time.Time
			if !tc.empty {
				proxy := startFakeProxy(t)
				expectedConn = domain.Connection{Transport: domain.TransportHTTP,
					Host: "127.0.0.1", Port: portOf(proxy.URL), Username: tc.user, Password: tc.password}
				u := &url.URL{Scheme: "http", Host: net.JoinHostPort(expectedConn.Host, strconv.Itoa(expectedConn.Port))}
				if tc.user != "" || tc.password != "" {
					u.User = url.UserPassword(tc.user, tc.password)
				}
				e.importProxy(t, sourceID, u.String())
				if tc.knownTTL {
					providerExpiry = time.Now().UTC().Add(2 * time.Minute)
					if _, err := e.store.DB().Exec(`UPDATE source_offers SET provider_expires_at = ? WHERE source_id = ?`,
						providerExpiry.Format(time.RFC3339Nano), sourceID); err != nil {
						t.Fatal(err)
					}
				}
			}
			srv := auditServer(t, e)
			req, err := http.NewRequest(http.MethodPost, srv.URL+"/api/v1/proxies/acquire",
				strings.NewReader(fmt.Sprintf(`{"count":%d,"timeout":1,"cd":0}`, tc.count)))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", "Bearer "+e.cfg.APIToken)
			req.Header.Set("Content-Type", "application/json")
			resp, err := srv.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("valid acquire returned HTTP %d", resp.StatusCode)
			}
			if !strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") ||
				!strings.Contains(resp.Header.Get("Cache-Control"), "no-store") {
				t.Errorf("credential-bearing JSON must prohibit shared caching: headers=%v", resp.Header)
			}
			var fields map[string]json.RawMessage
			if err := json.NewDecoder(resp.Body).Decode(&fields); err != nil {
				t.Fatal(err)
			}
			var items []json.RawMessage
			if err := json.Unmarshal(fields["items"], &items); err != nil || items == nil {
				t.Fatalf("items must be a JSON array: value=%s err=%v", fields["items"], err)
			}
			var requested, delivered, missing int
			var partial bool
			for name, ptr := range map[string]*int{"requested": &requested, "delivered": &delivered, "missing": &missing} {
				if err := json.Unmarshal(fields[name], ptr); err != nil {
					t.Fatalf("invalid count field %s: %v", name, err)
				}
			}
			if err := json.Unmarshal(fields["partial"], &partial); err != nil {
				t.Fatal(err)
			}
			wantDelivered := 1
			if tc.empty {
				wantDelivered = 0
			}
			if requested != tc.count || delivered != wantDelivered || delivered != len(items) ||
				missing != requested-delivered || partial != (delivered > 0 && delivered < requested) {
				t.Fatalf("inconsistent delivery counts: requested=%d delivered=%d missing=%d partial=%v items=%d",
					requested, delivered, missing, partial, len(items))
			}
			if _, old := fields["shortfall_reason"]; old {
				t.Error("legacy shortfall_reason alias must not be serialized")
			}
			if missing > 0 {
				var reason string
				if json.Unmarshal(fields["reason"], &reason) != nil || reason == "" {
					t.Error("a shortfall must contain one nonempty reason")
				}
			} else if _, exists := fields["reason"]; exists {
				t.Error("full delivery must omit reason")
			}
			for _, name := range []string{"session_id", "binding_version", "session_changed", "binding_expires_at", "change_reason"} {
				if _, exists := fields[name]; exists {
					t.Errorf("ordinary response leaked sticky-only field %s", name)
				}
			}
			for _, raw := range items {
				item := auditWireObject(t, raw)
				var conn domain.Connection
				if err := json.Unmarshal(item["connection"], &conn); err != nil || conn != expectedConn {
					t.Fatalf("canonical raw connection was altered: got=%+v expected=%+v err=%v", conn, expectedConn, err)
				}
				connection := auditWireObject(t, item["connection"])
				if _, old := connection["protocol"]; old {
					t.Error("connection.protocol alias must not be serialized")
				}
				if tc.user != "" || tc.password != "" {
					if _, present := connection["password"]; !present {
						t.Error("an authenticated connection must represent its actual empty or nonempty password")
					}
				} else if _, present := connection["username"]; present {
					t.Error("an anonymous connection must not invent authentication")
				}
				var proxyURI string
				if err := json.Unmarshal(item["proxy_uri"], &proxyURI); err != nil || proxyURI == "" {
					t.Errorf("missing proxy_uri: err=%v", err)
				} else {
					u, err := url.Parse(proxyURI)
					if err != nil || u.Scheme != string(conn.Transport) || u.Hostname() != conn.Host || u.Port() != strconv.Itoa(conn.Port) {
						t.Errorf("proxy_uri does not represent connection: uri=%q err=%v", proxyURI, err)
					} else if tc.user != "" || tc.password != "" {
						if u.User == nil {
							t.Error("proxy_uri lost authentication")
						} else if password, present := u.User.Password(); !present || u.User.Username() != tc.user || password != tc.password {
							t.Errorf("proxy_uri credentials were escaped incorrectly: uri=%q", proxyURI)
						}
					} else if u.User != nil {
						t.Error("anonymous proxy_uri invented userinfo")
					}
				}
				validation := auditWireObject(t, item["validation"])
				completed := auditWireUTC(t, validation, "completed_at")
				validUntil := auditWireUTC(t, validation, "valid_until")
				if !completed.IsZero() && !validUntil.Equal(completed.Add(e.cfg.MaxProofAge())) {
					t.Errorf("proof validity was replaced by a lease/cache deadline: completed=%s valid_until=%s", completed, validUntil)
				}
				leaseExpiry := auditWireUTC(t, item, "lease_expires_at")
				if !leaseExpiry.After(completed) {
					t.Error("delivered lease is already expired")
				}
				provider, present := item["provider_expires_at"]
				if !present {
					t.Error("provider_expires_at must be present, with null for unknown expiry")
				} else if tc.knownTTL {
					if stamp := auditWireUTC(t, item, "provider_expires_at"); !stamp.Equal(providerExpiry) || stamp.Equal(leaseExpiry) {
						t.Errorf("provider expiry was not the original supplier deadline: got=%s expected=%s", stamp, providerExpiry)
					}
				} else if string(provider) != "null" {
					t.Errorf("unknown supplier expiry was invented: %s", provider)
				}
				observed := auditWireObject(t, item["observed"])
				var exitIP string
				if json.Unmarshal(observed["exit_ip"], &exitIP) != nil || exitIP != "203.0.113.7" {
					t.Errorf("observed exit must come from the actual judge result: %s", observed["exit_ip"])
				}
				_ = auditWireUTC(t, observed, "observed_at")
			}
		})
	}
}

func TestAuditStickyWireSeparatesBindingLeaseAndProofExpiry(t *testing.T) {
	e := newEnv(t, startJudge(t), func(c *config.Config) {
		c.StickyBindingTTLMS = 90000
		c.LeaseTTLMS = 30000
		c.MaxProofAgeMS = 5000
		c.EvidenceCacheTTLMS = 5000
	})
	e.importProxy(t, "audit-sticky-wire", startFakeProxy(t).URL)
	srv := auditServer(t, e)
	const sessionID = "audit-wire-session"
	body := `{"count":1,"timeout":2,"cd":600,"mode":"sticky","session_id":"` + sessionID + `"}`
	var originalVersion int64
	var originalExpiry time.Time
	for call := 0; call < 2; call++ {
		status, data := auditPost(t, srv, e.cfg.APIToken, "/api/v1/proxies/acquire", body)
		if status != http.StatusOK {
			t.Fatalf("sticky request failed: status=%d body=%s", status, data)
		}
		fields := auditWireObject(t, data)
		var items []json.RawMessage
		if err := json.Unmarshal(fields["items"], &items); err != nil || len(items) != 1 {
			t.Fatalf("sticky request did not deliver one connection: body=%s err=%v", data, err)
		}
		var changed bool
		if err := json.Unmarshal(fields["session_changed"], &changed); err != nil {
			t.Errorf("sticky session_changed must be an explicit boolean, including false: value=%s err=%v", fields["session_changed"], err)
		}
		var version int64
		if err := json.Unmarshal(fields["binding_version"], &version); err != nil || version <= 0 {
			t.Fatalf("invalid sticky binding version: value=%s err=%v", fields["binding_version"], err)
		}
		bindingExpiry := auditWireUTC(t, fields, "binding_expires_at")
		binding, err := e.store.GetBinding(context.Background(), application.DefaultAcquireProject, sessionID)
		if err != nil || !bindingExpiry.Equal(binding.Deadline) {
			t.Errorf("wire binding expiry differs from durable binding: wire=%s database=%s err=%v", bindingExpiry, binding.Deadline, err)
		}
		item := auditWireObject(t, items[0])
		leaseExpiry := auditWireUTC(t, item, "lease_expires_at")
		validation := auditWireObject(t, item["validation"])
		proofExpiry := auditWireUTC(t, validation, "valid_until")
		if !bindingExpiry.After(leaseExpiry) || !leaseExpiry.After(proofExpiry) {
			t.Errorf("three independent deadlines were collapsed: proof=%s lease=%s binding=%s", proofExpiry, leaseExpiry, bindingExpiry)
		}
		for _, name := range []string{"session_changed", "session_binding_version", "session_change_reason"} {
			if _, exists := item[name]; exists {
				t.Errorf("sticky item duplicates top-level session field %s", name)
			}
		}
		if call == 0 {
			originalVersion, originalExpiry = version, bindingExpiry
		} else if changed || version != originalVersion || !bindingExpiry.Equal(originalExpiry) {
			t.Error("healthy reuse changed or renewed the existing logical binding")
		}
	}
}
