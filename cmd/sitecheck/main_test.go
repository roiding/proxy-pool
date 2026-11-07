package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"proxypool/internal/domain"
	"proxypool/internal/integrations/sites"
	"proxypool/internal/validation"
)

func TestSampleCanonicalDedupAndReproducibleSeed(t *testing.T) {
	entries := []sites.Entry{}
	for i := 1; i <= 30; i++ {
		entries = append(entries, sites.Entry{Conn: domain.Connection{Transport: domain.TransportHTTP, Host: fmt.Sprintf("8.8.8.%d", i), Port: 8080}})
	}
	entries = append(entries, entries[0])
	credential := entries[0]
	credential.Conn.Username = "secret-user"
	credential.Conn.Password = "secret-pass"
	entries = append(entries, credential)
	before := append([]sites.Entry(nil), entries...)
	a := sampleEntries(entries, 10, 42)
	reverse := append([]sites.Entry(nil), entries...)
	for i, j := 0, len(reverse)-1; i < j; i, j = i+1, j-1 {
		reverse[i], reverse[j] = reverse[j], reverse[i]
	}
	b := sampleEntries(reverse, 10, 42)
	if !reflect.DeepEqual(a, b) || !reflect.DeepEqual(before, entries) || len(sampleEntries(entries, 100, 1)) != 31 {
		t.Fatal("sample changed with page ordering, duplicate rows, or altered the input")
	}
	if reflect.DeepEqual(a, sampleEntries(entries, 10, 43)) {
		t.Fatal("seed did not affect the bounded sample")
	}
}

func TestDefaultCheckIsFetchOnlyNotProxyPass(t *testing.T) {
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1); _, _ = fmt.Fprint(w, "8.8.8.8:8080\n") }))
	defer server.Close()
	site := sites.Site{ID: "fixture", Kind: sites.ParserLines, Protocol: domain.TransportHTTP, State: sites.SiteActive, URL: server.URL, Pages: 1}
	result := checkSite(site, server.Client(), 1, 2, 0, "basic", "", "", "", 1)
	if !result.FetchOK || result.Verdict != "fetch_only" || result.Probe.Requested || result.Probe.Attempted != 0 || !requestedCheckPassed(result) || hits.Load() != 1 {
		t.Fatalf("fetch-only reported liveness or failed the requested phase: %+v", result)
	}
	encoded, _ := json.Marshal(result)
	if strings.Contains(string(encoded), "8.8.8.8") {
		t.Fatalf("raw proxy appeared in acceptance report: %s", encoded)
	}
}

func TestPausedCatalogEntryDoesNotDownload(t *testing.T) {
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
	defer server.Close()
	result := checkSite(sites.Site{ID: "paused", State: sites.SitePaused, URL: server.URL, Note: "awaiting recheck"}, server.Client(), 1, 2, 0, "basic", "", "", "", 1)
	if result.Verdict != "not_run" || requestedCheckPassed(result) || hits.Load() != 0 {
		t.Fatalf("paused source triggered network I/O: %+v hits=%d", result, hits.Load())
	}
}

func TestProbeVerdictMakesSampleCoverageExplicit(t *testing.T) {
	if probeVerdict(probePhase{Requested: true, Sample: 2, Attempted: 2, Pass: 1, Fail: 1}) != "sample_has_pass" {
		t.Fatal("partial sample was presented as all-pass")
	}
	if probeVerdict(probePhase{Requested: true, Sample: 2, Attempted: 2, Pass: 2, AllSamplePassed: true}) != "sample_all_pass" {
		t.Fatal("full sampled proof was not distinguished")
	}
	if probeVerdict(probePhase{Requested: false}) != "no_proxy_pass" {
		t.Fatal("missing probes became proxy liveness")
	}
}

func TestPausedRecheckNeverGuessesUnknownProtocolOrSchema(t *testing.T) {
	for _, id := range []string{"spys-socks", "spys-html", "jetkai-proxy-list", "7yip-html"} {
		site, ok := sites.ByID(id)
		if !ok || diagnosticRecheckAllowed(site) {
			t.Fatalf("unknown source %q can be forced into a running adapter: %+v", id, site)
		}
	}
	site, ok := sites.ByID("proxy-list-download-http")
	if !ok || !diagnosticRecheckAllowed(site) {
		t.Fatal("a paused endpoint with confirmed line parser cannot be diagnosed")
	}
}

func TestBroadDiagnosticDefaultsUseOneVariantPerAdmittedSupplier(t *testing.T) {
	seen := map[string]bool{}
	defaults := sites.DefaultSites()
	if len(defaults) != 12 {
		t.Fatalf("default checks count=%d", len(defaults))
	}
	for _, site := range defaults {
		family, ok := sites.ChannelForSite(site.ID)
		if !ok || seen[family.ID] {
			t.Fatalf("duplicate/unreviewed broad download: %+v", site)
		}
		seen[family.ID] = true
	}
	if seen["thespeedx"] || seen["kuaidaili"] || seen["roosterkid"] {
		t.Fatal("diagnostic all includes unadmitted/duplicate suppliers")
	}
}

func TestSafeLiveFailureReasonsOmitEndpointAndCredentials(t *testing.T) {
	for _, tc := range []struct{ detail, want string }{
		{"connect to proxy user:secret@8.8.8.8:8080: connection refused", "connection_refused"},
		{"read CONNECT response from secret@8.8.8.8: EOF", "invalid_proxy_protocol"},
		{"proxy CONNECT failed for 1.1.1.1:443: HTTP 503", "connect_http_503"},
		{"target TLS handshake: x509: certificate is valid for secret.invalid", "tls_certificate_rejected"},
		{"connect to proxy 8.8.8.8: i/o timeout", "timeout"},
	} {
		if got := safeProbeFailureReason(&validation.Failure{Kind: "PROXY", Detail: tc.detail}); got != tc.want {
			t.Fatalf("reason got=%s want=%s", got, tc.want)
		}
	}
}
