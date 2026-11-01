package sites

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestParseTextExtractsIPPort(t *testing.T) {
	body := []byte("junk http://10.0.0.1:8080 tail\n203.0.113.5:3128\n" +
		"198.51.100.9:99999\nnot-an-ip:80")
	out, err := parsePage(ParserText, body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(out.entries) != 2 || out.entries[1].Conn.Host != "203.0.113.5" || out.entries[1].Conn.Port != 3128 {
		t.Fatalf("text entries: %+v", out.entries)
	}
}

func TestParseJSONListShapes(t *testing.T) {
	page := map[string]any{
		"data": map[string]any{"list": []map[string]any{
			{"ip": "203.0.113.10", "port": 8080, "protocol": 1},
			{"ip": "203.0.113.11", "port": "3128", "protocol": 2},
			{"ip": "203.0.113.12", "port": 9000, "protocol": 5},
			{"ip": "999.1.1.1", "port": 80, "protocol": 1},
		}, "page_count": 3},
	}
	body, _ := json.Marshal(page)
	out, err := parsePage(ParserJSONList, body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(out.entries) != 2 || out.entries[1].VendorProtocol != "https" {
		t.Fatalf("entries: %+v", out.entries)
	}
	if len(out.rejected) != 1 || out.rejected[0].Line != 4 {
		t.Fatalf("rejected: %+v", out.rejected)
	}
	if n := autoPageCount(body); n != 3 {
		t.Fatalf("autoPageCount=%d", n)
	}

	root := []map[string]any{{"ip": "203.0.113.20", "port": 80, "protocol": 1}}
	body2, _ := json.Marshal(root)
	out2, err := parsePage(ParserJSONList, body2)
	if err != nil || len(out2.entries) != 1 {
		t.Fatalf("root array: %v %+v", err, out2.entries)
	}
}

func TestParseTableVariants(t *testing.T) {
	tdHTML := `<table><tr><td>IP</td><td>PORT</td></tr>
<tr><td>203.0.113.30</td><td>8080</td></tr>
<tr><td>ads</td><td>text</td></tr>
<tr><td>203.0.113.31</td><td>3128</td></tr></table>`
	out, err := parsePage(ParserTable2Cols, []byte(tdHTML))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(out.entries) != 2 || out.entries[0].Conn.Host != "203.0.113.30" {
		t.Fatalf("td entries: %+v", out.entries)
	}

	threeHTML := `<table><tr><td>rank</td><td>203.0.113.32</td><td>80</td></tr></table>`
	out3, _ := parsePage(ParserTable3Cols, []byte(threeHTML))
	if len(out3.entries) != 1 || out3.entries[0].Conn.Port != 80 {
		t.Fatalf("3col entries: %+v", out3.entries)
	}

	combinedHTML := `<table><tr><td>203.0.113.33:8080</td><td>other</td></tr></table>`
	outIP, _ := parsePage(ParserTableIPPort, []byte(combinedHTML))
	if len(outIP.entries) != 1 || outIP.entries[0].Conn.Port != 8080 {
		t.Fatalf("ipport entries: %+v", outIP.entries)
	}

	thHTML := `<table><tr><th>203.0.113.34</th><th>8888</th></tr></table>`
	outTH, _ := parsePage(ParserTableTH, []byte(thHTML))
	if len(outTH.entries) != 1 || outTH.entries[0].Conn.Port != 8888 {
		t.Fatalf("th entries: %+v", outTH.entries)
	}

	roleHTML := `<div role="rowgroup">
<div role="row"><span role="columnheader">IP</span><span role="columnheader">端口</span></div>
<div role="row"><span role="cell" title="203.0.113.35">203.0.113.35</span><span role="cell">8080</span><span role="cell">高匿名</span></div>
<div role="row"><span role="cell" title="203.0.113.36">203.0.113.36</span><span role="cell">3128</span><span role="cell">HTTP</span></div>
</div>`
	outRole, _ := parsePage(ParserRoleCells, []byte(roleHTML))
	if len(outRole.entries) != 2 || outRole.entries[0].Conn.Host != "203.0.113.35" || outRole.entries[1].Conn.Port != 3128 {
		t.Fatalf("role entries: %+v", outRole.entries)
	}
}

func TestParseFPSListAndFineProxy(t *testing.T) {
	fps := []byte("var x=1;\nconst fpsList = [{\"ip\":\"203.0.113.40\",\"port\":\"8080\"}," +
		"{\"ip\":\"10.0.0.1\",\"port\":80},{\"ip\":\"203.0.113.40\",\"port\":\"8080\"}];")
	out, err := parsePage(ParserFPSList, fps)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(out.entries) != 2 || out.entries[0].Conn.Port != 8080 {
		t.Fatalf("fps entries: %+v", out.entries)
	}

	rows := `<tr><td class="table-ip">203.0.113.41</td><td>3128</td></tr>`
	payload, _ := json.Marshal(map[string]any{"data": map[string]any{"rows": rows}})
	outFine, err := parsePage(ParserFineProxy, payload)
	if err != nil || len(outFine.entries) != 1 {
		t.Fatalf("fineproxy: %v %+v", err, outFine.entries)
	}
}

func TestFetchSitePagesDedupeAndStop(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		page := r.URL.Query().Get("page")
		w.Header().Set("Content-Type", "application/json")
		switch page {
		case "1":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{
				"list": []map[string]any{
					{"ip": "203.0.113.50", "port": 80, "protocol": 1},
					{"ip": "10.0.0.1", "port": 80, "protocol": 1},
				}, "page_count": 2}})
		case "2":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{
				"list": []map[string]any{
					{"ip": "203.0.113.50", "port": 80, "protocol": 1},
					{"ip": "203.0.113.51", "port": 80, "protocol": 1},
				}}})
		default:
			_, _ = w.Write([]byte("no data"))
		}
	}))
	defer srv.Close()
	site := Site{ID: "test", Name: "test", Kind: ParserJSONList, URL: srv.URL + "/?page={page}", AutoPages: true, Pages: 1}
	out, err := FetchSite(context.Background(), srv.Client(), site, 0, false)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(out.Entries) != 2 || out.PagesDone < 2 {
		t.Fatalf("entries=%d pages=%d/%d calls=%d rejected=%+v",
			len(out.Entries), out.PagesDone, out.PagesTotal, calls, out.Rejected)
	}
	privateRejected := false
	for _, r := range out.Rejected {
		if r.Input == "10.0.0.1:80" {
			privateRejected = true
		}
	}
	if !privateRejected {
		t.Fatalf("private address not rejected: %+v", out.Rejected)
	}
}

func TestFetchSiteFailsWhenEveryPageErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusBadGateway)
	}))
	defer srv.Close()
	site := Site{ID: "test", Name: "test", Kind: ParserText, URL: srv.URL, Pages: 2, DelayMS: 1}
	if _, err := FetchSite(context.Background(), srv.Client(), site, 0, false); err == nil {
		t.Fatal("fetch with zero usable pages must fail")
	}
}

func TestCatalogUniqueIDsAndPageBounds(t *testing.T) {
	seen := map[string]bool{}
	for _, s := range Catalog {
		if s.ID == "" || seen[s.ID] {
			t.Fatalf("bad or duplicate site id %q", s.ID)
		}
		seen[s.ID] = true
		if s.Pages > MaxPages {
			t.Fatalf("site %s pages %d exceed cap", s.ID, s.Pages)
		}
		paginates := strings.Contains(s.URL, "{page}") ||
			strings.EqualFold(s.Method, http.MethodPost) && strings.Contains(s.Form["page"], "{page}")
		if s.Pages > 1 && !paginates {
			t.Fatalf("site %s paginates without a {page} placeholder", s.ID)
		}
		if _, ok := ByID(s.ID); !ok {
			t.Fatalf("ByID missing %s", s.ID)
		}
	}
	if _, ok := ByID("missing"); ok {
		t.Fatal("ByID returned ok for unknown id")
	}
	// 2026-11-01 server-verified live set; every entry must come from that
	// check so dead sources cannot silently reappear.
	verified := map[string]bool{
		"ip3366-normal": true, "ip89": true, "jiliuip-elite": true, "qiyunip-elite": true,
		"lumiproxy-transparent": true, "lumiproxy-anon": true, "lumiproxy-elite": true,
		"proxyscrape-anon": true, "proxyscrape-elite": true,
		"openproxylist-http": true, "openproxylist-https": true,
	}
	for id := range seen {
		if !verified[id] {
			t.Fatalf("catalog id %q is not in the verified live set", id)
		}
	}
	if len(Catalog) != len(verified) {
		t.Fatalf("catalog has %d entries, want %d verified", len(Catalog), len(verified))
	}
}
