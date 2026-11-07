package sites

// Parser-level tests for every catalog parser shape. All fixtures are
// synthetic bodies based on documented/vendor response shapes; nothing here
// performs network I/O. Documentation/private addresses are only fed to the
// parser with AllowPrivate enabled; policy rejection cases assert that
// RowError.Input stays empty so no raw row ever leaks.
//
// Fixture contract for parser implementations (kind => accepted shape):
//   - text/lines: "ip:port" or scheme://[user:pass@]ip:port lines, '#' comments,
//     blank input is a recognized empty page; every bad row must reject.
//   - json_list: {"data":{"list":[{ip,port,protocol 1|2}],"page_count":N}}
//     (data.list, list, root array accepted); protocol 5+ must reject.
//   - table_ipport: one <td> holds "ip:port"; non-matching cells are skipped.
//   - table_2col/table_3col/table_th: header-aware tables (ip/port headers
//     skipped); invalid ports must reject, not vanish.
//   - role_cells: role=row with role=cell descendants, cells[0]=ip cells[1]=port.
//   - fpslist: "const fpsList = [...]" embedded JSON.
//   - fineproxy: {"data":{"rows":"<tr><td class=\"table-ip\">ip</td><td>port</td></tr>"}}.
//   - spys_text: leading "ip:port cc-anon S domain time" header line is not a
//     data row; first column ip:port; metadata kept as claims.
//   - geonode_json: {"data":[{ip,port,protocols[],lastChecked seconds}]};
//     rows whose protocols do not match opts.Protocol must not enter entries.
//   - freeproxy: free-proxy-list.net table (IP Address/Port header); the
//     socks variant carries an explicit Version column (Socks5 only).
//   - ihuan_html: <a data-ip="..." data-port="..."> anchors inside a table;
//     a table without anchors is a recognized empty page, not an error.
//   - ip89_html: two-column <td>ip</td><td>port</td> table with a header row.
//   - kuaidaili: kuaidaili free table with data-title attributes.
//   - table_html: header-aware table with an optional Protocol column.

import (
	"encoding/json"
	"strings"
	"testing"

	"proxypool/internal/domain"
)

// Test-only bridge keeps string fixture constructors readable; production accepts bytes.
func parseFixture(opts ParseOptions, kind ParserKind, body any) (ParseOutput, error) {
	switch b := body.(type) {
	case string:
		return ParseBody(opts, kind, []byte(b))
	case []byte:
		return ParseBody(opts, kind, b)
	default:
		panic("invalid fixture type")
	}
}

const (
	fixtureIP1 = "93.184.216.34"
	fixtureIP2 = "93.184.216.35"
	fixtureIP3 = "93.184.216.36"
)

var allParserKinds = []ParserKind{
	ParserText, ParserJSONList, ParserTableIPPort, ParserTable2Cols,
	ParserTable3Cols, ParserTableTH, ParserRoleCells, ParserFPSList,
	ParserFineProxy, ParserLines, ParserSpysText, ParserGeonodeJSON,
	ParserFreeProxy, ParserIHuanHTML, ParserIP89HTML, ParserKuaidaili,
	ParserTableHTML,
}

func TestParseTextExtractsIPPortAndCountsRejects(t *testing.T) {
	body := fixtureIP1 + ":8080\n" + fixtureIP2 + ":3128\n" +
		fixtureIP3 + ":99999\ngarbage line\n"
	out, err := parseFixture(ParseOptions{}, ParserText, body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(out.Entries) != 2 || out.Entries[1].Conn.Host != fixtureIP2 || out.Entries[1].Conn.Port != 3128 {
		t.Fatalf("entries: %+v", out.Entries)
	}
	if out.RejectedCount != 2 || len(out.Rejected) != 2 {
		t.Fatalf("rejected: count=%d kept=%d", out.RejectedCount, len(out.Rejected))
	}
	for _, r := range out.Rejected {
		if r.Input != "" {
			t.Fatalf("rejected input leaked: %+v", r)
		}
	}
}

func TestParseLinesCredentialsAndProtocolFilter(t *testing.T) {
	body := "socks5://user:secret@" + fixtureIP1 + ":1080\n" +
		fixtureIP2 + ":3128\nhttp://" + fixtureIP3 + ":8080\n"
	out, err := parseFixture(ParseOptions{DefaultTransport: domain.TransportSOCKS5}, ParserLines, body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(out.Entries) != 3 {
		t.Fatalf("entries: %+v", out.Entries)
	}
	if out.Entries[0].Conn.Username != "user" || out.Entries[0].Conn.Password != "secret" ||
		out.Entries[0].Conn.Transport != domain.TransportSOCKS5 {
		t.Fatalf("credential line: %+v", out.Entries[0])
	}

	mismatch, err := parseFixture(ParseOptions{Protocol: domain.TransportHTTP}, ParserLines,
		"socks5://"+fixtureIP1+":1080\n")
	if err != nil {
		t.Fatalf("mismatch parse: %v", err)
	}
	if len(mismatch.Entries) != 0 || mismatch.RejectedCount != 1 {
		t.Fatalf("protocol mismatch must reject: %+v", mismatch)
	}
}

func TestParseJSONListShapes(t *testing.T) {
	page := map[string]any{
		"data": map[string]any{"list": []map[string]any{
			{"ip": fixtureIP1, "port": 8080, "protocol": 1},
			{"ip": fixtureIP2, "port": "3128", "protocol": 2},
			{"ip": fixtureIP3, "port": 9000, "protocol": 5},
			{"ip": "300.1.1.1", "port": 80, "protocol": 1},
		}, "page_count": 3},
	}
	body, _ := json.Marshal(page)
	out, err := parseFixture(ParseOptions{}, ParserJSONList, body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(out.Entries) != 2 || out.Entries[1].VendorProtocol != "https" {
		t.Fatalf("entries: %+v", out.Entries)
	}
	if out.Entries[1].Claims["protocol"] != "https" || out.Entries[0].Claims["protocol"] != "http" {
		t.Fatalf("protocol claims: %+v", out.Entries)
	}
	if out.RejectedCount != 2 {
		t.Fatalf("protocol 5 and bad ip must reject: %+v", out.Rejected)
	}
	if n := autoPageCount(body); n != 3 {
		t.Fatalf("autoPageCount=%d", n)
	}

	root := []map[string]any{{"ip": fixtureIP1, "port": 80, "protocol": 1}}
	body2, _ := json.Marshal(root)
	out2, err := parseFixture(ParseOptions{}, ParserJSONList, body2)
	if err != nil || len(out2.Entries) != 1 {
		t.Fatalf("root array: %v %+v", err, out2.Entries)
	}
}

func TestParseTableVariants(t *testing.T) {
	tdHTML := `<table><tr><td>IP</td><td>PORT</td></tr>
<tr><td>` + fixtureIP1 + `</td><td>8080</td></tr>
<tr><td>ads</td><td>text</td></tr>
<tr><td>` + fixtureIP2 + `</td><td>3128</td></tr></table>`
	out, err := parseFixture(ParseOptions{}, ParserTable2Cols, tdHTML)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(out.Entries) != 2 || out.Entries[0].Conn.Host != fixtureIP1 {
		t.Fatalf("td entries: %+v", out.Entries)
	}

	threeHTML := `<table><tr><td>rank</td><td>` + fixtureIP2 + `</td><td>80</td></tr></table>`
	out3, err := parseFixture(ParseOptions{}, ParserTable3Cols, threeHTML)
	if err != nil || len(out3.Entries) != 1 || out3.Entries[0].Conn.Port != 80 {
		t.Fatalf("3col: %v %+v", err, out3.Entries)
	}

	combinedHTML := `<table><tr><td>` + fixtureIP3 + `:8080</td><td>other</td></tr></table>`
	outIP, err := parseFixture(ParseOptions{}, ParserTableIPPort, combinedHTML)
	if err != nil || len(outIP.Entries) != 1 || outIP.Entries[0].Conn.Port != 8080 {
		t.Fatalf("ipport: %v %+v", err, outIP.Entries)
	}

	thHTML := `<table><tr><th>ip</th><th>port</th></tr><tr><th>` + fixtureIP1 + `</th><th>8888</th></tr></table>`
	outTH, err := parseFixture(ParseOptions{}, ParserTableTH, thHTML)
	if err != nil || len(outTH.Entries) != 1 || outTH.Entries[0].Conn.Port != 8888 {
		t.Fatalf("th: %v %+v", err, outTH.Entries)
	}

	roleHTML := `<div role="rowgroup">
<div role="row"><span role="columnheader">IP</span><span role="columnheader">端口</span></div>
<div role="row"><span role="cell" title="` + fixtureIP1 + `">` + fixtureIP1 + `</span><span role="cell">8080</span><span role="cell">高匿名</span></div>
<div role="row"><span role="cell" title="` + fixtureIP2 + `">` + fixtureIP2 + `</span><span role="cell">3128</span><span role="cell">HTTP</span></div>
</div>`
	outRole, err := parseFixture(ParseOptions{}, ParserRoleCells, roleHTML)
	if err != nil || len(outRole.Entries) != 2 || outRole.Entries[0].Conn.Host != fixtureIP1 ||
		outRole.Entries[1].Conn.Port != 3128 {
		t.Fatalf("role: %v %+v", err, outRole.Entries)
	}
}

func TestParseFPSListAndFineProxy(t *testing.T) {
	fps := "var x=1;\nconst fpsList = [{\"ip\":\"" + fixtureIP1 + "\",\"port\":\"8080\"}," +
		"{\"ip\":\"" + fixtureIP2 + "\",\"port\":3128}];"
	out, err := parseFixture(ParseOptions{}, ParserFPSList, fps)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(out.Entries) != 2 || out.Entries[0].Conn.Port != 8080 {
		t.Fatalf("fps entries: %+v", out.Entries)
	}

	rows := `<tr><td class="table-ip">` + fixtureIP1 + `</td><td>3128</td></tr>`
	payload, _ := json.Marshal(map[string]any{"data": map[string]any{"rows": rows}})
	outFine, err := parseFixture(ParseOptions{}, ParserFineProxy, payload)
	if err != nil || len(outFine.Entries) != 1 {
		t.Fatalf("fineproxy: %v %+v", err, outFine.Entries)
	}
}

func TestParseSpysText(t *testing.T) {
	body := "Proxy list\nip:port CC-Anon-S Domain code time\n" +
		fixtureIP1 + ":8080 US-N S! - 00:05\n" +
		fixtureIP2 + ":3128 NL-! H - 00:06\n"
	out, err := parseFixture(ParseOptions{}, ParserSpysText, body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(out.Entries) != 2 || out.Entries[0].Conn.Host != fixtureIP1 || out.Entries[0].Conn.Port != 8080 {
		t.Fatalf("spys entries: %+v", out.Entries)
	}
}

func TestParseGeonodeJSON(t *testing.T) {
	body := `{"data":[` +
		`{"ip":"` + fixtureIP1 + `","port":8080,"protocols":["HTTP"],"anonymityLevel":"elite","lastChecked":1700000000},` +
		`{"ip":"` + fixtureIP2 + `","port":3128,"protocols":["SOCKS5"],"anonymityLevel":"transparent","lastChecked":1699990000}],` +
		`"total":2,"page":1,"limit":50}`
	out, err := parseFixture(ParseOptions{Protocol: domain.TransportHTTP}, ParserGeonodeJSON, body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(out.Entries) != 1 || out.Entries[0].Conn.Host != fixtureIP1 {
		t.Fatalf("http-filtered entries: %+v", out.Entries)
	}
	if out.SourceUpdatedAt == nil {
		t.Fatal("lastChecked seconds must surface as SourceUpdatedAt")
	}
	socks, err := parseFixture(ParseOptions{Protocol: domain.TransportSOCKS5}, ParserGeonodeJSON, body)
	if err != nil || len(socks.Entries) != 1 || socks.Entries[0].Conn.Port != 3128 {
		t.Fatalf("socks-filtered: %v %+v", err, socks.Entries)
	}
}

func TestParseFreeProxyTable(t *testing.T) {
	header := `<thead><tr><th class="th1">IP Address</th><th class="th1">Port</th>` +
		`<th class="th1">Code</th><th class="th1">Country</th><th class="th1">Anonymity</th>` +
		`<th class="th1">Google</th><th class="th1">Https</th><th class="th1">Last Checked</th>`
	httpTable := `<table class="table table-striped table-bordered">` + header + `</tr></thead><tbody>` +
		`<tr><td>` + fixtureIP1 + `</td><td>8080</td><td>US</td><td>United States</td>` +
		`<td>elite proxy</td><td>no</td><td>no</td><td>3 mins ago</td></tr>` +
		`<tr><td>` + fixtureIP2 + `</td><td>3128</td><td>NL</td><td>Netherlands</td>` +
		`<td>anonymous</td><td>yes</td><td>yes</td><td>1 min ago</td></tr>` +
		`</tbody></table>`
	out, err := parseFixture(ParseOptions{}, ParserFreeProxy, httpTable)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(out.Entries) != 2 || out.Entries[0].Conn.Host != fixtureIP1 {
		t.Fatalf("freeproxy entries: %+v", out.Entries)
	}

	socksTable := `<table class="table table-striped table-bordered">` + header +
		`<th class="th1">Version</th></tr></thead><tbody>` +
		`<tr><td>` + fixtureIP1 + `</td><td>1080</td><td>US</td><td>United States</td>` +
		`<td>elite proxy</td><td>no</td><td>no</td><td>3 mins ago</td><td>Socks5</td></tr>` +
		`<tr><td>` + fixtureIP2 + `</td><td>1080</td><td>NL</td><td>Netherlands</td>` +
		`<td>anonymous</td><td>no</td><td>no</td><td>2 mins ago</td><td>Socks4</td></tr>` +
		`</tbody></table>`
	socks, err := parseFixture(ParseOptions{Protocol: domain.TransportSOCKS5}, ParserFreeProxy, socksTable)
	if err != nil {
		t.Fatalf("socks parse: %v", err)
	}
	if len(socks.Entries) != 1 || socks.Entries[0].Conn.Transport != domain.TransportSOCKS5 {
		t.Fatalf("socks entries: %+v", socks.Entries)
	}
	if socks.RejectedCount != 1 {
		t.Fatalf("Socks4 row must reject: %+v", socks.Rejected)
	}
}

func TestParseIHuanIP89KuaidailiAndTableHTML(t *testing.T) {
	ihuan := `<table class="table"><tbody>` +
		`<tr><td><a data-ip="` + fixtureIP1 + `" data-port="8080">` + fixtureIP1 + `</a></td></tr>` +
		`<tr><td><a data-ip="` + fixtureIP2 + `" data-port="3128">` + fixtureIP2 + `</a></td></tr>` +
		`</tbody></table>`
	out, err := parseFixture(ParseOptions{}, ParserIHuanHTML, ihuan)
	if err != nil || len(out.Entries) != 2 || out.Entries[1].Conn.Port != 3128 {
		t.Fatalf("ihuan: %v %+v", err, out.Entries)
	}

	ip89 := `<table class="table table-bordered table-striped">` +
		`<thead><tr><th>IP地址</th><th>端口</th></tr></thead><tbody>` +
		`<tr><td>` + fixtureIP1 + `</td><td>8080</td></tr>` +
		`<tr><td>` + fixtureIP2 + `</td><td>3128</td></tr>` +
		`</tbody></table>`
	out89, err := parseFixture(ParseOptions{}, ParserIP89HTML, ip89)
	if err != nil || len(out89.Entries) != 2 || out89.Entries[0].Conn.Port != 8080 {
		t.Fatalf("ip89: %v %+v", err, out89.Entries)
	}

	kuai := `<div id="list"><table><thead><tr><th>IP</th><th>PORT</th><th>匿名度</th><th>类型</th><th>位置</th><th>响应速度</th><th>最后验证时间</th></tr></thead><tbody>` +
		`<tr><td data-title="IP">` + fixtureIP1 + `</td><td data-title="PORT">8080</td>` +
		`<td data-title="匿名度">高匿名</td><td data-title="类型">HTTP</td><td data-title="位置">美国</td>` +
		`<td data-title="响应速度">0.5秒</td><td data-title="最后验证时间">24-10-01 12:00:00</td></tr>` +
		`</tbody></table>`
	outKuai, err := parseFixture(ParseOptions{}, ParserKuaidaili, kuai)
	if err != nil || len(outKuai.Entries) != 1 || outKuai.Entries[0].Conn.Host != fixtureIP1 {
		t.Fatalf("kuaidaili: %v %+v", err, outKuai.Entries)
	}

	tableHTML := `<table><thead><tr><th>IP</th><th>Port</th><th>Protocol</th></tr></thead><tbody>` +
		`<tr><td>` + fixtureIP1 + `</td><td>8080</td><td>HTTP</td></tr>` +
		`</tbody></table>`
	outHTML, err := parseFixture(ParseOptions{}, ParserTableHTML, tableHTML)
	if err != nil || len(outHTML.Entries) != 1 || outHTML.Entries[0].Conn.Port != 8080 {
		t.Fatalf("table_html: %v %+v", err, outHTML.Entries)
	}
}

func TestParseRejectsPrivateAndDocumentationIPsWithoutAllowPrivate(t *testing.T) {
	body := "10.0.0.1:8080\n203.0.113.50:3128\n192.168.1.1:80\n" + fixtureIP1 + ":8080\n"
	out, err := parseFixture(ParseOptions{}, ParserLines, body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(out.Entries) != 1 || out.Entries[0].Conn.Host != fixtureIP1 {
		t.Fatalf("entries: %+v", out.Entries)
	}
	if out.RejectedCount != 3 {
		t.Fatalf("private/doc addresses must reject: %+v", out.Rejected)
	}
	for _, r := range out.Rejected {
		if r.Input != "" {
			t.Fatalf("rejected input leaked: %+v", r)
		}
	}

	allowed, err := parseFixture(ParseOptions{AllowPrivate: true}, ParserLines,
		"10.0.0.1:8080\n203.0.113.50:3128\n")
	if err != nil || len(allowed.Entries) != 2 {
		t.Fatalf("allowPrivate fixtures: %v %+v", err, allowed.Entries)
	}
}

func TestParseEveryKindRejectsAllBadRowsAsError(t *testing.T) {
	for _, kind := range allParserKinds {
		site := Site{ID: "probe", Kind: kind}
		body := fixtureFor(site, "malformed")
		if strings.Contains(body, "\x00") {
			t.Fatalf("kind %s has no malformed fixture", kind)
		}
		if _, err := parseFixture(ParseOptions{}, kind, body); err == nil {
			t.Fatalf("kind %s: all-bad rows must error, not succeed empty", kind)
		}
	}
}

func TestParseKnownEmptyVersusUnknownBlank(t *testing.T) {
	for _, kind := range allParserKinds {
		site := Site{ID: "probe", Kind: kind}
		out, err := parseFixture(ParseOptions{}, kind, fixtureFor(site, "empty"))
		if err != nil {
			t.Fatalf("kind %s: structural empty must parse: %v", kind, err)
		}
		if len(out.Entries) != 0 {
			t.Fatalf("kind %s: empty fixture yielded entries: %+v", kind, out.Entries)
		}
	}

	for _, kind := range allParserKinds {
		_, err := parseFixture(ParseOptions{}, kind, []byte("   \n  "))
		lineOriented := kind == ParserLines || kind == ParserText
		if lineOriented && err != nil {
			t.Fatalf("kind %s: blank body is a known empty: %v", kind, err)
		}
		if !lineOriented && err == nil {
			t.Fatalf("kind %s: blank body must not parse as success", kind)
		}
	}
}

func TestParseMaxErrorsCapsDiagnosticsOnly(t *testing.T) {
	var b strings.Builder
	for i := 0; i < 150; i++ {
		b.WriteString("junk row\n")
	}
	b.WriteString(fixtureIP1 + ":8080\n")
	out, err := parseFixture(ParseOptions{MaxErrors: 7}, ParserLines, b.String())
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(out.Rejected) != 7 || out.RejectedCount != 150 {
		t.Fatalf("rejected kept=%d count=%d, want 7/150", len(out.Rejected), out.RejectedCount)
	}
}

// fixtureFor builds a synthetic site response shape for one scenario:
// "normal", "empty" (recognized structural empty), or "malformed" (every row
// bad, so the parse must fail). Protocol-dependent parsers key off site.
func fixtureFor(site Site, scenario string) string {
	switch site.Kind {
	case ParserText:
		switch scenario {
		case "normal":
			return fixtureIP1 + ":8080\n" + fixtureIP2 + ":3128\n"
		case "empty":
			return "\n"
		default:
			return "this is not a proxy row\n"
		}
	case ParserLines:
		switch scenario {
		case "normal":
			return fixtureIP1 + ":8080\n" + fixtureIP2 + ":3128\n" + fixtureIP3 + ":9443\n"
		case "empty":
			return ""
		default:
			return "not a proxy line\n"
		}
	case ParserJSONList:
		switch scenario {
		case "normal":
			return `{"data":{"list":[{"ip":"` + fixtureIP1 + `","port":8080,"protocol":1},` +
				`{"ip":"` + fixtureIP2 + `","port":3128,"protocol":2}],"page_count":1}}`
		case "empty":
			return `{"data":{"list":[],"page_count":0}}`
		default:
			return `{"data":{"list":[{"ip":"300.1.1.1","port":80,"protocol":1},` +
				`{"ip":"` + fixtureIP1 + `","port":8080,"protocol":5}],"page_count":1}}`
		}
	case ParserTableIPPort:
		switch scenario {
		case "normal":
			return "<table><tr><td>" + fixtureIP1 + ":8080</td><td>US</td></tr>" +
				"<tr><td>" + fixtureIP2 + ":3128</td><td>NL</td></tr></table>"
		case "empty":
			return "<table></table>"
		default:
			return "<table><tr><td>" + fixtureIP1 + ":99999</td></tr></table>"
		}
	case ParserTable2Cols:
		switch scenario {
		case "normal":
			return "<table><tr><td>IP</td><td>PORT</td></tr>" +
				"<tr><td>" + fixtureIP1 + "</td><td>8080</td></tr>" +
				"<tr><td>" + fixtureIP2 + "</td><td>3128</td></tr></table>"
		case "empty":
			return "<table><tr><td>IP</td><td>PORT</td></tr></table>"
		default:
			return "<table><tr><td>" + fixtureIP1 + "</td><td>notaport</td></tr></table>"
		}
	case ParserTable3Cols:
		switch scenario {
		case "normal":
			return "<table><tr><td>#</td><td>IP</td><td>PORT</td></tr>" +
				"<tr><td>1</td><td>" + fixtureIP1 + "</td><td>8080</td></tr>" +
				"<tr><td>2</td><td>" + fixtureIP2 + "</td><td>3128</td></tr></table>"
		case "empty":
			return "<table><tr><td>#</td><td>IP</td><td>PORT</td></tr></table>"
		default:
			return "<table><tr><td>1</td><td>" + fixtureIP1 + "</td><td>notaport</td></tr></table>"
		}
	case ParserTableTH:
		switch scenario {
		case "normal":
			return "<table><tr><th>ip</th><th>port</th></tr>" +
				"<tr><th>" + fixtureIP1 + "</th><th>8080</th></tr>" +
				"<tr><th>" + fixtureIP2 + "</th><th>3128</th></tr></table>"
		case "empty":
			return "<table><tr><th>ip</th><th>port</th></tr></table>"
		default:
			return "<table><tr><th>" + fixtureIP1 + "</th><th>notaport</th></tr></table>"
		}
	case ParserRoleCells:
		switch scenario {
		case "normal":
			return `<div role="rowgroup">
<div role="row"><span role="columnheader">IP</span><span role="columnheader">端口</span><span role="columnheader">匿名度</span></div>
<div role="row"><span role="cell" title="` + fixtureIP1 + `">` + fixtureIP1 + `</span><span role="cell">8080</span><span role="cell">高匿名</span></div>
<div role="row"><span role="cell" title="` + fixtureIP2 + `">` + fixtureIP2 + `</span><span role="cell">3128</span><span role="cell">透明</span></div>
</div>`
		case "empty":
			return `<div role="row"></div>`
		default:
			return `<div role="row"><span role="cell">` + fixtureIP1 + `</span>` +
				`<span role="cell">notaport</span></div>`
		}
	case ParserFPSList:
		switch scenario {
		case "normal":
			return "var a=1;\nconst fpsList = [{\"ip\":\"" + fixtureIP1 + "\",\"port\":\"8080\"}," +
				"{\"ip\":\"" + fixtureIP2 + "\",\"port\":3128}];"
		case "empty":
			return `const fpsList = [];`
		default:
			return `const fpsList = [{"ip":"300.1.1.1","port":80}];`
		}
	case ParserFineProxy:
		switch scenario {
		case "normal":
			return `{"data":{"rows":"<tr><td class=\"table-ip\">` + fixtureIP1 + `</td><td>8080</td></tr>` +
				`<tr><td class=\"table-ip\">` + fixtureIP2 + `</td><td>3128</td></tr>"}}`
		case "empty":
			return `{"data":{"rows":""}}`
		default:
			return `{"data":{"rows":"<tr><td class=\"table-ip\">not-an-ip</td><td>8080</td></tr>"}}`
		}
	case ParserSpysText:
		switch scenario {
		case "normal":
			return "Proxy list\nip:port CC-Anon-S Domain code time\n" +
				fixtureIP1 + ":8080 US-N S! - 00:05\n" +
				fixtureIP2 + ":3128 NL-! H - 00:06\n"
		case "empty":
			return "Proxy list\nip:port CC-Anon-S Domain code time\n"
		default:
			return fixtureIP1 + ":notaport US-N S! - 00:05\n"
		}
	case ParserGeonodeJSON:
		match, other := "HTTP", "SOCKS5"
		if site.Protocol == domain.TransportSOCKS5 {
			match, other = "SOCKS5", "HTTP"
		}
		switch scenario {
		case "normal":
			return `{"data":[` +
				`{"ip":"` + fixtureIP1 + `","port":8080,"protocols":["` + match + `"],"anonymityLevel":"elite","lastChecked":1700000000},` +
				`{"ip":"` + fixtureIP2 + `","port":3128,"protocols":["` + other + `"],"anonymityLevel":"transparent","lastChecked":1699990000}],` +
				`"total":2,"page":1,"limit":50}`
		case "empty":
			return `{"data":[],"total":0,"page":1,"limit":50}`
		default:
			return `{"data":[{"ip":"300.1.1.1","port":80,"protocols":["` + match + `"],"lastChecked":1700000000}]}`
		}
	case ParserFreeProxy:
		header := `<thead><tr><th class="th1">IP Address</th><th class="th1">Port</th>` +
			`<th class="th1">Code</th><th class="th1">Country</th><th class="th1">Anonymity</th>` +
			`<th class="th1">Google</th><th class="th1">Https</th><th class="th1">Last Checked</th>`
		socks := site.Protocol == domain.TransportSOCKS5
		if socks {
			header += `<th class="th1">Version</th>`
		}
		header += `</tr></thead>`
		row := func(ip, port, version string) string {
			r := `<tr><td>` + ip + `</td><td>` + port + `</td><td>US</td><td>United States</td>` +
				`<td>elite proxy</td><td>no</td><td>no</td><td>3 mins ago</td>`
			if socks {
				r += `<td>` + version + `</td>`
			}
			return r + `</tr>`
		}
		switch scenario {
		case "normal":
			body := `<table class="table table-striped table-bordered">` + header + `<tbody>`
			if socks {
				body += row(fixtureIP1, "1080", "Socks5")
			} else {
				body += row(fixtureIP1, "8080", "")
			}
			return body + `</tbody></table>`
		case "empty":
			return `<table class="table table-striped table-bordered">` + header + `</table>`
		default:
			if socks {
				return `<table class="table table-striped table-bordered">` + header +
					`<tbody>` + row(fixtureIP1, "1080", "Socks4") + `</tbody></table>`
			}
			return `<table class="table table-striped table-bordered">` + header +
				`<tbody>` + row(fixtureIP1, "notaport", "") + `</tbody></table>`
		}
	case ParserIHuanHTML:
		switch scenario {
		case "normal":
			return `<table class="table"><tbody>` +
				`<tr><td><a data-ip="` + fixtureIP1 + `" data-port="8080">` + fixtureIP1 + `</a></td></tr>` +
				`<tr><td><a data-ip="` + fixtureIP2 + `" data-port="3128">` + fixtureIP2 + `</a></td></tr>` +
				`</tbody></table>`
		case "empty":
			return `<a href="https://ip.ihuan.me/">iHuan</a><table><tr><th>IP</th><th>端口</th></tr></table>`
		default:
			return `<table class="table"><tbody>` +
				`<tr><td><a data-ip="` + fixtureIP1 + `" data-port="notaport">` + fixtureIP1 + `</a></td></tr>` +
				`</tbody></table>`
		}
	case ParserIP89HTML:
		switch scenario {
		case "normal":
			return `<table class="table table-bordered table-striped">` +
				`<thead><tr><th>IP地址</th><th>端口</th></tr></thead><tbody>` +
				`<tr><td>` + fixtureIP1 + `</td><td>8080</td></tr>` +
				`<tr><td>` + fixtureIP2 + `</td><td>3128</td></tr>` +
				`</tbody></table>`
		case "empty":
			return `<table class="table table-bordered table-striped">` +
				`<thead><tr><th>IP地址</th><th>端口</th></tr></thead></table>`
		default:
			return `<table class="table table-bordered table-striped">` +
				`<tbody><tr><td>` + fixtureIP1 + `</td><td>99999</td></tr></tbody></table>`
		}
	case ParserKuaidaili:
		switch scenario {
		case "normal":
			return `<div id="list"><table><thead><tr><th>IP</th><th>PORT</th><th>匿名度</th><th>类型</th>` +
				`<th>位置</th><th>响应速度</th><th>最后验证时间</th></tr></thead><tbody>` +
				`<tr><td data-title="IP">` + fixtureIP1 + `</td><td data-title="PORT">8080</td>` +
				`<td data-title="匿名度">高匿名</td><td data-title="类型">HTTP</td><td data-title="位置">美国</td>` +
				`<td data-title="响应速度">0.5秒</td><td data-title="最后验证时间">24-10-01 12:00:00</td></tr>` +
				`<tr><td data-title="IP">` + fixtureIP2 + `</td><td data-title="PORT">3128</td>` +
				`<td data-title="匿名度">透明</td><td data-title="类型">HTTPS</td><td data-title="位置">荷兰</td>` +
				`<td data-title="响应速度">0.6秒</td><td data-title="最后验证时间">24-10-01 12:00:01</td></tr>` +
				`</tbody></table>`
		case "empty":
			return `<div id="list"><table><thead><tr><th>IP</th><th>PORT</th><th>匿名度</th></tr></thead></table>`
		default:
			return `<table><tbody>` +
				`<tr><td data-title="IP">300.1.1.1</td><td data-title="PORT">8080</td>` +
				`<td data-title="匿名度">高匿名</td></tr></tbody></table>`
		}
	case ParserTableHTML:
		proto := "HTTP"
		if site.Protocol == domain.TransportSOCKS5 {
			proto = "SOCKS5"
		}
		switch scenario {
		case "normal":
			return `<table><thead><tr><th>IP</th><th>Port</th><th>Protocol</th></tr></thead><tbody>` +
				`<tr><td>` + fixtureIP1 + `</td><td>8080</td><td>` + proto + `</td></tr>` +
				`</tbody></table>`
		case "empty":
			return `<table><thead><tr><th>IP</th><th>Port</th><th>Protocol</th></tr></thead></table>`
		default:
			return `<table><tbody>` +
				`<tr><td>` + fixtureIP1 + `</td><td>notaport</td><td>` + proto + `</td></tr>` +
				`</tbody></table>`
		}
	default:
		return "\x00unfixtured kind " + string(site.Kind) + "\x00"
	}
}
