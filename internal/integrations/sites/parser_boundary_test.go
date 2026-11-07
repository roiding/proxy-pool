package sites

import (
	"encoding/json"
	"strings"
	"testing"

	"golang.org/x/text/encoding/simplifiedchinese"
	"proxypool/internal/domain"
)

func TestGeonodeCamelCaseProtocolsSecondsAndCredentials(t *testing.T) {
	body := `{"data":[{"ip":"proxy.example","port":"8080","username":"one","password":"secret","protocols":["http","HTTP","https","socks5","SOCKS5"],"lastChecked":1700000000,"anonymityLevel":"elite","country":"US"}]}`
	out, err := ParseBody(ParseOptions{}, ParserGeonodeJSON, []byte(body))
	if err != nil || out.Fetched != 2 || out.Unique != 2 || out.SourceUpdatedAt == nil || out.SourceUpdatedAt.Unix() != 1700000000 {
		t.Fatalf("camelCase/dedup/time: %+v %v", out, err)
	}
	for _, e := range out.Entries {
		if e.Conn.Username != "one" || e.Conn.Password != "secret" || e.Claims["anonymity"] != "elite" || e.ProviderExpiresAt != nil {
			t.Fatalf("credential/claim lost: %+v", e)
		}
	}
	if out.Entries[0].Conn.Transport != domain.TransportHTTP || out.Entries[0].VendorProtocol != "https" {
		t.Fatal("HTTPS became proxy-side TLS")
	}
	out, err = ParseBody(ParseOptions{Protocol: domain.TransportHTTP}, ParserGeonodeJSON, []byte(`{"data":[{"ip":"93.184.216.34","port":8080,"protocols":["socks5"]}]}`))
	if err != nil || out.Unique != 0 || out.RejectedCount != 1 {
		t.Fatalf("nonmatching rolling page failed instead of filtering: %+v %v", out, err)
	}
	for _, value := range []string{"1700000000000", "-1", "0", "\"not-a-date\""} {
		out, err = ParseBody(ParseOptions{}, ParserGeonodeJSON, []byte(`{"data":[{"ip":"93.184.216.34","port":80,"protocols":["http"],"lastChecked":`+value+`}]}`))
		if err != nil || out.SourceUpdatedAt != nil {
			t.Fatalf("guessed milliseconds/invalid timestamp %s: %+v %v", value, out, err)
		}
	}
}

func TestSpysVendorMarkersNeverMintPassOrYearZero(t *testing.T) {
	body := "Proxy list 23:09:00 UTC\nIP address:port Country-Anonymity(-SSL) Google(+)\n93.184.216.34:8080 US-H-S +\n93.184.216.35:3128 NL-A -\n"
	out, err := ParseBody(ParseOptions{}, ParserSpysText, []byte(body))
	if err != nil || out.Unique != 2 || out.SourceUpdatedAt != nil {
		t.Fatalf("Spys metadata: %+v %v", out, err)
	}
	if out.Entries[0].Claims["anonymity"] != "elite" || out.Entries[0].Claims["vendor_google_access"] != "true" || out.Entries[0].Claims["google_pass"] != "" || out.Entries[0].VendorProtocol != "https" {
		t.Fatalf("Spys claims: %+v", out.Entries[0])
	}
	dated := strings.Replace(body, "23:09:00 UTC", "2026-11-04 23:09:00 UTC", 1)
	out, err = ParseBody(ParseOptions{}, ParserSpysText, []byte(dated))
	if err != nil || out.SourceUpdatedAt == nil || out.SourceUpdatedAt.Year() != 2026 {
		t.Fatalf("complete dated header missing: %+v %v", out, err)
	}
	if _, err = ParseBody(ParseOptions{Protocol: domain.TransportSOCKS5}, ParserSpysText, []byte(body)); err == nil {
		t.Fatal("Spys guessed SOCKS version")
	}
}

func TestParserRequiresRealStructureAndRejectsErrorEnvelopes(t *testing.T) {
	for _, kind := range allParserKinds {
		t.Run(string(kind), func(t *testing.T) {
			for _, body := range []string{`<html><title>Service Unavailable</title><table><tr><td>93.184.216.34</td><td>8080</td></tr></table></html>`, `<!doctype html><html><body>Access denied vendor-key 93.184.216.34:8080</body></html>`, `{"status":401,"data":[{"ip":"93.184.216.34","port":80}],"message":"vendor-secret"}`} {
				if _, err := ParseBody(ParseOptions{}, kind, []byte(body)); err == nil || strings.Contains(err.Error(), "vendor-") {
					t.Fatalf("error response parsed/leaked: %v", err)
				}
			}
		})
	}
	for kind, bodies := range map[ParserKind][]string{
		ParserFineProxy:   {`{}`, `{"data":{}}`, `{"data":{"rows":null}}`},
		ParserJSONList:    {`null`, `{"data":{"list":null}}`, `{"list":{}}`, `{}`},
		ParserGeonodeJSON: {`null`, `{"data":null}`, `{}`},
		ParserKuaidaili:   {`<table><tr><th>IP</th><th>PORT</th></tr><tr><td>93.184.216.34</td><td>8080</td></tr></table>`},
		ParserIHuanHTML:   {`<html>ip.ihuan.me <table><tr><td>93.184.216.34</td><td>8080</td></tr></table></html>`},
		ParserFreeProxy:   {`<table><tr><th>IP</th><th>Port</th></tr><tr><td>93.184.216.34</td><td>8080</td></tr></table>`},
	} {
		for _, body := range bodies {
			if _, err := ParseBody(ParseOptions{}, kind, []byte(body)); err == nil {
				t.Fatalf("%s accepted absent/ambiguous structure", kind)
			}
		}
	}
	out, err := ParseBody(ParseOptions{}, ParserJSONList, []byte(`{"data":{"data":{"list":[{"ip":"93.184.216.34","port":80,"protocol":1,"username":"u","password":"p"}],"page_count":2}}}`))
	if err != nil || out.Unique != 1 || out.Entries[0].Conn.Password != "p" || autoPageCount([]byte(`{"data":{"data":{"page_count":2}}}`)) != 2 {
		t.Fatalf("nested list/credentials: %+v %v", out, err)
	}
}

func TestHTMLKnownColumnsDemoAndProtocolAmbiguity(t *testing.T) {
	table := `<div id="list"><table><tr><th>IP</th><th>PORT</th><th>类型</th></tr><tr data-demo="true"><td>93.184.216.34</td><td>80</td><td>HTTP</td></tr><tr><td>93.184.216.35</td><td>8080</td><td>HTTPS</td></tr></table></div>`
	out, err := ParseBody(ParseOptions{}, ParserKuaidaili, []byte(table))
	if err != nil || out.Unique != 1 || out.Entries[0].Conn.Host != "93.184.216.35" || out.Entries[0].VendorProtocol != "https" {
		t.Fatalf("demo/claim: %+v %v", out, err)
	}
	table = `<table><tr><th>排名</th><th>IP地址</th><th>端口</th><th>国家</th></tr><tr><td>1</td><td>93.184.216.34</td><td>8080</td><td>US</td></tr></table>`
	out, err = ParseBody(ParseOptions{}, ParserIP89HTML, []byte(table))
	if err != nil || out.Unique != 1 || out.Entries[0].Conn.Port != 8080 {
		t.Fatalf("89ip extra columns: %+v %v", out, err)
	}
	table = `<table><tr><th>IP</th><th>Port</th></tr><tr><td>93.184.216.34</td><td>1080</td></tr></table>`
	if _, err = ParseBody(ParseOptions{Protocol: domain.TransportSOCKS5}, ParserTableHTML, []byte(table)); err == nil {
		t.Fatal("generic table guessed SOCKS5")
	}
	if _, err = ParseBody(ParseOptions{Protocol: domain.TransportSOCKS5}, ParserFreeProxy, []byte(`<html>Free proxy list<textarea></textarea></html>`)); err == nil {
		t.Fatal("empty textarea guessed SOCKS5")
	}
	table = `<table id="proxylisttable"><tr><th>IP</th><th>Port</th><th>Version</th></tr><tr><td>93.184.216.34</td><td>1080</td><td>SOCKS5</td></tr></table>`
	out, err = ParseBody(ParseOptions{Protocol: domain.TransportHTTP}, ParserFreeProxy, []byte(table))
	if err != nil || out.Unique != 0 || out.RejectedCount != 1 {
		t.Fatalf("freeproxy leaked SOCKS into HTTP channel: %+v %v", out, err)
	}
}

func TestStrict89BodyLinesEncodingAndDiagnostics(t *testing.T) {
	out, err := ParseBody(ParseOptions{}, ParserText, []byte(`<html><body>93.184.216.34:8080<br/>93.184.216.35:3128</body></html>`))
	if err != nil || out.Unique != 2 {
		t.Fatalf("pure 89 body/br: %+v %v", out, err)
	}
	for _, body := range []string{`<html><body>Error: 93.184.216.34:8080</body></html>`, `<html><body><a>93.184.216.34:8080</a></body></html>`} {
		if _, err = ParseBody(ParseOptions{}, ParserText, []byte(body)); err == nil {
			t.Fatal("regex extracted an endpoint from HTML")
		}
	}
	out, err = ParseBody(ParseOptions{}, ParserLines, []byte("\xef\xbb\xbfhttp://a:p%40ss@Proxy.Example.:8080\nsocks5://u:p@[2606:4700::1111]:1080\nhttp://secret-user:secret-pass@8.8.8.8:80/?key=secret-query\nhttp://u:%00@8.8.8.8:80\nhttp://[fe80::1%25en0]:80\n"))
	if err != nil || out.Unique != 2 || out.RejectedCount != 3 || out.Entries[0].Conn.Password != "p@ss" || out.Entries[0].Conn.Host != "proxy.example" {
		t.Fatalf("strict lines: %+v %v", out, err)
	}
	data, _ := json.Marshal(out.Rejected)
	if strings.Contains(string(data), "secret-") || strings.Contains(string(data), "%00") {
		t.Fatalf("diagnostics leaked: %s", data)
	}
	gbkBody, _ := simplifiedchinese.GBK.NewEncoder().Bytes([]byte(`<html><meta charset="GBK"><table><tr><td>IP地址</td><td>端口</td></tr><tr><td>93.184.216.34</td><td>8080</td></tr></table></html>`))
	out, err = ParseBody(ParseOptions{}, ParserTable2Cols, gbkBody)
	if err != nil || out.Unique != 1 {
		t.Fatalf("declared GBK: %+v %v", out, err)
	}
	if _, err = ParseBody(ParseOptions{}, ParserLines, []byte{0xff, 0xfe}); err == nil {
		t.Fatal("undeclared invalid encoding parsed")
	}
}

func TestParserRecordAndRowCaps(t *testing.T) {
	out, err := ParseBody(ParseOptions{MaxRecords: 2}, ParserLines, []byte("93.184.216.34:80\n93.184.216.34:80\n93.184.216.35:80\n"))
	if err != nil || out.Fetched != 2 || out.Unique != 1 || !out.Truncated || out.Complete {
		t.Fatalf("records examined/dedup cap: %+v %v", out, err)
	}
	out, err = ParseBody(ParseOptions{MaxRowBytes: 30}, ParserLines, []byte(strings.Repeat("secret", 100)+"\n93.184.216.34:80\n"))
	if err != nil || out.Unique != 1 || out.RejectedCount != 1 || out.Rejected[0].Input != "" {
		t.Fatalf("row cap: %+v %v", out, err)
	}
}

func TestCurrentIHuanEncodedLinkAnd89AdvertisedListShapes(t *testing.T) {
	ihuan := `<html><title>小幻HTTP代理</title><table class="table"><tr><th>IP地址</th><th>端口</th></tr><tr><td><a href="/check.html?proxy=OTMuMTg0LjIxNi4zNDo4MDgw" target="_blank"><img src="/flag/US.svg">93.184.216.34</a></td><td>8080</td></tr></table></html>`
	out, err := ParseBody(ParseOptions{}, ParserIHuanHTML, []byte(ihuan))
	if err != nil || out.Unique != 1 {
		t.Fatalf("encoded check-link: %+v %v", out, err)
	}
	bad := strings.Replace(ihuan, "OTMuMTg0LjIxNi4zNDo4MDgw", "OTMuMTg0LjIxNi4zNTo4MDgw", 1)
	if _, err = ParseBody(ParseOptions{}, ParserIHuanHTML, []byte(bad)); err == nil {
		t.Fatal("mismatched visible and encoded IP accepted")
	}
	banner := `<a href="https://www.qiyunip.com/" target="_blank" data-type="img"><img src="img/hfad.png"></a><br><script src="js/jquery.min.js"></script><div id="adarea"><div id="adclose"></div></div><script>$('#adarea').slideDown();$('#adclose').click();</script>`
	rows := "93.184.216.34:8080<br>93.184.216.35:3128<br>更好用的代理ip请访问：www.qiyunip.com"
	out, err = ParseBody(ParseOptions{}, ParserText, []byte(banner+rows))
	if err != nil || out.Unique != 2 {
		t.Fatalf("89 known advertised shape: %+v %v", out, err)
	}
	for _, body := range []string{banner + "Error: " + rows, banner + `<a>93.184.216.34:8080</a>`, strings.Replace(banner, "id=\"adarea\"", "id=\"error\"", 1) + rows} {
		if _, err = ParseBody(ParseOptions{}, ParserText, []byte(body)); err == nil {
			t.Fatal("89 shape accepted arbitrary HTML/error")
		}
	}
}

func TestSpysCurrentPreambleCombinedMarkersAndDatedOffset(t *testing.T) {
	body := "Proxy list (#400) updated at Thu, 05 Nov 26 03:58:02 +0300\nSocks proxy=https://spys.me/socks.txt\nSupport by donations:\nBTC synthetic-fixture-address\nIP address:Port CountryCode-Anonymity(Noa/Anm/Hia)-SSL_support(S)-Google_passed(+)\n\n93.184.216.34:8080 BR-N! -\n93.184.216.35:3128 CO-N-S! +\nFree open HTTP/HTTPS(SSL) proxy list  - TXT format - updated hourly.\n"
	out, err := ParseBody(ParseOptions{}, ParserSpysText, []byte(body))
	if err != nil || out.Unique != 2 || out.RejectedCount != 0 || out.SourceUpdatedAt == nil || out.SourceUpdatedAt.Year() != 2026 || out.SourceUpdatedAt.Hour() != 0 {
		t.Fatalf("Spys real shape: %+v %v", out, err)
	}
	if out.Entries[1].VendorProtocol != "https" || out.Entries[1].Claims["vendor_google_access"] != "true" || out.Entries[1].Claims["anonymity"] != "transparent" {
		t.Fatalf("combined markers changed anonymity: %+v", out.Entries[1])
	}
}

func Test89CurrentPortNumberHeaderAndMetadataColumns(t *testing.T) {
	out, err := ParseBody(ParseOptions{}, ParserIP89HTML, []byte(`<table class="layui-table"><tr><th>IP</th><th>端口号</th><th>代理位置</th><th>运营商</th><th>录取时间</th></tr><tr><td>93.184.216.34</td><td>80</td><td>测试地区</td><td>测试运营商</td><td>2026/11/05 10:15:03</td></tr></table>`))
	if err != nil || out.Unique != 1 || out.Entries[0].Conn.Port != 80 {
		t.Fatalf("89 current header: %+v %v", out, err)
	}
}
