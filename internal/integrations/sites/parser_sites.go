package sites

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/html"
	"proxypool/internal/domain"
)

var (
	spysHeaderTimePattern = regexp.MustCompile(`(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})\s*(UTC|GMT)`)
	spysMetaPattern       = regexp.MustCompile(`^([A-Za-z]{2})-([NAH!])(?:-(S))?([!+-])?$`)
	pureBodyListPattern   = regexp.MustCompile(`(?is)^<html>\s*<body>\s*(.*?)\s*</body>\s*</html>$`)
	pureBodyRowPattern    = regexp.MustCompile(`^\d{1,3}(?:\.\d{1,3}){3}:\d{1,5}$`)
	brTagPattern          = regexp.MustCompile(`(?i)<br\s*/?>`)
)

// parseSpysText reads the fixed spys.me "Proxy list" text shape: a header
// line, then "ip:port CC-A-S [+-]" rows. Only complete dated UTC/GMT header timestamps
// are trusted as source-claim timestamps; socks files stay unrunnable since
// they never declare socks4 vs socks5.
func parseSpysText(opts ParseOptions, body []byte) (parsedPage, error) {
	if opts.Protocol == domain.TransportSOCKS5 {
		return parsedPage{}, &Error{Kind: KindFormat, Detail: "spys text does not declare SOCKS5 version"}
	}
	p := newPageBuilder(opts)
	transport := opts.Protocol
	if transport == "" {
		transport = domain.TransportHTTP
	}
	records := 0
	recognized := false
	inData := false
	for index, line := range splitBoundedLines(body, opts.maxRowBytes()) {
		text := strings.TrimSpace(line.text)
		if text == "" {
			continue
		}
		if !recognized {
			if !strings.Contains(strings.ToLower(text), "proxy list") && !(strings.Contains(strings.ToLower(text), ":port") && strings.Contains(strings.ToLower(text), "anon")) {
				p.reject(index+1, "", "spys header line missing")
				continue
			}
			recognized = true
			if _, stampText, ok := strings.Cut(text, "updated at"); ok {
				stampText = strings.TrimSpace(stampText)
				for _, layout := range []string{time.RFC1123Z, "Mon, 02 Jan 06 15:04:05 -0700"} {
					if stamp, err := time.Parse(layout, stampText); err == nil {
						p.updateSourceTime(&stamp)
						break
					}
				}
			}
			if m := spysHeaderTimePattern.FindStringSubmatch(text); m != nil {
				if stamp, err := time.Parse("2006-01-02 15:04:05", strings.ReplaceAll(m[1], "T", " ")); err == nil {
					p.updateSourceTime(&stamp)
				}
			}
			continue
		}
		if line.tooLong {
			p.reject(index+1, "", "line exceeds row byte limit")
			continue
		}
		if strings.Contains(strings.ToLower(text), ":port") && (strings.Contains(strings.ToLower(text), "country") || strings.Contains(strings.ToLower(text), "anon")) {
			inData = true
			continue
		}
		if !inData {
			if strings.HasPrefix(text, "Socks proxy=") || text == "Support by donations:" || strings.HasPrefix(text, "BTC ") {
				continue
			}
			// Historical fixture headers can be the only header. First endpoint starts rows.
			fields := strings.Fields(text)
			if len(fields) > 0 {
				_, _, inData = splitIPv4Port(fields[0])
			}
			if !inData {
				p.reject(index+1, "", "unexpected spys preamble")
				continue
			}
		}
		if text == "Free open HTTP/HTTPS(SSL) proxy list  - TXT format - updated hourly." {
			continue
		}
		fields := strings.Fields(text)
		if len(fields) < 2 {
			p.reject(index+1, "", "spys row lacks the country/anonymity column")
			continue
		}
		host, portText, ok := splitIPv4Port(fields[0])
		port, perr := strconv.Atoi(portText)
		meta := spysMetaPattern.FindStringSubmatch(fields[1])
		if !ok || !validIPv4(host) || perr != nil || port < 1 || port > 65535 {
			p.reject(index+1, "", "invalid ip or port")
			continue
		}
		if meta == nil {
			p.reject(index+1, "", "unrecognized country/anonymity marker")
			continue
		}
		records++
		if records > opts.maxRecords() {
			p.out.truncated = true
			break
		}
		claims := map[string]string{
			"protocol":      string(transport),
			"vendor_marker": meta[4],
			"country":       strings.ToUpper(meta[1]),
			"anonymity":     spysAnonymity(meta[2]),
		}
		vendorHTTPS := meta[3] == "S"
		if vendorHTTPS {
			claims["http_connect"] = "true"
		}
		if meta[4] == "+" || len(fields) > 2 && fields[len(fields)-1] == "+" {
			claims["vendor_google_access"] = "true"
		}
		p.accept(index+1, "", Entry{
			Conn:           domain.Connection{Transport: transport, Host: host, Port: port},
			VendorProtocol: vendorProtocol(vendorHTTPS),
			Claims:         claims,
		})
	}
	p.out.recognized = recognized
	return p.out, nil
}

func splitIPv4Port(token string) (string, string, bool) {
	index := strings.LastIndexByte(token, ':')
	if index <= 0 || index == len(token)-1 {
		return "", "", false
	}
	return token[:index], token[index+1:], true
}

func spysAnonymity(code string) string {
	switch code {
	case "N":
		return "transparent"
	case "A":
		return "anonymous"
	case "!", "H":
		return "elite"
	}
	return "unknown"
}

type geonodeEnvelope struct {
	Data json.RawMessage `json:"data"`
}

type geonodeItem struct {
	IP          json.RawMessage `json:"ip"`
	Port        json.RawMessage `json:"port"`
	Protocols   []string        `json:"protocols"`
	Anonymity   string          `json:"anonymityLevel"`
	Country     string          `json:"country"`
	LastChecked json.RawMessage `json:"lastChecked"`
	Username    string          `json:"username"`
	Password    string          `json:"password"`
}

// parseGeonodeJSON reads the proxylist.geonode.com shape: data[] with
// per-row protocols[]. HTTP/HTTPS collapse onto one HTTP connection with a
// vendor HTTPS claim; socks5 stays independent. lastChecked (unix seconds)
// is a claim only, surfaced as SourceUpdatedAt.
func parseGeonodeJSON(opts ParseOptions, body []byte) (parsedPage, error) {
	var env geonodeEnvelope
	if err := json.Unmarshal(body, &env); err != nil || len(env.Data) == 0 || string(env.Data) == "null" {
		return parsedPage{}, &Error{Kind: KindFormat, Detail: "geonode response lacks a non-null data[] list"}
	}
	var items []geonodeItem
	if err := json.Unmarshal(env.Data, &items); err != nil {
		return parsedPage{}, &Error{Kind: KindFormat, Detail: "decode geonode data[] list"}
	}
	p := newPageBuilder(opts)
	p.out.recognized = true
	for i, item := range items {
		if i >= opts.maxRecords() {
			p.out.truncated = true
			break
		}
		ip := jsonText(item.IP)
		port, ok := jsonInt(item.Port)
		if ip == "" || !ok || port < 1 || port > 65535 {
			p.reject(i+1, "", "invalid ip or port")
			continue
		}
		transports, hasHTTPS, unknown := geonodeTransports(item.Protocols)
		if unknown {
			p.reject(i+1, "", "unknown vendor protocol")
		}
		if len(transports) == 0 {
			p.reject(i+1, "", "missing vendor protocol list")
			continue
		}
		if opts.Protocol != "" && !containsTransport(transports, opts.Protocol) {
			p.rejectProtocol(i + 1)
			continue
		}
		claims := map[string]string{}
		if anonymity := vendorAnonymity(item.Anonymity); anonymity != "" {
			claims["anonymity"] = anonymity
		}
		if country := vendorCountry(item.Country); country != "" {
			claims["country"] = country
		}
		if seconds, ok := jsonInt(item.LastChecked); ok && seconds >= 946684800 && int64(seconds) <= 4102444800 {
			checked := time.Unix(int64(seconds), 0).UTC()
			claims["last_checked"] = checked.Format(time.RFC3339)
			p.updateSourceTime(&checked)
		}
		for _, transport := range transports {
			if opts.Protocol != "" && transport != opts.Protocol {
				continue
			}
			entry := Entry{
				Conn:   domain.Connection{Transport: transport, Host: ip, Port: port, Username: item.Username, Password: item.Password},
				Claims: copyClaims(claims),
			}
			if hasHTTPS && transport == domain.TransportHTTP {
				entry.VendorProtocol = "https"
				entry.Claims["http_connect"] = "true"
			}
			p.accept(i+1, "", entry)
		}
	}
	return p.out, nil
}

func geonodeTransports(protocols []string) (transports []domain.Transport, hasHTTPS, unknown bool) {
	for _, proto := range protocols {
		switch strings.ToLower(strings.TrimSpace(proto)) {
		case "http":
			if !containsTransport(transports, domain.TransportHTTP) {
				transports = append(transports, domain.TransportHTTP)
			}
		case "https":
			hasHTTPS = true
		case "socks5":
			if !containsTransport(transports, domain.TransportSOCKS5) {
				transports = append(transports, domain.TransportSOCKS5)
			}
		default:
			unknown = true
		}
	}
	if hasHTTPS && !containsTransport(transports, domain.TransportHTTP) {
		if !containsTransport(transports, domain.TransportHTTP) {
			transports = append(transports, domain.TransportHTTP)
		}
	}
	return transports, hasHTTPS, unknown
}

func containsTransport(transports []domain.Transport, want domain.Transport) bool {
	for _, transport := range transports {
		if transport == want {
			return true
		}
	}
	return false
}

// parseFreeProxy reads the free-proxy-list.net family: the proxylisttable
// table (header-aware, optional https/version columns) or a signed textarea
// of proxy rows. SOCKS5 is accepted only from an explicit per-row
// declaration; bare textareas never guess a transport.
func parseFreeProxy(opts ParseOptions, body []byte) (parsedPage, error) {
	root, err := html.Parse(bytes.NewReader(body))
	if err != nil {
		return parsedPage{}, &Error{Kind: KindFormat, Detail: "invalid freeproxy HTML"}
	}
	p := newPageBuilder(opts)
	for _, table := range htmlTables(root) {
		header, rows, rowClasses := tableRowsOf(table)
		ipIndex, portIndex := headerIPPortIndexes(header)
		if ipIndex < 0 || portIndex < 0 || (attr(table, "id") != "proxylisttable" && (headerIndex(header, "google") < 0 || headerIndexAny(header, "last checked", "last checked time") < 0)) {
			continue
		}
		p.out.recognized = true
		parseFreeProxyTableRows(p, opts, header, rows, rowClasses, ipIndex, portIndex)
		return p.out, nil
	}
	return parseFreeProxyTextarea(p, root, body)
}

func parseFreeProxyTableRows(p *pageBuilder, opts ParseOptions, header []string, rows [][]string, rowClasses []string, ipIndex, portIndex int) {
	anonymityIndex := headerIndexAny(header, "anonymity", "anonymity level", "匿名")
	countryIndex := headerIndexAny(header, "country", "国家")
	httpsIndex := headerIndex(header, "https")
	versionIndex := headerIndexAny(header, "version", "socks version", "protocol", "type")
	for i, cells := range rows {
		if i >= opts.maxRecords() {
			p.out.truncated = true
			break
		}
		line := i + 1
		if isTableHeader(cells) || isDemoRow(classAt(rowClasses, i), cells) {
			continue
		}
		host := cellAt(cells, ipIndex)
		port, perr := strconv.Atoi(cellAt(cells, portIndex))
		if !validIPv4(host) || perr != nil || port < 1 || port > 65535 {
			p.reject(line, "", "invalid ip or port")
			continue
		}
		transport, vendorHTTPS, terr := freeproxyRowTransport(opts, cellAt(cells, versionIndex), cellAt(cells, httpsIndex))
		if terr != nil {
			p.reject(line, "", terr.Error())
			continue
		}
		claims := map[string]string{"protocol": string(transport)}
		if anonymity := vendorAnonymity(cellAt(cells, anonymityIndex)); anonymity != "" {
			claims["anonymity"] = anonymity
		}
		if country := vendorCountry(cellAt(cells, countryIndex)); country != "" {
			claims["country"] = country
		}
		if vendorHTTPS {
			claims["http_connect"] = "true"
		}
		p.accept(line, "", Entry{
			Conn:           domain.Connection{Transport: transport, Host: host, Port: port},
			VendorProtocol: vendorProtocol(vendorHTTPS),
			Claims:         claims,
		})
	}
}

func freeproxyRowTransport(opts ParseOptions, versionCell, httpsCell string) (domain.Transport, bool, error) {
	if opts.Protocol == domain.TransportSOCKS5 && strings.TrimSpace(versionCell) == "" {
		return "", false, &Error{Kind: KindFormat, Detail: "no explicit socks5 declaration in row"}
	}
	transport := opts.Protocol
	if transport == "" {
		transport = domain.TransportHTTP
	}
	if versionCell != "" {
		value := strings.ToLower(strings.TrimSpace(versionCell))
		switch {
		case value == "socks4":
			return "", false, &Error{Kind: KindFormat, Detail: "socks4 is not supported"}
		case value == "socks5":
			transport = domain.TransportSOCKS5
		case value == "https":
			transport = domain.TransportHTTP
		case value == "http":
			transport = domain.TransportHTTP
		default:
			return "", false, &Error{Kind: KindFormat, Detail: "unknown protocol declaration"}
		}
	}
	httpsCell = strings.TrimSpace(httpsCell)
	vendorHTTPS := transport == domain.TransportHTTP && (strings.EqualFold(httpsCell, "yes") || strings.EqualFold(httpsCell, "true") || strings.EqualFold(versionCell, "https"))
	return transport, vendorHTTPS, nil
}

func parseFreeProxyTextarea(p *pageBuilder, root *html.Node, body []byte) (parsedPage, error) {
	lower := strings.ToLower(string(body))
	signed := strings.Contains(lower, "proxylisttable") ||
		strings.Contains(lower, "free proxy list") ||
		strings.Contains(lower, "free-proxy-list")
	textarea := htmlTextArea(root)
	if textarea == nil || !signed {
		return p.out, nil
	}
	if p.opts.Protocol == domain.TransportSOCKS5 {
		return parsedPage{}, &Error{Kind: KindFormat, Detail: "textarea has no explicit socks5 protocol structure"}
	}
	p.out.recognized = true
	content := textContent(textarea)
	if strings.TrimSpace(content) == "" {
		return p.out, nil
	}
	if p.opts.Protocol == domain.TransportSOCKS5 {
		p.reject(1, "", "textarea rows carry no explicit socks5 declaration")
		return p.out, nil
	}
	lineOpts := p.opts
	lineOpts.DefaultTransport = domain.TransportHTTP
	records := 0
	for index, line := range splitBoundedLines([]byte(content), p.opts.maxRowBytes()) {
		text := strings.TrimSpace(line.text)
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		records++
		if records > p.opts.maxRecords() {
			p.out.truncated = true
			break
		}
		if line.tooLong {
			p.reject(index+1, "", "line exceeds row byte limit")
			continue
		}
		conn, err := parseProxyLine(lineOpts, text)
		if err != nil {
			p.reject(index+1, "", err.Error())
			continue
		}
		if lineOpts.Protocol != "" && conn.Transport != lineOpts.Protocol {
			p.rejectProtocol(index + 1)
			continue
		}
		p.accept(index+1, "", Entry{
			Conn:   conn,
			Claims: map[string]string{"protocol": string(conn.Transport)},
		})
	}
	return p.out, nil
}

// parseIHuanHTML reads ip.ihuan.me rows: an IP link cell followed by the
// adjacent port cell. Recognition requires the site signature or a real
// IPv4 link, so random two-column tables do not count as ihuan pages.
func parseIHuanHTML(opts ParseOptions, body []byte) (parsedPage, error) {
	root, err := html.Parse(bytes.NewReader(body))
	if err != nil {
		return parsedPage{}, &Error{Kind: KindFormat, Detail: "invalid ihuan HTML"}
	}
	p := newPageBuilder(opts)
	for _, table := range htmlTables(root) {
		header, _, _ := tableRowsOf(table)
		ipIndex, portIndex := headerIPPortIndexes(header)
		signed := strings.Contains(strings.ToLower(string(body)), "ip.ihuan.me") && ipIndex >= 0 && portIndex >= 0
		rowIndex := 0
		var walk func(*html.Node)
		walk = func(n *html.Node) {
			if p.out.truncated {
				return
			}
			if n.Type == html.ElementNode && n.Data == "tr" {
				rowIndex++
				if rowIndex > opts.maxRecords()+1 {
					p.out.truncated = true
					return
				}
				var cells []*html.Node
				for c := n.FirstChild; c != nil; c = c.NextSibling {
					if c.Type == html.ElementNode && c.Data == "td" {
						cells = append(cells, c)
					}
				}
				for j, c := range cells {
					var link *html.Node
					var find func(*html.Node)
					find = func(x *html.Node) {
						if x.Type == html.ElementNode && x.Data == "a" && validIPv4(strings.TrimSpace(textContent(x))) && (strings.Contains(attr(x, "href"), strings.TrimSpace(textContent(x))) || attr(x, "data-ip") == strings.TrimSpace(textContent(x)) || ihuanCheckLinkMatches(x)) {
							link = x
							return
						}
						for k := x.FirstChild; k != nil; k = k.NextSibling {
							find(k)
						}
					}
					find(c)
					if link == nil {
						continue
					}
					p.out.recognized = true
					host := strings.TrimSpace(textContent(link))
					portText := attr(link, "data-port")
					if j+1 < len(cells) {
						portText = strings.TrimSpace(textContent(cells[j+1]))
					}
					port, err := strconv.Atoi(portText)
					if err != nil || port < 1 || port > 65535 {
						p.reject(rowIndex, "", "invalid ihuan port adjacent to IP link")
						break
					}
					p.accept(rowIndex, "", Entry{Conn: domain.Connection{Transport: domain.TransportHTTP, Host: host, Port: port}, Claims: map[string]string{"protocol": "http"}})
					break
				}
				return
			}
			for c := n.FirstChild; c != nil; c = c.NextSibling {
				walk(c)
			}
		}
		walk(table)
		if signed {
			p.out.recognized = true
		}
	}
	return p.out, nil
}

// IP89 tables carry named IP and port columns; extra metadata columns are allowed.
func parseIP89HTML(opts ParseOptions, body []byte) (parsedPage, error) {
	root, err := html.Parse(bytes.NewReader(body))
	if err != nil {
		return parsedPage{}, &Error{Kind: KindFormat, Detail: "invalid 89ip HTML"}
	}
	p := newPageBuilder(opts)
	for _, table := range htmlTables(root) {
		header, rows, classes := tableRowsOf(table)
		ipIndex, portIndex := headerIPPortIndexes(header)
		if ipIndex < 0 || portIndex < 0 {
			continue
		}
		p.out.recognized = true
		for i, cells := range rows {
			if i >= opts.maxRecords() {
				p.out.truncated = true
				break
			}
			if isDemoRow(classAt(classes, i), cells) {
				continue
			}
			host := cellAt(cells, ipIndex)
			port, err := strconv.Atoi(cellAt(cells, portIndex))
			if !validIPv4(host) || err != nil || port < 1 || port > 65535 {
				p.reject(i+1, "", "invalid 89ip IP or port")
				continue
			}
			p.accept(i+1, "", Entry{Conn: domain.Connection{Transport: domain.TransportHTTP, Host: host, Port: port}, Claims: map[string]string{"protocol": "http"}})
		}
		return p.out, nil
	}
	return p.out, nil
}

// parseKuaidaili reads the kuaidaili free table: an IP/PORT header row is
// required, demo/data-demo/演示 rows are skipped as page decoration, and no
// JS value (fpsList) is treated as a live protocol.
func parseKuaidaili(opts ParseOptions, body []byte) (parsedPage, error) {
	root, err := html.Parse(bytes.NewReader(body))
	if err != nil {
		return parsedPage{}, &Error{Kind: KindFormat, Detail: "invalid kuaidaili HTML"}
	}
	p := newPageBuilder(opts)
	for _, table := range htmlTables(root) {
		header, rows, rowClasses := tableRowsOf(table)
		ipIndex, portIndex := headerIPPortIndexes(header)
		if ipIndex < 0 || portIndex < 0 || !hasAncestorID(table, "list", "freelist") {
			continue
		}
		p.out.recognized = true
		anonymityIndex := headerIndexAny(header, "anonymity", "anonymity level", "匿名", "匿名度")
		transport := domain.TransportHTTP
		for i, cells := range rows {
			if i >= opts.maxRecords() {
				p.out.truncated = true
				break
			}
			line := i + 1
			if isTableHeader(cells) || isDemoRow(classAt(rowClasses, i), cells) {
				continue
			}
			host := cellAt(cells, ipIndex)
			port, perr := strconv.Atoi(cellAt(cells, portIndex))
			if !validIPv4(host) || perr != nil || port < 1 || port > 65535 {
				p.reject(line, "", "invalid kuaidaili row")
				continue
			}
			claims := map[string]string{"protocol": string(transport)}
			if anonymity := vendorAnonymity(cellAt(cells, anonymityIndex)); anonymity != "" {
				claims["anonymity"] = anonymity
			}
			protoIndex := headerIndexAny(header, "类型", "protocol", "type")
			vendorHTTPS := strings.EqualFold(cellAt(cells, protoIndex), "https")
			if protoIndex >= 0 && !strings.EqualFold(cellAt(cells, protoIndex), "http") && !vendorHTTPS {
				p.reject(line, "", "unsupported kuaidaili protocol")
				continue
			}
			if vendorHTTPS {
				claims["http_connect"] = "true"
			}
			p.accept(line, "", Entry{
				VendorProtocol: vendorProtocol(vendorHTTPS),
				Conn:           domain.Connection{Transport: transport, Host: host, Port: port},
				Claims:         claims,
			})
		}
		return p.out, nil
	}
	return p.out, nil
}

// parseTableHTML reads any header-aware table with an optional protocol
// column (protocol/type/scheme/version or an https yes/no column). socks4
// rows are always rejected and the channel protocol filters rows.
func parseTableHTML(opts ParseOptions, body []byte) (parsedPage, error) {
	root, err := html.Parse(bytes.NewReader(body))
	if err != nil {
		return parsedPage{}, &Error{Kind: KindFormat, Detail: "invalid table HTML"}
	}
	p := newPageBuilder(opts)
	for _, table := range htmlTables(root) {
		header, rows, rowClasses := tableRowsOf(table)
		ipIndex, portIndex := headerIPPortIndexes(header)
		if ipIndex < 0 || portIndex < 0 {
			continue
		}
		p.out.recognized = true
		protoIndex := headerIndexAny(header, "protocol", "type", "scheme", "version")
		httpsIndex := headerIndex(header, "https")
		anonymityIndex := headerIndexAny(header, "anonymity", "anonymity level", "匿名")
		for i, cells := range rows {
			if i >= opts.maxRecords() {
				p.out.truncated = true
				break
			}
			line := i + 1
			if isTableHeader(cells) || isDemoRow(classAt(rowClasses, i), cells) {
				continue
			}
			host := cellAt(cells, ipIndex)
			port, perr := strconv.Atoi(cellAt(cells, portIndex))
			if host == "" || perr != nil || port < 1 || port > 65535 {
				p.reject(line, "", "invalid ip or port")
				continue
			}
			transport, vendorHTTPS, terr := tableHTMLRowTransport(opts, cellAt(cells, protoIndex), cellAt(cells, httpsIndex))
			if terr != nil {
				p.reject(line, "", terr.Error())
				continue
			}
			if opts.Protocol != "" && transport != opts.Protocol {
				p.rejectProtocol(line)
				continue
			}
			claims := map[string]string{"protocol": string(transport)}
			if anonymity := vendorAnonymity(cellAt(cells, anonymityIndex)); anonymity != "" {
				claims["anonymity"] = anonymity
			}
			if vendorHTTPS {
				claims["http_connect"] = "true"
			}
			p.accept(line, "", Entry{
				Conn:           domain.Connection{Transport: transport, Host: host, Port: port},
				VendorProtocol: vendorProtocol(vendorHTTPS),
				Claims:         claims,
			})
		}
		return p.out, nil
	}
	return p.out, nil
}

func tableHTMLRowTransport(opts ParseOptions, protoCell, httpsCell string) (domain.Transport, bool, error) {
	if opts.Protocol == domain.TransportSOCKS5 && strings.TrimSpace(protoCell) == "" {
		return "", false, &Error{Kind: KindFormat, Detail: "table row lacks explicit SOCKS5 declaration"}
	}
	transport := opts.Protocol
	if transport == "" {
		transport = domain.TransportHTTP
	}
	vendorHTTPS := false
	if strings.TrimSpace(protoCell) != "" {
		switch strings.ToLower(strings.TrimSpace(protoCell)) {
		case "http":
			transport = domain.TransportHTTP
		case "https":
			transport, vendorHTTPS = domain.TransportHTTP, true
		case "socks5":
			transport = domain.TransportSOCKS5
		case "socks4":
			return "", false, &Error{Kind: KindFormat, Detail: "socks4 is not supported"}
		default:
			return "", false, &Error{Kind: KindFormat, Detail: "unknown protocol declaration"}
		}
	}
	if transport == domain.TransportHTTP && strings.EqualFold(strings.TrimSpace(httpsCell), "yes") {
		vendorHTTPS = true
	}
	return transport, vendorHTTPS, nil
}

// pureBodyListContent accepts only the strict 89ip API shape: an
// <html><body> wrapper whose inner content is nothing but empty lines and
// ip:port rows joined by <br>. Anything else keeps markup semantics.
func pureBodyListContent(body []byte) ([]byte, bool) {
	m := pureBodyListPattern.FindSubmatch(bytes.TrimSpace(body))
	if m == nil {
		return nil, false
	}
	inner := brTagPattern.ReplaceAll(m[1], []byte("\n"))
	hasRow := false
	for _, raw := range bytes.Split(inner, []byte("\n")) {
		line := strings.TrimSpace(string(raw))
		if line == "" {
			continue
		}
		if !pureBodyRowPattern.MatchString(line) {
			return nil, false
		}
		hasRow = true
	}
	if !hasRow {
		return nil, false
	}
	return inner, true
}

func isDemoRow(class string, cells []string) bool {
	if strings.Contains(strings.ToLower(class), "demo") {
		return true
	}
	for _, cell := range cells {
		value := strings.ToLower(cell)
		if strings.Contains(value, "demo") || strings.Contains(value, "演示") {
			return true
		}
	}
	return false
}

func vendorAnonymity(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "elite", "elite proxy", "high anonymous", "high anonymity", "高匿名", "高匿":
		return "elite"
	case "anonymous", "匿名":
		return "anonymous"
	case "transparent", "透明":
		return "transparent"
	}
	return ""
}

func vendorCountry(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 32 {
		return ""
	}
	if strings.ContainsFunc(value, func(r rune) bool { return r < ' ' || r == 127 }) {
		return ""
	}
	return value
}

func vendorProtocol(https bool) string {
	if https {
		return "https"
	}
	return ""
}

func copyClaims(claims map[string]string) map[string]string {
	out := make(map[string]string, len(claims))
	for key, value := range claims {
		out[key] = value
	}
	return out
}

func headerIPPortIndexes(header []string) (int, int) {
	ip := headerIndexAny(header, "ip", "ip address", "ip地址", "ip地址(点击查看)", "address", "host", "地址")
	port := headerIndexAny(header, "port", "端口", "端口号")
	return ip, port
}

func headerIndexAny(header []string, names ...string) int {
	for i, cell := range header {
		value := strings.ToLower(strings.TrimSpace(cell))
		for _, name := range names {
			if value == name {
				return i
			}
		}
	}
	return -1
}

func headerIndex(header []string, name string) int {
	return headerIndexAny(header, name)
}

func cellAt(cells []string, index int) string {
	if index < 0 || index >= len(cells) {
		return ""
	}
	return strings.TrimSpace(cells[index])
}

func classAt(classes []string, index int) string {
	if index < 0 || index >= len(classes) {
		return ""
	}
	return classes[index]
}

func htmlTables(root *html.Node) []*html.Node {
	var tables []*html.Node
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "table" {
			tables = append(tables, n)
			return
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(root)
	return tables
}

func tableRowsOf(table *html.Node) (header []string, rows [][]string, rowClasses []string) {
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "tr" {
			var cells []string
			thCount := 0
			for c := n.FirstChild; c != nil; c = c.NextSibling {
				if c.Type == html.ElementNode && (c.Data == "td" || c.Data == "th") {
					cells = append(cells, strings.TrimSpace(textContent(c)))
					if c.Data == "th" {
						thCount++
					}
				}
			}
			if len(cells) > 0 {
				ip, port := headerIPPortIndexes(cells)
				if header == nil && (thCount == len(cells) || ip >= 0 && port >= 0) {
					header = cells
				} else {
					rows = append(rows, cells)
					rowClass := strings.ToLower(attr(n, "class"))
					for _, a := range n.Attr {
						if a.Key == "data-demo" {
							rowClass += " demo"
						}
					}
					rowClasses = append(rowClasses, rowClass)
				}
			}
			return
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(table)
	return header, rows, rowClasses
}

func htmlTextArea(root *html.Node) *html.Node {
	var walk func(*html.Node) *html.Node
	walk = func(n *html.Node) *html.Node {
		if n.Type == html.ElementNode && n.Data == "textarea" {
			return n
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			if found := walk(c); found != nil {
				return found
			}
		}
		return nil
	}
	return walk(root)
}

func htmlHasIPv4Link(root *html.Node) bool {
	var walk func(*html.Node) bool
	walk = func(n *html.Node) bool {
		if n.Type == html.ElementNode && n.Data == "a" && validIPv4(strings.TrimSpace(textContent(n))) {
			return true
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			if walk(c) {
				return true
			}
		}
		return false
	}
	return walk(root)
}

func hasAncestorID(n *html.Node, ids ...string) bool {
	for ; n != nil; n = n.Parent {
		for _, id := range ids {
			if attr(n, "id") == id {
				return true
			}
		}
	}
	return false
}

// Current iHuan links encode the same IP:port in a relative check.html link.
// Decode only that endpoint token, never JS, and validate it against visible IP.
func ihuanCheckLinkMatches(link *html.Node) bool {
	u, err := url.Parse(attr(link, "href"))
	if err != nil || u.IsAbs() || u.Host != "" || u.Path != "/check.html" || u.Fragment != "" {
		return false
	}
	raw := u.Query().Get("proxy")
	if len(raw) > 128 {
		return false
	}
	token, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return false
	}
	host, portText, ok := splitIPv4Port(string(token))
	port, err := strconv.Atoi(portText)
	return ok && err == nil && port > 0 && port <= 65535 && host == strings.TrimSpace(textContent(link))
}
