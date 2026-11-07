package sites

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/html"
	"proxypool/internal/domain"
	"proxypool/internal/integrations/custom"
)

var (
	ipv4PortPattern = regexp.MustCompile(`(\d{1,3}(?:\.\d{1,3}){3}):(\d{1,5})`)
	fpsListPattern  = regexp.MustCompile(`(?s)const\s+fpsList\s*=\s*(\[.*?\]);`)
	fineRowPattern  = regexp.MustCompile(`<td\s+class=["']table-ip["']\s*>\s*(\d{1,3}(?:\.\d{1,3}){3})\s*</td>\s*<td[^>]*>\s*(\d{1,5})\s*</td>`)
)

type parsedPage struct {
	entries         []Entry
	rejected        []RowError
	rejectedCount   int
	filteredCount   int
	fetched         int
	truncated       bool
	recognized      bool
	sourceUpdatedAt *time.Time
}

type connectionKey struct {
	transport domain.Transport
	host      string
	port      int
	username  string
	password  string
}

type pageBuilder struct {
	opts ParseOptions
	out  parsedPage
	seen map[connectionKey]int
}

func newPageBuilder(opts ParseOptions) *pageBuilder {
	return &pageBuilder{opts: opts, seen: make(map[connectionKey]int)}
}

func (p *pageBuilder) reject(line int, input, reason string) {
	p.out.rejectedCount++
	if len(p.out.rejected) < p.opts.maxErrors() {
		p.out.rejected = append(p.out.rejected, RowError{
			Line: line, Input: redactInput(input), Error: reason,
		})
	}
}

// A declared but nonmatching protocol is a filtered row, not a malformed page.
func (p *pageBuilder) rejectProtocol(line int) {
	p.reject(line, "", "row protocol does not match the source")
	p.out.filteredCount++
}

func (p *pageBuilder) accept(line int, input string, entry Entry) bool {
	if p.opts.Protocol != "" && entry.Conn.Transport != p.opts.Protocol {
		p.rejectProtocol(line)
		return false
	}
	if p.out.fetched >= p.opts.maxRecords() {
		p.out.truncated = true
		return false
	}
	conn, err := normalizeConnection(p.opts, entry.Conn)
	if err != nil {
		p.reject(line, input, err.Error())
		return false
	}
	entry.Conn = conn
	p.out.fetched++
	key := keyForConnection(conn)
	if index, ok := p.seen[key]; ok {
		mergeEntryClaims(&p.out.entries[index], entry)
		return true
	}
	p.seen[key] = len(p.out.entries)
	p.out.entries = append(p.out.entries, entry)
	return true
}

func (p *pageBuilder) updateSourceTime(value *time.Time) {
	if value == nil {
		return
	}
	utc := value.UTC()
	if p.out.sourceUpdatedAt == nil || utc.After(*p.out.sourceUpdatedAt) {
		p.out.sourceUpdatedAt = &utc
	}
}

func (o ParseOptions) maxRecords() int {
	if o.MaxRecords > 0 {
		return min(o.MaxRecords, DefaultMaxRecords)
	}
	return DefaultMaxRecords
}

func (o ParseOptions) maxErrors() int {
	if o.MaxErrors > 0 {
		return min(o.MaxErrors, DefaultMaxErrors)
	}
	return DefaultMaxErrors
}

func (o ParseOptions) maxRowBytes() int {
	if o.MaxRowBytes > 0 {
		return min(o.MaxRowBytes, DefaultMaxRowBytes)
	}
	return DefaultMaxRowBytes
}

func redactInput(input string) string {
	return ""
}

func normalizeConnection(opts ParseOptions, conn domain.Connection) (domain.Connection, error) {
	if conn.Port < 1 || conn.Port > 65535 {
		return domain.Connection{}, &Error{Kind: KindFormat, Detail: "invalid port"}
	}
	if conn.Transport != domain.TransportHTTP && conn.Transport != domain.TransportSOCKS5 {
		return domain.Connection{}, &Error{Kind: KindFormat, Detail: "unsupported transport"}
	}
	if err := custom.ValidateCredentials(conn.Username, conn.Password); err != nil {
		return domain.Connection{}, &Error{Kind: KindFormat, Detail: err.Error()}
	}
	host, err := custom.NormalizeHost(conn.Host, opts.AllowPrivate)
	if err != nil {
		return domain.Connection{}, err
	}
	conn.Host = host
	return conn, nil
}

func keyForConnection(conn domain.Connection) connectionKey {
	return connectionKey{
		transport: conn.Transport,
		host:      domain.CanonicalHost(conn.Host),
		port:      conn.Port,
		username:  conn.Username,
		password:  conn.Password,
	}
}

func mergeEntryClaims(dst *Entry, src Entry) {
	if dst.VendorProtocol == "" {
		dst.VendorProtocol = src.VendorProtocol
	} else if dst.VendorProtocol == "http" && src.VendorProtocol == "https" {
		dst.VendorProtocol = "https"
	}
	if dst.RecordID == "" {
		dst.RecordID = src.RecordID
	}
	if dst.ProviderExpiresAt == nil {
		dst.ProviderExpiresAt = src.ProviderExpiresAt
	}
	if len(src.Claims) == 0 {
		return
	}
	if dst.Claims == nil {
		dst.Claims = make(map[string]string, len(src.Claims))
	}
	for key, value := range src.Claims {
		if key == "protocols" && dst.Claims[key] != "" && value != "" && dst.Claims[key] != value {
			dst.Claims[key] = mergeCSV(dst.Claims[key], value)
			continue
		}
		if _, exists := dst.Claims[key]; !exists {
			dst.Claims[key] = value
		}
	}
}

func mergeCSV(left, right string) string {
	seen := make(map[string]bool)
	var values []string
	for _, group := range []string{left, right} {
		for _, value := range strings.Split(group, ",") {
			value = strings.TrimSpace(value)
			if value == "" || seen[value] {
				continue
			}
			seen[value] = true
			values = append(values, value)
		}
	}
	return strings.Join(values, ",")
}

// parsePage converts one fetched page into endpoint entries. It is the
// internal canonical parser used by FetchSite; ParseBody is the read-only
// fixture/console wrapper with the same signature.
func parsePage(opts ParseOptions, kind ParserKind, body []byte) (parsedPage, error) {
	if opts.Protocol != "" && opts.Protocol != domain.TransportHTTP &&
		opts.Protocol != domain.TransportSOCKS5 {
		return parsedPage{}, &Error{Kind: KindFormat, Detail: "unsupported parser protocol"}
	}
	if len(body) > 4<<20 {
		return parsedPage{}, &Error{Kind: KindFormat, Detail: "parser body exceeds its byte limit"}
	}
	body = trimUTF8BOM(body)
	if len(strings.TrimSpace(string(body))) == 0 {
		if kind == ParserLines || kind == ParserText {
			return parsedPage{recognized: true}, nil
		}
		return parsedPage{}, &Error{Kind: KindFormat, Detail: "empty response has no declared list structure"}
	}
	decoded, err := decodePage(body)
	if err != nil {
		return parsedPage{}, err
	}
	body = decoded
	if envelopeErr := responseEnvelopeError(body); envelopeErr != nil {
		return parsedPage{}, envelopeErr
	}
	if blocked, reason := blockedPageReason(body); blocked {
		return parsedPage{}, &Error{Kind: KindAccess, Detail: reason}
	}

	var page parsedPage
	switch kind {
	case ParserText:
		page, err = parseText(opts, body)
	case ParserJSONList:
		page, err = parseJSONList(opts, body)
	case ParserTableIPPort:
		page, err = parseTable(opts, body, -1, "td")
	case ParserTable2Cols:
		page, err = parseTable(opts, body, 0, "td")
	case ParserTable3Cols:
		page, err = parseTable(opts, body, 1, "td")
	case ParserTableTH:
		page, err = parseTable(opts, body, 0, "th")
	case ParserRoleCells:
		page, err = parseRoleCells(opts, body)
	case ParserFPSList:
		page, err = parseFPSList(opts, body)
	case ParserFineProxy:
		page, err = parseFineProxy(opts, body)
	case ParserLines:
		page, err = parseLines(opts, body)
	case ParserSpysText:
		page, err = parseSpysText(opts, body)
	case ParserGeonodeJSON:
		page, err = parseGeonodeJSON(opts, body)
	case ParserFreeProxy:
		page, err = parseFreeProxy(opts, body)
	case ParserIHuanHTML:
		page, err = parseIHuanHTML(opts, body)
	case ParserIP89HTML:
		page, err = parseIP89HTML(opts, body)
	case ParserKuaidaili:
		page, err = parseKuaidaili(opts, body)
	case ParserTableHTML:
		page, err = parseTableHTML(opts, body)
	default:
		return parsedPage{}, &Error{Kind: KindFormat, Detail: "unknown parser " + string(kind)}
	}
	if err != nil {
		return parsedPage{}, err
	}
	if len(page.entries) == 0 {
		if page.rejectedCount > page.filteredCount {
			return parsedPage{}, &Error{Kind: KindFormat, Detail: "no valid proxy records in " + string(kind) + " page"}
		}
		if !page.recognized {
			return parsedPage{}, &Error{Kind: KindFormat, Detail: "unrecognized " + string(kind) + " page structure"}
		}
	}
	return page, nil
}

// ParseBody parses one controlled fixture or fetched body. Errors never
// include the body, query string, or raw credential-bearing row.
func ParseBody(opts ParseOptions, kind ParserKind, body []byte) (ParseOutput, error) {
	page, err := parsePage(opts, kind, body)
	if err != nil {
		return ParseOutput{}, err
	}
	return ParseOutput{
		Entries:         append([]Entry(nil), page.entries...),
		Rejected:        append([]RowError(nil), page.rejected...),
		Fetched:         page.fetched,
		Unique:          len(page.entries),
		RejectedCount:   page.rejectedCount,
		Truncated:       page.truncated,
		Complete:        !page.truncated,
		SourceUpdatedAt: page.sourceUpdatedAt,
	}, nil
}

// Legacy text parsing is intentionally not an unanchored IPv4 regex. It
// retains line credentials/protocols and refuses HTML/error-page endpoints,
// except for the 89ip API shape: a pure <html><body> wrapper holding only
// ip:port rows separated by <br>.
func parseText(opts ParseOptions, body []byte) (parsedPage, error) {
	if content, ok := ip89AdvertisedListContent(body); ok {
		return parseLines(opts, content)
	}
	if content, ok := pureBodyListContent(body); ok {
		return parseLines(opts, content)
	}
	return parseLines(opts, body)
}

type jsonListEnvelope struct {
	Data json.RawMessage `json:"data"`
	List json.RawMessage `json:"list"`
}

type listEnvelope struct {
	List      json.RawMessage `json:"list"`
	PageCount json.RawMessage `json:"page_count"`
}

type jsonProxyItem struct {
	Protocol json.RawMessage `json:"protocol"`
	IP       json.RawMessage `json:"ip"`
	Port     json.RawMessage `json:"port"`
	Username string          `json:"username"`
	Password string          `json:"password"`
}

func parseJSONList(opts ParseOptions, body []byte) (parsedPage, error) {
	p := newPageBuilder(opts)
	list, err := extractProxyList(body)
	if err != nil {
		return parsedPage{}, err
	}
	p.out.recognized = true
	var items []jsonProxyItem
	if err := json.Unmarshal(list, &items); err != nil {
		return parsedPage{}, &Error{Kind: KindFormat, Detail: "decode proxy list"}
	}
	for i, item := range items {
		if i >= opts.maxRecords() {
			p.out.truncated = true
			break
		}
		input := jsonText(item.IP) + ":" + jsonText(item.Port)
		proto, ok := jsonInt(item.Protocol)
		if !ok || (proto != 1 && proto != 2) {
			p.reject(i+1, input, "unsupported protocol")
			continue
		}
		ip := jsonText(item.IP)
		port, ok := jsonInt(item.Port)
		if ip == "" || !ok || !validIPv4(ip) || port < 1 || port > 65535 {
			p.reject(i+1, input, "invalid ip or port")
			continue
		}
		transport := domain.TransportHTTP
		if opts.Protocol == domain.TransportSOCKS5 {
			p.rejectProtocol(i + 1)
			continue
		}
		e := Entry{
			Conn:   domain.Connection{Transport: transport, Host: ip, Port: port, Username: item.Username, Password: item.Password},
			Claims: map[string]string{"protocol": "http"},
		}
		if proto == 2 {
			e.VendorProtocol = "https"
			e.Claims["protocol"] = "https"
			e.Claims["http_connect"] = "true"
		}
		p.accept(i+1, input, e)
	}
	return p.out, nil
}

// extractProxyList accepts data.data.list, data.list, a bare list, or a root
// array, mirroring the vendor shape tolerance of free-proxy-pool.
func extractProxyList(body []byte) (json.RawMessage, error) {
	// Only an actual array is a list; null/missing/wrong-typed envelopes fail.
	var find func(json.RawMessage, int) json.RawMessage
	find = func(raw json.RawMessage, depth int) json.RawMessage {
		raw = bytes.TrimSpace(raw)
		if len(raw) > 0 && raw[0] == '[' {
			var arr []json.RawMessage
			if json.Unmarshal(raw, &arr) == nil {
				return raw
			}
			return nil
		}
		if depth >= 3 {
			return nil
		}
		var obj map[string]json.RawMessage
		if json.Unmarshal(raw, &obj) != nil || obj == nil {
			return nil
		}
		if list, ok := obj["list"]; ok {
			list = bytes.TrimSpace(list)
			if len(list) > 0 && list[0] == '[' {
				return find(list, depth+1)
			}
			return nil
		}
		if data, ok := obj["data"]; ok {
			return find(data, depth+1)
		}
		return nil
	}
	if list := find(body, 0); list != nil {
		return list, nil
	}
	return nil, &Error{Kind: KindFormat, Detail: "no non-null proxy list found in JSON"}
}

func autoPageCount(body []byte) int {
	var find func(json.RawMessage, int) int
	find = func(raw json.RawMessage, depth int) int {
		if depth > 3 {
			return 0
		}
		var obj map[string]json.RawMessage
		if json.Unmarshal(raw, &obj) != nil {
			return 0
		}
		for _, key := range []string{"page_count", "total_pages", "total_page"} {
			if n, ok := jsonInt(obj[key]); ok && n > 0 {
				return n
			}
		}
		return find(obj["data"], depth+1)
	}
	return find(body, 0)
}

func parseTable(opts ParseOptions, body []byte, ipIndex int, cellTag string) (parsedPage, error) {
	p := newPageBuilder(opts)
	p.out.recognized = strings.Contains(strings.ToLower(string(body)), "<table")
	protocol := opts.Protocol
	if protocol == "" {
		protocol = domain.TransportHTTP
	}
	for rowIndex, cells := range tableRows(body, cellTag) {
		if rowIndex >= opts.maxRecords()+1 {
			p.out.truncated = true
			break
		}
		var host string
		var portText string
		if ipIndex < 0 {
			if len(cells) < 1 {
				continue
			}
			m := ipv4PortPattern.FindStringSubmatch(strings.TrimSpace(cells[0]))
			if m == nil || m[0] != strings.TrimSpace(cells[0]) {
				if strings.Contains(cells[0], ":") {
					p.reject(rowIndex+1, "", "invalid endpoint cell")
				}
				continue
			}
			host, portText = m[1], m[2]
		} else {
			if len(cells) < ipIndex+2 {
				if len(cells) > 0 && !isTableHeader(cells) {
					p.reject(rowIndex+1, strings.Join(cells, "|"), "missing ip or port column")
				}
				continue
			}
			host, portText = cells[ipIndex], cells[ipIndex+1]
		}
		if isTableHeader(append([]string{host}, portText)) {
			continue
		}
		port, err := strconv.Atoi(strings.TrimSpace(portText))
		if err != nil || port < 1 || port > 65535 {
			p.reject(rowIndex+1, host+":"+portText, "invalid ip or port")
			continue
		}
		p.accept(rowIndex+1, host+":"+portText, Entry{
			Conn: domain.Connection{
				Transport: protocol, Host: strings.TrimSpace(host), Port: port,
			},
			Claims: map[string]string{"protocol": string(protocol)},
		})
	}
	return p.out, nil
}

func isTableHeader(cells []string) bool {
	for _, cell := range cells {
		value := strings.ToLower(strings.TrimSpace(cell))
		switch value {
		case "ip", "ip address", "ip地址", "host", "address", "地址", "port", "端口":
			return true
		}
	}
	return false
}

// parseRoleCells reads aria-role based "tables" (jiliuip's v2 layout): every
// role=row element contributes role=cell descendants; cells[0] is the IPv4
// and cells[1] the port.
func parseRoleCells(opts ParseOptions, body []byte) (parsedPage, error) {
	p := newPageBuilder(opts)
	root, err := html.Parse(strings.NewReader(string(body)))
	if err != nil {
		return parsedPage{}, &Error{Kind: KindFormat, Detail: "invalid role-cell HTML"}
	}
	protocol := opts.Protocol
	if protocol == "" {
		protocol = domain.TransportHTTP
	}
	rowNumber := 0
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if p.out.truncated {
			return
		}
		if n.Type == html.ElementNode && attr(n, "role") == "row" {
			p.out.recognized = true
			rowNumber++
			if rowNumber > opts.maxRecords()+1 {
				p.out.truncated = true
				return
			}
			var cells []string
			var collect func(*html.Node)
			collect = func(c *html.Node) {
				if c.Type == html.ElementNode && attr(c, "role") == "cell" {
					cells = append(cells, strings.TrimSpace(textContent(c)))
				}
				for k := c.FirstChild; k != nil; k = k.NextSibling {
					collect(k)
				}
			}
			for c := n.FirstChild; c != nil; c = c.NextSibling {
				collect(c)
			}
			if len(cells) < 2 {
				if len(cells) > 0 {
					p.reject(rowNumber, strings.Join(cells, "|"), "missing ip or port cell")
				}
				return
			}
			port, perr := strconv.Atoi(cells[1])
			if perr != nil || port < 1 || port > 65535 {
				p.reject(rowNumber, cells[0]+":"+cells[1], "invalid port")
				return
			}
			claims := map[string]string{"protocol": string(protocol)}
			if len(cells) > 2 && strings.TrimSpace(cells[2]) != "" {
				claims["anonymity"] = strings.TrimSpace(cells[2])
			}
			p.accept(rowNumber, cells[0]+":"+cells[1], Entry{
				Conn: domain.Connection{
					Transport: protocol, Host: cells[0], Port: port,
				},
				Claims: claims,
			})
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(root)
	return p.out, nil
}

func attr(n *html.Node, key string) string {
	for _, a := range n.Attr {
		if a.Key == key {
			return a.Val
		}
	}
	return ""
}

// tableRows extracts trimmed cell text for every row built from the given
// cell tag, matching the BeautifulSoup find_all('tr') behaviour of the
// reference implementation.
func tableRows(body []byte, cellTag string) [][]string {
	root, err := html.Parse(strings.NewReader(string(body)))
	if err != nil {
		return nil
	}
	var rows [][]string
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "tr" {
			var cells []string
			for c := n.FirstChild; c != nil; c = c.NextSibling {
				if c.Type == html.ElementNode && c.Data == cellTag {
					cells = append(cells, strings.TrimSpace(textContent(c)))
				}
			}
			if len(cells) > 0 {
				rows = append(rows, cells)
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(root)
	return rows
}

func textContent(n *html.Node) string {
	var b strings.Builder
	var walk func(*html.Node)
	walk = func(cur *html.Node) {
		if cur.Type == html.TextNode {
			b.WriteString(cur.Data)
		}
		for c := cur.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(n)
	return b.String()
}

func parseFPSList(opts ParseOptions, body []byte) (parsedPage, error) {
	p := newPageBuilder(opts)
	m := fpsListPattern.FindSubmatch(body)
	if m == nil {
		return parsedPage{}, &Error{Kind: KindFormat, Detail: "fpsList declaration not found"}
	}
	p.out.recognized = true
	var items []struct {
		IP       json.RawMessage `json:"ip"`
		Port     json.RawMessage `json:"port"`
		Username string          `json:"username"`
		Password string          `json:"password"`
	}
	if err := json.Unmarshal(m[1], &items); err != nil {
		return parsedPage{}, &Error{Kind: KindFormat, Detail: "invalid fpsList JSON"}
	}
	for i, item := range items {
		if i >= opts.maxRecords() {
			p.out.truncated = true
			break
		}
		ip := jsonText(item.IP)
		port, ok := jsonInt(item.Port)
		if ip == "" || !ok || port < 1 || port > 65535 {
			p.reject(i+1, ip+":"+jsonText(item.Port), "invalid ip or port")
			continue
		}
		p.accept(i+1, ip+":"+jsonText(item.Port), Entry{
			Conn: domain.Connection{
				Transport: domain.TransportHTTP, Host: ip, Port: port, Username: item.Username, Password: item.Password,
			},
			Claims: map[string]string{"protocol": "http"},
		})
	}
	return p.out, nil
}

func parseFineProxy(opts ParseOptions, body []byte) (parsedPage, error) {
	p := newPageBuilder(opts)
	var env struct {
		Data *struct {
			Rows *string `json:"rows"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return parsedPage{}, &Error{Kind: KindFormat, Detail: "invalid fineproxy JSON"}
	}
	if env.Data == nil || env.Data.Rows == nil {
		return parsedPage{}, &Error{Kind: KindFormat, Detail: "fineproxy response lacks data.rows string"}
	}
	p.out.recognized = true
	rows := strings.TrimSpace(*env.Data.Rows)
	if rows == "" {
		return p.out, nil
	}
	matches := fineRowPattern.FindAllStringSubmatch(rows, -1)
	if len(matches) == 0 {
		p.reject(1, "", "fineproxy rows do not contain a recognized table")
		return p.out, nil
	}
	for i, m := range matches {
		if i >= opts.maxRecords() {
			p.out.truncated = true
			break
		}
		port, err := strconv.Atoi(m[2])
		if err != nil || port < 1 || port > 65535 {
			p.reject(i+1, m[1]+":"+m[2], "invalid ip or port")
			continue
		}
		p.accept(i+1, m[1]+":"+m[2], Entry{
			Conn: domain.Connection{
				Transport: domain.TransportHTTP, Host: m[1], Port: port,
			},
			Claims: map[string]string{"protocol": "http"},
		})
	}
	return p.out, nil
}

func validIPv4(host string) bool {
	parts := strings.Split(host, ".")
	if len(parts) != 4 {
		return false
	}
	for _, p := range parts {
		if p == "" || len(p) > 3 {
			return false
		}
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 || n > 255 {
			return false
		}
	}
	return true
}

func jsonInt(raw json.RawMessage) (int, bool) {
	s := strings.Trim(strings.TrimSpace(string(raw)), `"`)
	if s == "" || s == "null" {
		return 0, false
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, false
	}
	return n, true
}

func jsonText(raw json.RawMessage) string {
	s := strings.TrimSpace(string(raw))
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		var v string
		if err := json.Unmarshal(raw, &v); err == nil {
			return strings.TrimSpace(v)
		}
	}
	return s
}
