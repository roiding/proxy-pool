package sites

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"

	"golang.org/x/net/html"
	"proxypool/internal/domain"
)

var (
	ipv4PortPattern = regexp.MustCompile(`(\d{1,3}(?:\.\d{1,3}){3}):(\d{1,5})`)
	fpsListPattern  = regexp.MustCompile(`const\s+fpsList\s*=\s*(\[.*?\]);`)
	fineRowPattern  = regexp.MustCompile(`<td\s+class=["']table-ip["']\s*>\s*(\d{1,3}(?:\.\d{1,3}){3})\s*</td>\s*<td[^>]*>\s*(\d{1,5})\s*</td>`)
)

type parsedPage struct {
	entries  []Entry
	rejected []RowError
}

// parsePage converts one fetched page into endpoint entries. Header rows and
// advertising rows in HTML tables are skipped silently; structured formats
// report per-row failures so operators can see vendor drift.
func parsePage(kind ParserKind, body []byte) (parsedPage, error) {
	switch kind {
	case ParserText:
		return parseText(body), nil
	case ParserJSONList:
		return parseJSONList(body)
	case ParserTableIPPort:
		return parseTable(body, -1), nil
	case ParserTable2Cols:
		return parseTable(body, 0), nil
	case ParserTable3Cols:
		return parseTable(body, 1), nil
	case ParserTableTH:
		return parseTH(body), nil
	case ParserRoleCells:
		return parseRoleCells(body), nil
	case ParserFPSList:
		return parseFPSList(body), nil
	case ParserFineProxy:
		return parseFineProxy(body)
	default:
		return parsedPage{}, &Error{Kind: KindFormat, Detail: "unknown parser " + string(kind)}
	}
}

func parseText(body []byte) parsedPage {
	var out parsedPage
	seen := map[string]bool{}
	for _, m := range ipv4PortPattern.FindAllStringSubmatch(string(body), -1) {
		port, err := strconv.Atoi(m[2])
		if err != nil || port < 1 || port > 65535 || !validIPv4(m[1]) {
			continue
		}
		if seen[m[1]+"|"+m[2]] {
			continue
		}
		seen[m[1]+"|"+m[2]] = true
		out.entries = append(out.entries, Entry{Conn: domain.Connection{Transport: domain.TransportHTTP, Host: m[1], Port: port}})
	}
	return out
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
}

func parseJSONList(body []byte) (parsedPage, error) {
	var out parsedPage
	list, err := extractProxyList(body)
	if err != nil {
		return out, err
	}
	var items []jsonProxyItem
	if err := json.Unmarshal(list, &items); err != nil {
		return out, &Error{Kind: KindFormat, Detail: "decode proxy list: " + err.Error()}
	}
	for i, item := range items {
		proto, ok := jsonInt(item.Protocol)
		if !ok || (proto != 1 && proto != 2) {
			continue
		}
		ip := jsonText(item.IP)
		port, ok := jsonInt(item.Port)
		if ip == "" || !ok || !validIPv4(ip) || port < 1 || port > 65535 {
			out.rejected = append(out.rejected, RowError{Line: i + 1,
				Input: strings.TrimSpace(string(item.IP)) + ":" + strings.TrimSpace(string(item.Port)),
				Error: "invalid ip or port"})
			continue
		}
		e := Entry{Conn: domain.Connection{Transport: domain.TransportHTTP, Host: ip, Port: port}}
		if proto == 2 {
			e.VendorProtocol = "https"
		}
		out.entries = append(out.entries, e)
	}
	return out, nil
}

// extractProxyList accepts data.data.list, data.list, a bare list, or a root
// array, mirroring the vendor shape tolerance of free-proxy-pool.
func extractProxyList(body []byte) (json.RawMessage, error) {
	var root json.RawMessage
	if err := json.Unmarshal(body, &root); err != nil {
		return nil, &Error{Kind: KindFormat, Detail: "body is not JSON: " + err.Error()}
	}
	var env jsonListEnvelope
	if err := json.Unmarshal(body, &env); err == nil {
		if len(env.Data) > 0 && string(env.Data) != "null" {
			var inner listEnvelope
			if err := json.Unmarshal(env.Data, &inner); err == nil && len(inner.List) > 0 {
				return inner.List, nil
			}
		}
		if len(env.List) > 0 {
			return env.List, nil
		}
	}
	var arr []json.RawMessage
	if err := json.Unmarshal(root, &arr); err == nil {
		return root, nil
	}
	return nil, &Error{Kind: KindFormat, Detail: "no proxy list found in JSON"}
}

func autoPageCount(body []byte) int {
	var env jsonListEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return 0
	}
	candidates := []json.RawMessage{}
	if len(env.Data) > 0 && string(env.Data) != "null" {
		var inner map[string]json.RawMessage
		if json.Unmarshal(env.Data, &inner) == nil {
			for _, k := range []string{"page_count", "total_pages", "total_page"} {
				if v, ok := inner[k]; ok {
					candidates = append(candidates, v)
				}
			}
		}
	}
	var top map[string]json.RawMessage
	if json.Unmarshal(body, &top) == nil {
		if v, ok := top["page_count"]; ok {
			candidates = append(candidates, v)
		}
	}
	for _, c := range candidates {
		if n, ok := jsonInt(c); ok && n > 0 {
			return n
		}
	}
	return 0
}

func parseTable(body []byte, ipIndex int) parsedPage {
	var out parsedPage
	seen := map[string]bool{}
	for _, cells := range tableRows(body, "td") {
		var host string
		var portText string
		if ipIndex < 0 {
			if len(cells) < 1 {
				continue
			}
			m := ipv4PortPattern.FindStringSubmatch(cells[0])
			if m == nil {
				continue
			}
			host, portText = m[1], m[2]
		} else {
			if len(cells) < ipIndex+2 {
				continue
			}
			host, portText = cells[ipIndex], cells[ipIndex+1]
		}
		port, err := strconv.Atoi(strings.TrimSpace(portText))
		if err != nil || port < 1 || port > 65535 || !validIPv4(strings.TrimSpace(host)) {
			continue
		}
		key := host + "|" + strings.TrimSpace(portText)
		if seen[key] {
			continue
		}
		seen[key] = true
		out.entries = append(out.entries, Entry{Conn: domain.Connection{
			Transport: domain.TransportHTTP, Host: strings.ToLower(strings.TrimSpace(host)), Port: port}})
	}
	return out
}

func parseTH(body []byte) parsedPage {
	var out parsedPage
	seen := map[string]bool{}
	for _, cells := range tableRows(body, "th") {
		if len(cells) < 2 {
			continue
		}
		host := strings.TrimSpace(cells[0])
		port, err := strconv.Atoi(strings.TrimSpace(cells[1]))
		if err != nil || port < 1 || port > 65535 || !validIPv4(host) {
			continue
		}
		key := host + "|" + strconv.Itoa(port)
		if seen[key] {
			continue
		}
		seen[key] = true
		out.entries = append(out.entries, Entry{Conn: domain.Connection{
			Transport: domain.TransportHTTP, Host: strings.ToLower(host), Port: port}})
	}
	return out
}

// parseRoleCells reads aria-role based "tables" (jiliuip's v2 layout): every
// role=row element contributes role=cell descendants; cells[0] is the IPv4
// and cells[1] the port.
func parseRoleCells(body []byte) parsedPage {
	var out parsedPage
	root, err := html.Parse(strings.NewReader(string(body)))
	if err != nil {
		return out
	}
	seen := map[string]bool{}
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode && attr(n, "role") == "row" {
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
			if len(cells) >= 2 {
				host := strings.ToLower(cells[0])
				port, perr := strconv.Atoi(cells[1])
				if perr == nil && port >= 1 && port <= 65535 && validIPv4(host) {
					key := host + "|" + strconv.Itoa(port)
					if !seen[key] {
						seen[key] = true
						out.entries = append(out.entries, Entry{Conn: domain.Connection{
							Transport: domain.TransportHTTP, Host: host, Port: port}})
					}
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(root)
	return out
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

func parseFPSList(body []byte) parsedPage {
	var out parsedPage
	m := fpsListPattern.FindSubmatch(body)
	if m == nil {
		return out
	}
	var items []struct {
		IP   json.RawMessage `json:"ip"`
		Port json.RawMessage `json:"port"`
	}
	if err := json.Unmarshal(m[1], &items); err != nil {
		return out
	}
	seen := map[string]bool{}
	for i, item := range items {
		ip := jsonText(item.IP)
		port, ok := jsonInt(item.Port)
		if ip == "" || !ok || !validIPv4(ip) || port < 1 || port > 65535 {
			out.rejected = append(out.rejected, RowError{Line: i + 1,
				Input: jsonText(item.IP) + ":" + jsonText(item.Port), Error: "invalid ip or port"})
			continue
		}
		key := ip + "|" + strconv.Itoa(port)
		if seen[key] {
			continue
		}
		seen[key] = true
		out.entries = append(out.entries, Entry{Conn: domain.Connection{
			Transport: domain.TransportHTTP, Host: strings.ToLower(ip), Port: port}})
	}
	return out
}

func parseFineProxy(body []byte) (parsedPage, error) {
	var out parsedPage
	var env struct {
		Data struct {
			Rows string `json:"rows"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return out, &Error{Kind: KindFormat, Detail: "decode fineproxy JSON: " + err.Error()}
	}
	rows := strings.TrimSpace(env.Data.Rows)
	if rows == "" {
		return out, nil
	}
	seen := map[string]bool{}
	for i, m := range fineRowPattern.FindAllStringSubmatch(rows, -1) {
		port, err := strconv.Atoi(m[2])
		if err != nil || port < 1 || port > 65535 || !validIPv4(m[1]) {
			out.rejected = append(out.rejected, RowError{Line: i + 1, Input: m[1] + ":" + m[2], Error: "invalid ip or port"})
			continue
		}
		key := m[1] + "|" + m[2]
		if seen[key] {
			continue
		}
		seen[key] = true
		out.entries = append(out.entries, Entry{Conn: domain.Connection{
			Transport: domain.TransportHTTP, Host: m[1], Port: port}})
	}
	return out, nil
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
