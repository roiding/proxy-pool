// Package sites catalogs public free-proxy list pages and their parsers.
// Each catalog entry maps to one site_fetch channel: fetching is bounded
// (page count, body size, per-page delay) and results import additively
// into the normal pool, where validation and retirement keep quality.
// Source definitions follow the parsers implemented by
// github.com/fog-forest/free-proxy-pool (MIT).
package sites

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"proxypool/internal/domain"
)

const (
	// MaxPageBytes bounds one page body; every catalog page is far smaller.
	MaxPageBytes = 512 << 10
	// MaxPages caps auto-detected and configured page counts per fetch.
	MaxPages = 20
	// MinFetchInterval is the per-source guard between manual fetches.
	MinFetchInterval = 15 * time.Second
	// fetchBudget bounds one whole fetch so HTTP responses stay under the
	// server write timeout.
	fetchBudget   = 20 * time.Second
	emptyPageStop = 3
	maxPageDelay  = 5 * time.Second
	userAgent     = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)

type Anonymity string

const (
	AnonymityElite       Anonymity = "elite"
	AnonymityAnonymous   Anonymity = "anonymous"
	AnonymityTransparent Anonymity = "transparent"
	AnonymityUnknown     Anonymity = "unknown"
)

type ParserKind string

const (
	ParserText        ParserKind = "text"         // IPv4:port tokens anywhere in the body
	ParserJSONList    ParserKind = "json_list"    // {data:{list|...}} / list / root array with protocol 1|2
	ParserTableIPPort ParserKind = "table_ipport" // one <td> holds ip:port
	ParserTable2Cols  ParserKind = "table_2col"   // td[ip], td[port]
	ParserTable3Cols  ParserKind = "table_3col"   // td[1], td[2]
	ParserTableTH     ParserKind = "table_th"     // th[ip], th[port]
	ParserRoleCells   ParserKind = "role_cells"   // role=row groups, first two role=cell spans are ip/port
	ParserFPSList     ParserKind = "fpslist"      // const fpsList = [...]; embedded JSON
	ParserFineProxy   ParserKind = "fineproxy"    // JSON whose data.rows is an HTML fragment
)

type Site struct {
	ID        string            `json:"site_id"`
	Name      string            `json:"name"`
	Anonymity Anonymity         `json:"anonymity"`
	Kind      ParserKind        `json:"parser"`
	URL       string            `json:"-"` // {page} placeholder; a literal URL without it fetches one page
	Method    string            `json:"-"`
	Form      map[string]string `json:"-"`
	Pages     int               `json:"pages"`
	AutoPages bool              `json:"auto_pages"`
	DelayMS   int               `json:"delay_ms"`
	Note      string            `json:"note"`
}

// Catalog mirrors the source list of free-proxy-pool. One entry = one channel.
var Catalog = []Site{
	{ID: "ip3366-normal", Name: "云代理(普通)", Anonymity: AnonymityTransparent, Kind: ParserTable2Cols,
		URL: "http://www.ip3366.net/free/?stype=2&page={page}", Pages: 2, DelayMS: 2000},
	{ID: "jiliuip-elite", Name: "积流代理(高匿)", Anonymity: AnonymityElite, Kind: ParserRoleCells,
		URL: "https://www.jiliuip.com/free/page-{page}", Pages: 10, DelayMS: 2000},
	{ID: "qiyunip-elite", Name: "齐云代理(高匿)", Anonymity: AnonymityElite, Kind: ParserTableTH,
		URL: "https://www.qiyunip.com/freeProxy/{page}.html", Pages: 6, DelayMS: 2000},
	{ID: "ip89", Name: "89免费代理", Anonymity: AnonymityUnknown, Kind: ParserText,
		URL: "http://api.89ip.cn/tqdl.html?api=1&num=3000&port=&address=&isp=", Pages: 1, DelayMS: 1000},
	{ID: "lumiproxy-transparent", Name: "LumiProxy(透明)", Anonymity: AnonymityTransparent, Kind: ParserJSONList,
		URL: "https://api.lumiproxy.com/web_v1/free-proxy/list?anonymity=0&page_size=60&page={page}", AutoPages: true, DelayMS: 2000},
	{ID: "lumiproxy-anon", Name: "LumiProxy(普通)", Anonymity: AnonymityAnonymous, Kind: ParserJSONList,
		URL: "https://api.lumiproxy.com/web_v1/free-proxy/list?anonymity=1&page_size=60&page={page}", AutoPages: true, DelayMS: 2000},
	{ID: "lumiproxy-elite", Name: "LumiProxy(高匿)", Anonymity: AnonymityElite, Kind: ParserJSONList,
		URL: "https://api.lumiproxy.com/web_v1/free-proxy/list?anonymity=2&page_size=60&page={page}", AutoPages: true, DelayMS: 2000},
	{ID: "proxyscrape-anon", Name: "ProxyScrape(普通)", Anonymity: AnonymityAnonymous, Kind: ParserText,
		URL: "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&protocol=http&proxy_format=protocolipport&format=text&anonymity=Anonymous,Transparent&timeout=20000", Pages: 1, DelayMS: 1000},
	{ID: "proxyscrape-elite", Name: "ProxyScrape(高匿)", Anonymity: AnonymityElite, Kind: ParserText,
		URL: "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&protocol=http&proxy_format=protocolipport&format=text&anonymity=Elite&timeout=20000", Pages: 1, DelayMS: 1000},
	{ID: "openproxylist-http", Name: "OpenProxyList(HTTP)", Anonymity: AnonymityElite, Kind: ParserText,
		URL: "https://api.openproxylist.xyz/http.txt", Pages: 1, DelayMS: 1000},
	{ID: "openproxylist-https", Name: "OpenProxyList(HTTPS)", Anonymity: AnonymityElite, Kind: ParserText,
		URL: "https://api.openproxylist.xyz/https.txt", Pages: 1, DelayMS: 1000},
}

var catalogByID = func() map[string]Site {
	m := make(map[string]Site, len(Catalog))
	for _, s := range Catalog {
		m[s.ID] = s
	}
	return m
}()

func ByID(id string) (Site, bool) {
	s, ok := catalogByID[id]
	return s, ok
}

type ErrorKind string

const (
	KindNetwork ErrorKind = "network"
	KindFormat  ErrorKind = "format"
)

type Error struct {
	Kind   ErrorKind
	Detail string
}

func (e *Error) Error() string { return string(e.Kind) + ": " + e.Detail }

type RowError struct {
	Line  int    `json:"line"`
	Input string `json:"input"`
	Error string `json:"error"`
}

type Entry struct {
	Conn           domain.Connection
	VendorProtocol string
}

type FetchOutput struct {
	Entries    []Entry
	Rejected   []RowError
	PagesDone  int
	PagesTotal int
}

// FetchSite walks the site's pages, parses each one, and returns validated,
// deduplicated endpoints. Pages that fail or parse empty only count toward
// the consecutive-empty stop; a fetch with zero successful pages fails.
func FetchSite(ctx context.Context, client *http.Client, site Site, maxPages int, allowPrivate bool) (FetchOutput, error) {
	out := FetchOutput{}
	if client == nil {
		client = http.DefaultClient
	}
	pages := site.Pages
	if site.AutoPages || pages <= 0 {
		pages = 1
	}
	if maxPages > 0 && maxPages < pages {
		pages = maxPages
	}
	if pages > MaxPages {
		pages = MaxPages
	}
	delay := time.Duration(site.DelayMS) * time.Millisecond
	if delay > maxPageDelay {
		delay = maxPageDelay
	}

	fetchCtx, cancel := context.WithTimeout(ctx, fetchBudget)
	defer cancel()
	seen := map[string]bool{}
	emptyPages := 0
	var lastErr error
	for page := 1; page <= pages; page++ {
		if err := fetchCtx.Err(); err != nil {
			break
		}
		body, err := fetchPage(fetchCtx, client, site, page)
		if err != nil {
			lastErr = err
			emptyPages++
		} else {
			parsed, perr := parsePage(site.Kind, body)
			if perr != nil {
				lastErr = perr
				emptyPages++
			} else {
				before := len(out.Entries)
				for i, e := range parsed.entries {
					if err := validateConn(e.Conn, allowPrivate); err != nil {
						out.Rejected = append(out.Rejected, RowError{
							Line: (out.PagesDone)*1000 + i + 1, Input: fmt.Sprintf("%s:%d", e.Conn.Host, e.Conn.Port),
							Error: err.Error()})
						continue
					}
					key := string(e.Conn.Transport) + "|" + e.Conn.Host + "|" + strconv.Itoa(e.Conn.Port)
					if seen[key] {
						continue
					}
					seen[key] = true
					out.Entries = append(out.Entries, e)
				}
				out.Rejected = append(out.Rejected, parsed.rejected...)
				out.PagesDone++
				if len(out.Entries) == before {
					emptyPages++
				} else {
					emptyPages = 0
				}
				if site.AutoPages && page == 1 {
					if auto := autoPageCount(body); auto > 0 {
						pages = auto
						capPages := MaxPages
						if maxPages > 0 && maxPages < capPages {
							capPages = maxPages
						}
						if pages > capPages {
							pages = capPages
						}
					}
				}
			}
		}
		out.PagesTotal = pages
		if emptyPages >= emptyPageStop {
			break
		}
		if page < pages && delay > 0 {
			select {
			case <-fetchCtx.Done():
			case <-time.After(delay):
			}
		}
	}
	if out.PagesDone == 0 {
		if lastErr == nil {
			lastErr = &Error{Kind: KindFormat, Detail: "no usable data on any page"}
		}
		return FetchOutput{}, lastErr
	}
	return out, nil
}

func fetchPage(ctx context.Context, client *http.Client, site Site, page int) ([]byte, error) {
	url := strings.ReplaceAll(site.URL, "{page}", strconv.Itoa(page))
	var req *http.Request
	var err error
	if strings.EqualFold(site.Method, http.MethodPost) {
		form := make(map[string][]string, len(site.Form))
		for k, v := range site.Form {
			form[k] = []string{strings.ReplaceAll(v, "{page}", strconv.Itoa(page))}
		}
		req, err = http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(encodeForm(form)))
		if err == nil {
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		}
	} else {
		req, err = http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	}
	if err != nil {
		return nil, &Error{Kind: KindFormat, Detail: "build request: " + err.Error()}
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "text/html,application/json,*/*")
	pageCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	resp, err := client.Do(req.WithContext(pageCtx))
	if err != nil {
		return nil, &Error{Kind: KindNetwork, Detail: "page " + strconv.Itoa(page) + ": " + err.Error()}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, &Error{Kind: KindNetwork, Detail: "page " + strconv.Itoa(page) + ": HTTP " + strconv.Itoa(resp.StatusCode)}
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, MaxPageBytes+1))
	if err != nil {
		return nil, &Error{Kind: KindNetwork, Detail: "read page " + strconv.Itoa(page) + ": " + err.Error()}
	}
	if len(body) > MaxPageBytes {
		return nil, &Error{Kind: KindFormat, Detail: fmt.Sprintf("page %d exceeds %d bytes", page, MaxPageBytes)}
	}
	return body, nil
}

func encodeForm(form map[string][]string) string {
	parts := make([]string, 0, len(form))
	for k, vs := range form {
		for _, v := range vs {
			parts = append(parts, urlQueryEscape(k)+"="+urlQueryEscape(v))
		}
	}
	return strings.Join(parts, "&")
}

func urlQueryEscape(s string) string {
	const hex = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' || '0' <= c && c <= '9' ||
			c == '-' || c == '_' || c == '.' || c == '~' || c == '[' || c == ']' {
			b.WriteByte(c)
			continue
		}
		b.WriteByte('%')
		b.WriteByte(hex[c>>4])
		b.WriteByte(hex[c&0xF])
	}
	return b.String()
}

func validateConn(conn domain.Connection, allowPrivate bool) error {
	if conn.Port < 1 || conn.Port > 65535 {
		return fmt.Errorf("invalid port %d", conn.Port)
	}
	ip, err := netip.ParseAddr(conn.Host)
	if err != nil {
		return fmt.Errorf("invalid host %q", conn.Host)
	}
	if !domain.AddrAllowed(ip, allowPrivate) {
		return fmt.Errorf("address %s rejected by address policy", conn.Host)
	}
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
