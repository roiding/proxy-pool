// Package sites catalogs public free-proxy list pages and their parsers.
// Each catalog entry maps to one site_fetch channel: fetching is bounded
// (page count, body size, per-page delay) and results import additively
// into the normal pool, where validation and retirement keep quality.
// Source definitions follow the parsers implemented by
// github.com/fog-forest/free-proxy-pool (MIT).
package sites

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"proxypool/internal/domain"
	"proxypool/internal/integrations/custom"
	"proxypool/internal/integrations/sourcehttp"
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
	ParserText        ParserKind = "text"         // strict text rows or declared 89ip list markup
	ParserJSONList    ParserKind = "json_list"    // {data:{list|...}} / list / root array with protocol 1|2
	ParserTableIPPort ParserKind = "table_ipport" // one <td> holds ip:port
	ParserTable2Cols  ParserKind = "table_2col"   // td[ip], td[port]
	ParserTable3Cols  ParserKind = "table_3col"   // td[1], td[2]
	ParserTableTH     ParserKind = "table_th"     // th[ip], th[port]
	ParserRoleCells   ParserKind = "role_cells"   // role=row groups, first two role=cell spans are ip/port
	ParserFPSList     ParserKind = "fpslist"      // const fpsList = [...]; embedded JSON
	ParserFineProxy   ParserKind = "fineproxy"    // JSON whose data.rows is an HTML fragment
	ParserLines       ParserKind = "lines"        // bounded line/URI parser, credential preserving
	ParserSpysText    ParserKind = "spys_text"    // Spys first-column connection plus metadata
	ParserGeonodeJSON ParserKind = "geonode_json" // data[] with protocols[] and lastChecked
	ParserFreeProxy   ParserKind = "freeproxy"    // Free-Proxy-List table/textarea family
	ParserIHuanHTML   ParserKind = "ihuan_html"   // ip.ihuan.me IP link plus adjacent port
	ParserIP89HTML    ParserKind = "ip89_html"    // 89ip.cn named-column HTML fallback
	ParserKuaidaili   ParserKind = "kuaidaili"    // kuaidaili free table, demo rows excluded
	ParserTableHTML   ParserKind = "table_html"   // header-aware HTTP table with optional protocol column
)

type SiteState string

const (
	SiteActive           SiteState = "active"
	SitePaused           SiteState = "paused"
	SiteAwaitingProtocol SiteState = "awaiting_protocol"
	SiteAwaitingContract SiteState = "awaiting_contract"
	SiteIncompatible     SiteState = "incompatible"
	SiteRetired          SiteState = "retired"
)

func (s SiteState) Runnable() bool { return s == "" || s == SiteActive }

type Site struct {
	ID             string            `json:"site_id"`
	Name           string            `json:"name"`
	Anonymity      Anonymity         `json:"anonymity"`
	Protocol       domain.Transport  `json:"protocol"`
	VendorProtocol string            `json:"vendor_protocol,omitempty"`
	Kind           ParserKind        `json:"parser"`
	ParserVersion  string            `json:"parser_version"`
	URL            string            `json:"-"` // {page} placeholder; a literal URL without it fetches one page
	Method         string            `json:"-"`
	Form           map[string]string `json:"-"`
	Pages          int               `json:"pages"`
	AutoPages      bool              `json:"auto_pages"`
	DelayMS        int               `json:"delay_ms"`
	State          SiteState         `json:"state"`
	Note           string            `json:"note"`
}

// ParseOptions bounds one parser invocation. Zero values select safe
// defaults; MaxErrors bounds diagnostics only, never the rejected count.
type ParseOptions struct {
	Protocol         domain.Transport
	DefaultTransport domain.Transport
	AllowPrivate     bool
	MaxRecords       int
	MaxRowBytes      int
	MaxErrors        int
}

// ParseOutput is shared by FetchSite and the read-only fixture/console path.
type ParseOutput struct {
	Entries         []Entry
	Rejected        []RowError
	Fetched         int
	Unique          int
	RejectedCount   int
	Truncated       bool
	Complete        bool
	SourceUpdatedAt *time.Time
}

const (
	DefaultMaxRecords  = 10000
	DefaultMaxRowBytes = 4096
	DefaultMaxErrors   = 100
)

// Catalog is the low-level endpoint/parser diagnostic registry. Protocol
// variants are grouped into supplier channels by channels.go. An empty State is compatible with historical
// in-process Site literals; every code-owned catalog entry is explicit.
var Catalog = []Site{
	{ID: "ip3366-normal", Name: "云代理(普通)", Anonymity: AnonymityTransparent,
		Protocol: domain.TransportHTTP, Kind: ParserTable2Cols, ParserVersion: "table_2col_v1",
		URL: "http://www.ip3366.net/free/?stype=2&page={page}", Pages: 2, DelayMS: 2000,
		State: SiteActive, Note: "GBK page; configured HTTP table"},
	{ID: "ip3366-elite", Name: "云代理(高匿)", Anonymity: AnonymityElite,
		Protocol: domain.TransportHTTP, Kind: ParserTable2Cols, ParserVersion: "table_2col_v1",
		URL: "http://www.ip3366.net/free/?stype=1&page={page}", Pages: 1, DelayMS: 2000,
		State: SiteActive, Note: "GBK page; anonymity category remains a claim"},
	{ID: "jiliuip-elite", Name: "积流代理(高匿)", Anonymity: AnonymityElite,
		Protocol: domain.TransportHTTP, Kind: ParserRoleCells, ParserVersion: "role_cells_v1",
		URL: "https://www.jiliuip.com/free/page-{page}", Pages: 10, DelayMS: 2000,
		State: SiteActive, Note: "role-cell layout"},
	{ID: "qiyunip-elite", Name: "齐云代理(高匿)", Anonymity: AnonymityElite,
		Protocol: domain.TransportHTTP, Kind: ParserTableTH, ParserVersion: "table_th_v1",
		URL: "https://www.qiyunip.com/freeProxy/{page}.html", Pages: 6, DelayMS: 2000,
		State: SiteActive, Note: "TH table; 403 is an access outcome, not an empty list"},
	{ID: "ip89", Name: "89免费代理", Anonymity: AnonymityUnknown,
		Protocol: domain.TransportHTTP, Kind: ParserText, ParserVersion: "text_v1",
		URL: "http://api.89ip.cn/tqdl.html?api=1&num=3000&port=&address=&isp=", Pages: 1, DelayMS: 1000,
		State: SiteActive, Note: "original plain-text API"},
	{ID: "ip89-html", Name: "89免费代理(HTML)", Anonymity: AnonymityUnknown,
		Protocol: domain.TransportHTTP, Kind: ParserIP89HTML, ParserVersion: "ip89_html_v1",
		URL: "http://www.89ip.cn/", Pages: 1, DelayMS: 1000,
		State: SiteActive, Note: "distinct HTML fallback endpoint"},
	{ID: "lumiproxy-transparent", Name: "LumiProxy(透明)", Anonymity: AnonymityTransparent,
		Protocol: domain.TransportHTTP, Kind: ParserJSONList, ParserVersion: "json_list_v1",
		URL: "https://api.lumiproxy.com/web_v1/free-proxy/list?anonymity=0&page_size=60&page={page}", AutoPages: true, DelayMS: 2000,
		State: SiteActive, Note: "protocol 1 maps HTTP; protocol 2 is vendor HTTPS claim"},
	{ID: "lumiproxy-anon", Name: "LumiProxy(普通)", Anonymity: AnonymityAnonymous,
		Protocol: domain.TransportHTTP, Kind: ParserJSONList, ParserVersion: "json_list_v1",
		URL: "https://api.lumiproxy.com/web_v1/free-proxy/list?anonymity=1&page_size=60&page={page}", AutoPages: true, DelayMS: 2000,
		State: SiteActive, Note: "protocol 1 maps HTTP; protocol 2 is vendor HTTPS claim"},
	{ID: "lumiproxy-elite", Name: "LumiProxy(高匿)", Anonymity: AnonymityElite,
		Protocol: domain.TransportHTTP, Kind: ParserJSONList, ParserVersion: "json_list_v1",
		URL: "https://api.lumiproxy.com/web_v1/free-proxy/list?anonymity=2&page_size=60&page={page}", AutoPages: true, DelayMS: 2000,
		State: SiteActive, Note: "protocol 1 maps HTTP; protocol 2 is vendor HTTPS claim"},
	{ID: "proxyscrape-anon", Name: "ProxyScrape(普通)", Anonymity: AnonymityUnknown,
		Protocol: domain.TransportHTTP, Kind: ParserLines, ParserVersion: "lines_v1",
		URL: "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&protocol=http&proxy_format=protocolipport&format=text&anonymity=Anonymous,Transparent&timeout=20000", Pages: 1, DelayMS: 1000,
		State: SiteActive, Note: "mixed anonymity is not relabeled per row"},
	{ID: "proxyscrape-elite", Name: "ProxyScrape(高匿)", Anonymity: AnonymityElite,
		Protocol: domain.TransportHTTP, Kind: ParserLines, ParserVersion: "lines_v1",
		URL: "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&protocol=http&proxy_format=protocolipport&format=text&anonymity=Elite&timeout=20000", Pages: 1, DelayMS: 1000,
		State: SiteActive, Note: "vendor anonymity claim only"},
	{ID: "proxyscrape-http", Name: "ProxyScrape(HTTP)", Anonymity: AnonymityUnknown,
		Protocol: domain.TransportHTTP, Kind: ParserLines, ParserVersion: "lines_v1",
		URL: "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&protocol=http&proxy_format=protocolipport&format=text&timeout=20000", Pages: 1, DelayMS: 1000,
		State: SiteActive, Note: "fixed v4 HTTP template"},
	{ID: "proxyscrape-socks5", Name: "ProxyScrape(SOCKS5)", Anonymity: AnonymityUnknown,
		Protocol: domain.TransportSOCKS5, Kind: ParserLines, ParserVersion: "lines_v1",
		URL: "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&protocol=socks5&proxy_format=protocolipport&format=text&timeout=20000", Pages: 1, DelayMS: 1000,
		State: SiteActive, Note: "fixed v4 SOCKS5 template"},
	{ID: "geonode-http", Name: "Geonode(HTTP)", Anonymity: AnonymityUnknown,
		Protocol: domain.TransportHTTP, Kind: ParserGeonodeJSON, ParserVersion: "geonode_json_v1",
		URL:   "https://proxylist.geonode.com/api/proxy-list?limit=50&page={page}&sort_by=lastChecked&sort_type=desc",
		Pages: 5, DelayMS: 1000, State: SiteActive,
		Note: "bounded 5x50 sample; protocols[] expanded; lastChecked is seconds"},
	{ID: "geonode-socks5", Name: "Geonode(SOCKS5)", Anonymity: AnonymityUnknown,
		Protocol: domain.TransportSOCKS5, Kind: ParserGeonodeJSON, ParserVersion: "geonode_json_v1",
		URL:   "https://proxylist.geonode.com/api/proxy-list?limit=50&page={page}&sort_by=lastChecked&sort_type=desc",
		Pages: 5, DelayMS: 1000, State: SiteActive,
		Note: "same bounded sample, filtered to declared socks5"},
	{ID: "monosans-http", Name: "monosans(HTTP)", Anonymity: AnonymityUnknown,
		Protocol: domain.TransportHTTP, Kind: ParserLines, ParserVersion: "lines_v1",
		URL: "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt", Pages: 1, DelayMS: 1000,
		State: SiteActive, Note: "protocol file"},
	{ID: "monosans-socks5", Name: "monosans(SOCKS5)", Anonymity: AnonymityUnknown,
		Protocol: domain.TransportSOCKS5, Kind: ParserLines, ParserVersion: "lines_v1",
		URL: "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt", Pages: 1, DelayMS: 1000,
		State: SiteActive, Note: "protocol file; authentication preserved"},
	{ID: "thespeedx-http", Name: "TheSpeedX(HTTP)", Anonymity: AnonymityUnknown,
		Protocol: domain.TransportHTTP, Kind: ParserLines, ParserVersion: "lines_v1",
		URL: "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt", Pages: 1, DelayMS: 1000,
		State: SiteActive, Note: "repository renamed; legacy download URL retained"},
	{ID: "thespeedx-socks5", Name: "TheSpeedX(SOCKS5)", Anonymity: AnonymityUnknown,
		Protocol: domain.TransportSOCKS5, Kind: ParserLines, ParserVersion: "lines_v1",
		URL: "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt", Pages: 1, DelayMS: 1000,
		State: SiteActive, Note: "repository renamed; legacy download URL retained"},
	{ID: "roosterkid-https", Name: "roosterkid(HTTPS)", Anonymity: AnonymityUnknown,
		VendorProtocol: "https",
		Protocol:       domain.TransportHTTP, Kind: ParserLines, ParserVersion: "lines_v1",
		URL: "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt", Pages: 1, DelayMS: 1000,
		State: SiteActive, Note: "vendor HTTPS/CONNECT file; transport maps to HTTP"},
	{ID: "roosterkid-socks5", Name: "roosterkid(SOCKS5)", Anonymity: AnonymityUnknown,
		Protocol: domain.TransportSOCKS5, Kind: ParserLines, ParserVersion: "lines_v1",
		URL: "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5_RAW.txt", Pages: 1, DelayMS: 1000,
		State: SiteActive, Note: "protocol file"},
	{ID: "openproxylist-http", Name: "OpenProxyList(HTTP)", Anonymity: AnonymityUnknown,
		Protocol: domain.TransportHTTP, Kind: ParserLines, ParserVersion: "lines_v1",
		URL: "https://api.openproxylist.xyz/http.txt", Pages: 1, DelayMS: 1000,
		State: SiteActive, Note: "existing directory endpoint preserved"},
	{ID: "openproxylist-https", Name: "OpenProxyList(HTTPS)", Anonymity: AnonymityUnknown,
		VendorProtocol: "https",
		Protocol:       domain.TransportHTTP, Kind: ParserLines, ParserVersion: "lines_v1",
		URL: "https://api.openproxylist.xyz/https.txt", Pages: 1, DelayMS: 1000,
		State: SiteActive, Note: "existing endpoint; HTTPS/CONNECT claim"},
	{ID: "spys-http", Name: "Spys.me(HTTP)", Anonymity: AnonymityUnknown,
		Protocol: domain.TransportHTTP, Kind: ParserSpysText, ParserVersion: "spys_text_v1",
		URL: "https://spys.me/proxy.txt", Pages: 1, DelayMS: 1000,
		State: SiteActive, Note: "first column connection; metadata retained as claims"},
	{ID: "freeproxy-http", Name: "Free-Proxy-List(HTTP)", Anonymity: AnonymityUnknown,
		Protocol: domain.TransportHTTP, Kind: ParserFreeProxy, ParserVersion: "freeproxy_html_v1",
		URL: "https://free-proxy-list.net/", Pages: 1, DelayMS: 1000,
		State: SiteActive, Note: "textarea/table signature required"},
	{ID: "sslproxies-https", Name: "SSLProxies(HTTPS)", Anonymity: AnonymityUnknown,
		VendorProtocol: "https",
		Protocol:       domain.TransportHTTP, Kind: ParserFreeProxy, ParserVersion: "freeproxy_html_v1",
		URL: "https://free-proxy-list.net/ssl-proxy.html", Pages: 1, DelayMS: 1000,
		State: SiteActive, Note: "canonical SSL subpage; HTTPS/CONNECT claim"},
	{ID: "us-proxy-http", Name: "US-Proxy(HTTP)", Anonymity: AnonymityUnknown,
		Protocol: domain.TransportHTTP, Kind: ParserFreeProxy, ParserVersion: "freeproxy_html_v1",
		URL: "https://free-proxy-list.net/us-proxy.html", Pages: 1, DelayMS: 1000,
		State: SiteActive, Note: "canonical US subpage; country is a vendor claim"},
	{ID: "socks-proxy-socks5", Name: "Socks-Proxy(SOCKS5)", Anonymity: AnonymityUnknown,
		Protocol: domain.TransportSOCKS5, Kind: ParserFreeProxy, ParserVersion: "freeproxy_html_v1",
		URL: "https://free-proxy-list.net/socks-proxy.html", Pages: 1, DelayMS: 1000,
		State: SiteActive, Note: "only explicit SOCKS5 rows; ambiguous/SOCKS4 rejected"},
	{ID: "ihuan-free", Name: "小幻免费代理", Anonymity: AnonymityUnknown,
		Protocol: domain.TransportHTTP, Kind: ParserIHuanHTML, ParserVersion: "ihuan_html_v1",
		URL: "https://ip.ihuan.me/", Pages: 1, DelayMS: 1000,
		State: SiteActive, Note: "IP link adjacent port; one page only"},
	{ID: "kuaidaili-free", Name: "快代理免费", Anonymity: AnonymityUnknown,
		Protocol: domain.TransportHTTP, Kind: ParserKuaidaili, ParserVersion: "kuaidaili_html_v1",
		URL: "https://www.kuaidaili.com/free/", Pages: 1, DelayMS: 1000,
		State: SiteActive, Note: "recognized free table only; demo rows are not claims"},

	{ID: "proxy-list-download-http", Name: "Proxy-List.download(HTTP)", Anonymity: AnonymityUnknown,
		Protocol: domain.TransportHTTP, Kind: ParserLines, ParserVersion: "lines_v1",
		URL: "https://www.proxy-list.download/api/v1/get?type=http", Pages: 1, State: SitePaused,
		Note: "paused historical www endpoint; 502/429 are not retired evidence"},
	{ID: "proxy-list-download-https", Name: "Proxy-List.download(HTTPS)", Anonymity: AnonymityUnknown,
		VendorProtocol: "https",
		Protocol:       domain.TransportHTTP, Kind: ParserLines, ParserVersion: "lines_v1",
		URL: "https://www.proxy-list.download/api/v1/get?type=https", Pages: 1, State: SitePaused,
		Note: "paused historical www endpoint; HTTPS/CONNECT semantics require confirmation"},
	{ID: "proxy-list-download-socks5", Name: "Proxy-List.download(SOCKS5)", Anonymity: AnonymityUnknown,
		Protocol: domain.TransportSOCKS5, Kind: ParserLines, ParserVersion: "lines_v1",
		URL: "https://www.proxy-list.download/api/v1/get?type=socks5", Pages: 1, State: SitePaused,
		Note: "paused historical www endpoint; SOCKS4 must be rejected"},
	{ID: "spys-socks", Name: "Spys.me(SOCKS)", Anonymity: AnonymityUnknown,
		Protocol: "", Kind: ParserSpysText, ParserVersion: "spys_text_v1",
		URL: "https://spys.me/socks.txt", Pages: 1, State: SiteAwaitingProtocol,
		Note: "vendor file does not declare SOCKS4/5; never guess"},
	{ID: "spys-html", Name: "Spys.one(HTML)", Anonymity: AnonymityUnknown,
		Protocol: "", Kind: ParserText, ParserVersion: "awaiting_fixture",
		URL: "https://spys.one/", Pages: 1, State: SitePaused,
		Note: "independent HTML decoder awaits a real fixture"},
	{ID: "jetkai-proxy-list", Name: "jetkai/proxy-list", Anonymity: AnonymityUnknown,
		Protocol: "", Kind: ParserLines, ParserVersion: "awaiting_fixture",
		URL: "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies.txt", Pages: 1, State: SitePaused,
		Note: "stale and protocol-ambiguous; not runnable"},
	{ID: "7yip-html", Name: "齐云旧入口", Anonymity: AnonymityUnknown,
		Protocol: "", Kind: ParserTableTH, ParserVersion: "awaiting_fixture",
		URL: "https://www.7yip.cn/", Pages: 1, State: SitePaused,
		Note: "schema unknown; qiyunip.com remains independent"},
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
	KindNetwork     ErrorKind = "network"
	KindFormat      ErrorKind = "format"
	KindAccess      ErrorKind = "access"
	KindUnavailable ErrorKind = "unavailable"
)

type Error struct {
	Kind       ErrorKind
	Code       string
	Status     int
	RetryAfter time.Duration
	Detail     string
}

func (e *Error) Error() string { return string(e.Kind) + ": " + e.Detail }

type RowError struct {
	Line  int    `json:"line"`
	Input string `json:"input"`
	Error string `json:"error"`
}

type Entry struct {
	Conn              domain.Connection
	VendorProtocol    string
	Claims            map[string]string
	RecordID          string
	ProviderExpiresAt *time.Time
}

type FetchOutput struct {
	Entries         []Entry
	Rejected        []RowError
	PagesDone       int
	PagesTotal      int
	Fetched         int
	Unique          int
	RejectedCount   int
	Truncated       bool
	Complete        bool
	SourceUpdatedAt *time.Time
}

// FetchSite walks the site's pages, parses each one, and returns validated,
// deduplicated endpoints. A fetch with zero successful pages fails; a partial
// fetch is returned only when at least one page yielded data, with Complete
// set to false.
func FetchSite(ctx context.Context, client *http.Client, site Site, maxPages int, allowPrivate bool) (FetchOutput, error) {
	out := FetchOutput{}
	if !site.State.Runnable() {
		return out, &Error{Kind: KindUnavailable, Code: "SOURCE_NOT_RUNNABLE", Status: 409,
			Detail: fmt.Sprintf("site %s is %s", site.ID, site.State)}
	}
	if site.Protocol != "" && site.Protocol != domain.TransportHTTP &&
		site.Protocol != domain.TransportSOCKS5 {
		return out, &Error{Kind: KindFormat, Code: "SOURCE_FORMAT_ERROR", Status: 502,
			Detail: "site protocol has no supported transport mapping"}
	}
	if client == nil {
		client = sourcehttp.NewHTTPClient(sourcehttp.Options{
			AllowPrivate: allowPrivate,
			MaxBytes:     MaxPageBytes,
		})
	}
	pages := site.Pages
	if site.AutoPages || pages <= 0 {
		pages = 1
	}
	complete := site.Kind != ParserGeonodeJSON
	if !complete {
		// Geonode is a bounded rolling window, not a full vendor snapshot.
		out.Truncated = true
	}
	if maxPages > 0 && maxPages < pages {
		pages = maxPages
		complete = false
		out.Truncated = true
	}
	if pages > MaxPages {
		pages = MaxPages
		complete = false
		out.Truncated = true
	}
	delay := time.Duration(site.DelayMS) * time.Millisecond
	if delay > maxPageDelay {
		delay = maxPageDelay
	}

	fetchCtx, cancel := context.WithTimeout(ctx, fetchBudget)
	defer cancel()
	type connectionKey struct {
		transport domain.Transport
		host      string
		port      int
		username  string
		password  string
	}
	seen := map[connectionKey]int{}
	emptyPages := 0
	successfulPages := 0
	failedPages := 0
	var lastErr error
pageLoop:
	for page := 1; page <= pages; page++ {
		if err := fetchCtx.Err(); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return out, &Error{Kind: KindNetwork, Code: "SOURCE_TIMEOUT", Status: 504,
					Detail: "site fetch exceeded its total deadline"}
			}
			return out, &Error{Kind: KindNetwork, Code: "SOURCE_CANCELLED", Status: 503,
				Detail: "site fetch was cancelled"}
		}
		if out.Fetched >= DefaultMaxRecords {
			out.Truncated = true
			complete = false
			break
		}
		body, err := fetchPage(fetchCtx, client, site, page)
		if err != nil {
			var ferr *Error
			if fetchCtx.Err() != nil || errors.As(err, &ferr) && (ferr.Kind == KindAccess || ferr.Code == "SOURCE_TIMEOUT" || ferr.Code == "REQUEST_CANCELED" || ferr.Code == "SOURCE_CANCELLED") {
				return FetchOutput{}, err
			}
			lastErr = err
			emptyPages++
			failedPages++
		} else {
			remaining := DefaultMaxRecords - out.Fetched
			parsed, perr := ParseBody(ParseOptions{
				Protocol: site.Protocol, AllowPrivate: allowPrivate,
				MaxRecords: remaining, MaxErrors: DefaultMaxErrors,
			}, site.Kind, body)
			if perr != nil {
				lastErr = siteParseError(page, site.Kind, perr)
				var ferr *Error
				if errors.As(perr, &ferr) && ferr.Kind == KindAccess {
					return FetchOutput{}, lastErr
				}
				emptyPages++
				failedPages++
			} else {
				successfulPages++
				out.RejectedCount += parsed.RejectedCount
				appendRejectedDetails(&out, parsed.Rejected)
				pageAccepted := parsed.Fetched
				if pageAccepted < len(parsed.Entries) {
					pageAccepted = len(parsed.Entries)
				}
				policyRejected := 0
				for i, e := range parsed.Entries {
					if err := validateConn(e.Conn, allowPrivate); err != nil {
						policyRejected++
						out.RejectedCount++
						appendRejectedDetails(&out, []RowError{{
							Line:  page*MaxRecordsLineStride + i + 1,
							Input: redactInput(e.Conn.Host + ":" + strconv.Itoa(e.Conn.Port)),
							Error: err.Error(),
						}})
						continue
					}
					e.Conn.Host = domain.CanonicalHost(e.Conn.Host)
					if site.VendorProtocol == "https" {
						e.VendorProtocol = "https"
						if e.Claims == nil {
							e.Claims = map[string]string{}
						}
						e.Claims["vendor_protocol"] = "https"
						e.Claims["http_connect"] = "true"
					}
					key := connectionKey{
						transport: e.Conn.Transport,
						host:      e.Conn.Host,
						port:      e.Conn.Port,
						username:  e.Conn.Username,
						password:  e.Conn.Password,
					}
					if index, exists := seen[key]; exists {
						mergeEntryClaims(&out.Entries[index], e)
						continue
					}
					if len(out.Entries) >= DefaultMaxRecords {
						out.Truncated = true
						complete = false
						break
					}
					seen[key] = len(out.Entries)
					out.Entries = append(out.Entries, e)
				}
				if pageAccepted > policyRejected {
					pageAccepted -= policyRejected
				} else {
					pageAccepted = 0
				}
				if remaining < pageAccepted {
					pageAccepted = remaining
					parsed.Truncated = true
				}
				out.Fetched += pageAccepted
				if parsed.Truncated {
					out.Truncated = true
					complete = false
				}
				if !parsed.Complete {
					complete = false
				}
				if parsed.SourceUpdatedAt != nil &&
					(out.SourceUpdatedAt == nil || parsed.SourceUpdatedAt.After(*out.SourceUpdatedAt)) {
					updated := parsed.SourceUpdatedAt.UTC()
					out.SourceUpdatedAt = &updated
				}
				out.PagesDone++
				if len(parsed.Entries) == 0 {
					emptyPages++
				} else {
					emptyPages = 0
				}
				if site.AutoPages && page == 1 {
					if auto := autoPageCount(body); auto > 0 {
						capPages := MaxPages
						if maxPages > 0 && maxPages < capPages {
							capPages = maxPages
						}
						if auto > capPages {
							pages = capPages
							complete = false
							out.Truncated = true
						} else {
							pages = auto
						}
					} else {
						complete = false
					}
				}
			}
		}
		out.PagesTotal = pages
		if emptyPages >= emptyPageStop {
			if page < pages {
				complete = false
				out.Truncated = true
			}
			break
		}
		if out.Fetched >= DefaultMaxRecords && page < pages {
			out.Truncated = true
			complete = false
			break pageLoop
		}
		if page < pages && delay > 0 {
			select {
			case <-fetchCtx.Done():
			case <-time.After(delay):
			}
		}
	}
	out.Unique = len(out.Entries)
	out.Complete = complete && failedPages == 0 && out.PagesDone == out.PagesTotal
	if out.PagesDone == 0 {
		if lastErr == nil {
			lastErr = &Error{Kind: KindFormat, Code: "SOURCE_FORMAT_ERROR", Status: 502,
				Detail: "no usable data on any page"}
		}
		return FetchOutput{}, lastErr
	}
	if len(out.Entries) == 0 && failedPages > 0 {
		if lastErr == nil {
			lastErr = &Error{Kind: KindUnavailable, Code: "SOURCE_UPSTREAM_FAILED", Status: 502,
				Detail: "pagination failed before any usable page"}
		}
		return out, lastErr
	}
	return out, nil
}

func siteParseError(page int, kind ParserKind, err error) *Error {
	var parseErr *Error
	if errors.As(err, &parseErr) && parseErr.Kind == KindAccess {
		code := parseErr.Code
		if code == "" {
			code = "SOURCE_ACCESS_BLOCKED"
		}
		return &Error{Kind: KindAccess, Code: code, Status: 502, RetryAfter: parseErr.RetryAfter,
			Detail: fmt.Sprintf("page %d returned an access-blocked %s response", page, kind)}
	}
	return &Error{Kind: KindFormat, Code: "SOURCE_FORMAT_ERROR", Status: 502,
		Detail: fmt.Sprintf("page %d response did not match parser %s", page, kind)}
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
		return nil, &Error{Kind: KindFormat, Code: "SOURCE_INVALID_URL", Status: 400,
			Detail: fmt.Sprintf("page %d request URL is invalid", page)}
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "text/html,application/json,*/*")
	pageCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	resp, err := client.Do(req.WithContext(pageCtx))
	if err != nil {
		return nil, siteRequestError(page, pageCtx, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, siteStatusError(page, resp.StatusCode, resp.Header.Get("Retry-After"))
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, MaxPageBytes+1))
	if err != nil {
		return nil, siteRequestError(page, pageCtx, err)
	}
	if len(body) > MaxPageBytes {
		return nil, &Error{Kind: KindFormat, Code: "SOURCE_RESPONSE_TOO_LARGE", Status: 502,
			Detail: fmt.Sprintf("page %d exceeds %d bytes", page, MaxPageBytes)}
	}
	if envelopeErr := responseEnvelopeError(body); envelopeErr != nil && envelopeErr.Kind == KindAccess {
		envelopeErr.RetryAfter = parseRetryAfter(resp.Header.Get("Retry-After"))
		return nil, envelopeErr
	}
	if looksLikeRejectedPage(body) {
		return nil, &Error{Kind: KindAccess, Code: "SOURCE_ACCESS_BLOCKED", Status: 502, RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
			Detail: fmt.Sprintf("page %d returned an HTML challenge or access-denied response", page)}
	}
	return body, nil
}

const MaxRecordsLineStride = 10000

func appendRejectedDetails(out *FetchOutput, rows []RowError) {
	remaining := DefaultMaxErrors - len(out.Rejected)
	if remaining <= 0 {
		return
	}
	if len(rows) > remaining {
		rows = rows[:remaining]
	}
	out.Rejected = append(out.Rejected, rows...)
}

func siteRequestError(page int, ctx context.Context, err error) *Error {
	var sourceErr *sourcehttp.Error
	if errors.As(err, &sourceErr) {
		kind := KindNetwork
		if sourceErr.Code == "SOURCE_AUTH_FAILED" || sourceErr.Code == "SOURCE_RATE_LIMITED" || sourceErr.Code == "SOURCE_ACCESS_BLOCKED" {
			kind = KindAccess
		}
		return &Error{
			Kind: kind, Code: sourceErr.Code, Status: sourceErr.Status,
			RetryAfter: sourceErr.RetryAfter,
			Detail:     sourceErr.Detail,
		}
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return &Error{Kind: KindNetwork, Code: "SOURCE_TIMEOUT", Status: 504,
			Detail: fmt.Sprintf("page %d request timed out", page)}
	}
	if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
		return &Error{Kind: KindNetwork, Code: "SOURCE_CANCELLED", Status: 503,
			Detail: fmt.Sprintf("page %d request was cancelled", page)}
	}
	return &Error{Kind: KindNetwork, Code: "SOURCE_NETWORK_ERROR", Status: 503,
		Detail: fmt.Sprintf("page %d request failed", page)}
}

func siteStatusError(page, status int, retryAfter string) *Error {
	err := &Error{
		Kind: KindNetwork, Status: status,
		Detail:     fmt.Sprintf("page %d returned HTTP %d", page, status),
		RetryAfter: parseRetryAfter(retryAfter),
	}
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		err.Kind, err.Code, err.Status = KindAccess, "SOURCE_AUTH_FAILED", 502
	case status == http.StatusTooManyRequests:
		err.Kind, err.Code = KindAccess, "SOURCE_RATE_LIMITED"
	case status >= 500:
		err.Code, err.Status = "SOURCE_UPSTREAM_FAILED", 502
	default:
		err.Kind, err.Code, err.Status = KindFormat, "SOURCE_FORMAT_ERROR", 502
	}
	return err
}

func parseRetryAfter(value string) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil && seconds > 0 {
		return time.Duration(min(seconds, 86400)) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil {
		if wait := time.Until(at); wait > 0 {
			return minDuration(wait, 24*time.Hour)
		}
	}
	return 0
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func looksLikeRejectedPage(body []byte) bool {
	blocked, _ := blockedPageReason(body)
	return blocked
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
	if conn.Port < 1 || conn.Port > 65535 || conn.Transport != domain.TransportHTTP && conn.Transport != domain.TransportSOCKS5 {
		return fmt.Errorf("invalid proxy connection")
	}
	_, err := custom.NormalizeHost(conn.Host, allowPrivate)
	return err
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
