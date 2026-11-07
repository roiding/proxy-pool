package sites

// Channel is a curated supplier family, not a protocol, anonymity filter,
// repository mirror, or deployed source instance. Site IDs remain low-level
// adapter/diagnostic identifiers for compatibility.
type Channel struct {
	ID      string   `json:"id"`
	Name    string   `json:"name"`
	Reason  string   `json:"reason"`
	SiteIDs []string `json:"site_ids"`
}

// Channels contains only families worth maintaining under the current
// project contract. This is an engineering admission decision, not a vendor
// liveness claim; each deployment must still accept its selected variant.
func Channels() []Channel {
	return []Channel{
		{"ip3366", "云代理", "保留既有接口与 GBK 表格适配；匿名类别是配置，不增加渠道数。", []string{"ip3366-normal", "ip3366-elite"}},
		{"jiliuip", "积流代理", "保留既有 role-cell 接口兼容；当前可用性仍需逐渠道验收。", []string{"jiliuip-elite"}},
		{"qiyunip", "齐云代理", "保留 qiyunip.com 既有接口；不将 7yip 旧域名视为可用备用。", []string{"qiyunip-elite"}},
		{"ip89", "89免费代理", "保留既有 API；明确 HTML 备用形状，同品牌只维护一个渠道。", []string{"ip89", "ip89-html"}},
		{"lumiproxy", "LumiProxy", "既有分页 JSON 契约可复用；匿名筛选归入配置。", []string{"lumiproxy-elite", "lumiproxy-anon", "lumiproxy-transparent"}},
		{"proxyscrape", "ProxyScrape", "固定协议列表契约、低维护成本；HTTP/SOCKS5 和匿名筛选同属一个渠道。", []string{"proxyscrape-http", "proxyscrape-socks5", "proxyscrape-elite", "proxyscrape-anon"}},
		{"openproxylist", "OpenProxyList", "保留用户指定域名入口；同名仓库作为显式候选路径，非独立供给或自动回退，不假定内容相同。", []string{"openproxylist-http", "openproxylist-https", "roosterkid-https", "roosterkid-socks5"}},
		{"geonode", "Geonode", "结构化协议/更新时间与有界滚动窗口有诊断价值；不是完整库存或本池 PASS。", []string{"geonode-http", "geonode-socks5"}},
		{"monosans", "monosans", "可复用规范行解析，含协议文件与认证行；维护成本低，增量供给待部署抽样。", []string{"monosans-http", "monosans-socks5"}},
		{"spys", "Spys", "HTTP 固定文本与明确厂商标记可维护；不另建 Spys.one HTML 或猜 SOCKS 版本。", []string{"spys-http"}},
		{"freeproxy", "Free-Proxy-List", "主站、SSL/US/SOCKS 子页共享产品与解析器；协议与地区是配置，不重复计渠道。", []string{"freeproxy-http", "sslproxies-https", "us-proxy-http", "socks-proxy-socks5"}},
		{"ihuan", "小幻免费代理", "既有页面线索与清晰 IP 链接/相邻端口可做低成本补充，需真实页面验收。", []string{"ihuan-free"}},
	}
}

func ChannelForSite(id string) (Channel, bool) {
	for _, channel := range Channels() {
		for _, siteID := range channel.SiteIDs {
			if siteID == id {
				return channel, true
			}
		}
	}
	return Channel{}, false
}

func SiteWorthBuilding(id string) bool { _, ok := ChannelForSite(id); return ok }

// DefaultSites uses one primary variant per admitted supplier. Explicit
// -site diagnostics may select another variant without multiplying brands.
func DefaultSites() []Site {
	out := make([]Site, 0, len(Channels()))
	for _, channel := range Channels() {
		if len(channel.SiteIDs) > 0 {
			if site, ok := ByID(channel.SiteIDs[0]); ok && site.State.Runnable() {
				out = append(out, site)
			}
		}
	}
	return out
}
