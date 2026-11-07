package application

import (
	"encoding/json"

	"proxypool/internal/domain"
	"proxypool/internal/integrations/sites"
)

// ChannelVariant chooses an endpoint/protocol/product configuration within
// one supplier. It is not another channel and performs no fetch on selection.
type ChannelVariant struct {
	ID            string             `json:"id"`
	Name          string             `json:"name"`
	Transports    []domain.Transport `json:"transports"`
	Config        *SourceConfig      `json:"config"`
	ParserVersion string             `json:"parser_version,omitempty"`
}

type ChannelTemplate struct {
	ID             string             `json:"id"`
	Brand          string             `json:"brand"`
	Name           string             `json:"name"`
	AdapterType    string             `json:"adapter_type"`
	ParserType     string             `json:"parser_type"`
	State          string             `json:"state"`
	Reason         string             `json:"reason"`
	Transports     []domain.Transport `json:"transports"`
	Config         *SourceConfig      `json:"config"`
	Variants       []ChannelVariant   `json:"variants"`
	RequiresSecret bool               `json:"requires_secret"`
	Runnable       bool               `json:"runnable"`
}

func (s *IngestService) ChannelTemplates() []ChannelTemplate {
	out := make([]ChannelTemplate, 0, len(sites.Channels())+2)
	for _, channel := range sites.Channels() {
		template := ChannelTemplate{ID: channel.ID, Brand: channel.Name, Name: channel.Name, AdapterType: AdapterSite, ParserType: ParserSite, State: "active", Reason: channel.Reason, Runnable: true, Transports: []domain.Transport{}, Variants: []ChannelVariant{}}
		for _, id := range channel.SiteIDs {
			site, ok := sites.ByID(id)
			if !ok || !site.State.Runnable() {
				continue
			}
			cfg := &SourceConfig{Site: &SiteConfig{SiteID: id, MaxPages: 1}, MaxPoolLeases: 1, Refresh: &RefreshConfig{Mode: "demand", MaxCandidates: 1000}}
			template.Variants = append(template.Variants, ChannelVariant{ID: id, Name: variantName(site), Transports: []domain.Transport{site.Protocol}, Config: cfg, ParserVersion: site.ParserVersion})
			found := false
			for _, transport := range template.Transports {
				if transport == site.Protocol {
					found = true
				}
			}
			if !found {
				template.Transports = append(template.Transports, site.Protocol)
			}
		}
		if len(template.Variants) == 0 {
			continue
		}
		template.Config = cloneTemplateConfig(template.Variants[0].Config)
		out = append(out, template)
	}
	scdnTemplate := ChannelTemplate{ID: "scdn", Brand: "SCDN", Name: "SCDN", AdapterType: AdapterProvider, ParserType: ParserJSONMapping, State: "active", Reason: "免费 API 代理列表；与其他网络渠道统一缓存、黑名单和并发预热。", Runnable: true, Transports: []domain.Transport{domain.TransportHTTP, domain.TransportSOCKS5}, Variants: []ChannelVariant{}}
	accountTemplate := ChannelTemplate{ID: "webshare", Brand: "Webshare", Name: "Webshare", AdapterType: AdapterAccount, ParserType: ParserWebshare, State: "awaiting_credentials", Reason: "只读已分配 direct 列表，契约明确、无需购买；需管理员配置私有项目与账号 Key，不保证每日免费。", Runnable: true, RequiresSecret: true, Transports: []domain.Transport{domain.TransportHTTP, domain.TransportSOCKS5}, Variants: []ChannelVariant{}}
	for _, protocol := range []domain.Transport{domain.TransportHTTP, domain.TransportSOCKS5} {
		cfg := &SourceConfig{Provider: &ProviderConfig{VendorID: "scdn", Protocol: string(protocol), FetchCount: 20}}
		scdnTemplate.Variants = append(scdnTemplate.Variants, ChannelVariant{ID: string(protocol), Name: string(protocol) + " · 代理列表", Transports: []domain.Transport{protocol}, Config: cfg})
		cfg = &SourceConfig{Account: &AccountConfig{VendorID: "webshare", Protocol: string(protocol), Mode: "direct", PageSize: 100, MaxPages: 10}, Access: &SourceAccessConfig{ProjectIDs: []string{}}, Refresh: &RefreshConfig{Mode: "demand", CacheTTLMS: 1800000, MaxCandidates: 1000}, MaxPoolLeases: 1}
		accountTemplate.Variants = append(accountTemplate.Variants, ChannelVariant{ID: string(protocol), Name: string(protocol) + " · 已分配 direct 列表", Transports: []domain.Transport{protocol}, Config: cfg})
	}
	scdnTemplate.Config = cloneTemplateConfig(scdnTemplate.Variants[0].Config)
	accountTemplate.Config = cloneTemplateConfig(accountTemplate.Variants[0].Config)
	return append(out, scdnTemplate, accountTemplate)
}

func cloneTemplateConfig(cfg *SourceConfig) *SourceConfig {
	raw, _ := json.Marshal(cfg)
	var copy SourceConfig
	_ = json.Unmarshal(raw, &copy)
	return &copy
}

func variantName(site sites.Site) string {
	switch site.ID {
	case "openproxylist-http":
		return "HTTP · API 域名"
	case "openproxylist-https":
		return "HTTP CONNECT 声明 · API 域名"
	case "roosterkid-https":
		return "HTTP CONNECT 声明 · 同名仓库（显式选择）"
	case "roosterkid-socks5":
		return "SOCKS5 · 同名仓库（显式选择）"
	case "freeproxy-http":
		return "HTTP · 主列表"
	case "sslproxies-https":
		return "HTTP CONNECT 声明 · SSL 筛选"
	case "us-proxy-http":
		return "HTTP · 美国筛选（厂商声明）"
	case "socks-proxy-socks5":
		return "SOCKS5 · 明确版本记录"
	case "ip89":
		return "HTTP · API"
	case "ip89-html":
		return "HTTP · HTML 备用"
	default:
		return site.Name
	}
}

// SourceChannelIdentity is presentation metadata only. Source IDs, account
// scopes, cache versions, permissions, leases and CD are never merged.
func SourceChannelIdentity(src domain.Source) (string, string) {
	var cfg SourceConfig
	if json.Unmarshal([]byte(src.ConfigJSON), &cfg) != nil {
		return src.ID, src.ID
	}
	if cfg.Site != nil {
		if channel, ok := sites.ChannelForSite(cfg.Site.SiteID); ok {
			return channel.ID, channel.Name
		}
	}
	if cfg.Account != nil && cfg.Account.VendorID == "webshare" {
		return "webshare", "Webshare"
	}
	if cfg.Provider != nil && (cfg.Provider.VendorID == "scdn" || cfg.Provider.VendorID == "") {
		return "scdn", "SCDN"
	}
	return src.ID, src.ID
}

func admittedSiteConfig(cfg SourceConfig) *domain.Error {
	if cfg.Site != nil {
		if _, ok := sites.ByID(cfg.Site.SiteID); !ok {
			return domain.InvalidRequest("unknown site_id %q; see admitted channel templates", cfg.Site.SiteID)
		}
	}
	if cfg.Site != nil && !sites.SiteWorthBuilding(cfg.Site.SiteID) {
		return domain.E("CHANNEL_NOT_ADMITTED", 409, "site %q is diagnostic/legacy only and is not admitted as a new channel; see the channel value review", cfg.Site.SiteID)
	}
	return nil
}
