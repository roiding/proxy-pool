package application

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"

	"proxypool/internal/domain"
	"proxypool/internal/integrations/sites"
)

// FixedChannel exposes the supplier as the operational unit. Protocol
// endpoints are internal source definitions, never additional channels.
type FixedChannel struct {
	ID               string             `json:"channel_id"`
	Name             string             `json:"name"`
	Transports       []domain.Transport `json:"transports"`
	Enabled          bool               `json:"enabled"`
	PartiallyEnabled bool               `json:"partially_enabled"`
	RequiresSecret   bool               `json:"requires_secret"`
	Available        bool               `json:"available"`
	Version          string             `json:"version"`
	SourceIDs        []string           `json:"source_ids"`
}

// fixedChannelVariants selects one maintained path per supported protocol.
// Filters and mirrors are not extra supply. OpenProxyList uses its requested
// API domain; its repository alternative is not silently enabled.
func fixedChannelVariants(t ChannelTemplate) []ChannelVariant {
	var out []ChannelVariant
	seen := map[domain.Transport]bool{}
	for _, v := range t.Variants {
		if t.ID == "openproxylist" && v.ID != "openproxylist-http" {
			continue
		}
		for _, protocol := range v.Transports {
			if !seen[protocol] {
				out = append(out, v)
				seen[protocol] = true
				break
			}
		}
	}
	return out
}

func channelSources(all []domain.Source, id string) []domain.Source {
	var out []domain.Source
	for _, src := range all {
		channel, _ := fixedSourceIdentity(src)
		if channel == id {
			out = append(out, src)
		}
	}
	return out
}

func fixedSourceIdentity(src domain.Source) (string, string) {
	var cfg SourceConfig
	if json.Unmarshal([]byte(src.ConfigJSON), &cfg) == nil {
		if cfg.Site != nil {
			if _, ok := sites.ChannelForSite(cfg.Site.SiteID); ok {
				return SourceChannelIdentity(src)
			}
		}
		if cfg.Provider != nil && (cfg.Provider.VendorID == "" || cfg.Provider.VendorID == "scdn") ||
			cfg.Account != nil && cfg.Account.VendorID == "webshare" {
			return SourceChannelIdentity(src)
		}
	}
	// A user upload named after a supplier is still independent inventory.
	return "source:" + src.ID, src.ID
}

func channelVersion(sources []domain.Source) string {
	type item struct {
		ID      string
		Version int64
	}
	items := make([]item, 0, len(sources))
	for _, src := range sources {
		items = append(items, item{src.ID, src.ConfigVersion})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	raw, _ := json.Marshal(items)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func (s *IngestService) FixedChannels(ctx context.Context) ([]FixedChannel, *domain.Error) {
	all, derr := s.ListSources(ctx)
	if derr != nil {
		return nil, derr
	}
	out := make([]FixedChannel, 0)
	known := map[string]bool{}
	for _, template := range s.ChannelTemplates() {
		row := FixedChannel{ID: template.ID, Name: template.Name, Available: template.Runnable,
			RequiresSecret: template.RequiresSecret, Transports: []domain.Transport{}, SourceIDs: []string{}}
		for _, v := range fixedChannelVariants(template) {
			row.Transports = append(row.Transports, v.Transports...)
		}
		out = append(out, row)
		known[row.ID] = true
	}
	for _, src := range all {
		id, name := fixedSourceIdentity(src)
		if !known[id] {
			out = append(out, FixedChannel{ID: id, Name: name, Available: true,
				Transports: []domain.Transport{}, SourceIDs: []string{}})
			known[id] = true
		}
	}
	for i := range out {
		row := &out[i]
		members := channelSources(all, row.ID)
		row.Version = channelVersion(members)
		enabled := 0
		for _, src := range members {
			row.SourceIDs = append(row.SourceIDs, src.ID)
			if src.Enabled {
				enabled++
			}
			_, transports := SourceProtocolMetadata(src)
			for _, transport := range transports {
				found := false
				for _, existing := range row.Transports {
					found = found || existing == transport
				}
				if !found {
					row.Transports = append(row.Transports, transport)
				}
			}
		}
		row.Enabled = enabled > 0
		row.PartiallyEnabled = enabled > 0 && enabled < len(members)
		if row.RequiresSecret {
			row.Available = len(members) > 0
			for _, src := range members {
				if _, err := s.readAccountToken(ctx, src.ID); err != nil {
					row.Available = false
				}
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func SourceProtocolMetadata(src domain.Source) (string, []domain.Transport) {
	var cfg SourceConfig
	if json.Unmarshal([]byte(src.ConfigJSON), &cfg) != nil {
		return src.ID, []domain.Transport{}
	}
	if src.AdapterType == AdapterCustom {
		return "上传代理", []domain.Transport{domain.TransportHTTP, domain.TransportSOCKS5}
	}
	if cfg.Site != nil {
		if site, ok := sites.ByID(cfg.Site.SiteID); ok {
			return variantName(site), []domain.Transport{site.Protocol}
		}
	}
	protocol := cfg.DefaultTransport
	if cfg.Provider != nil {
		protocol = domain.Transport(cfg.Provider.Protocol)
	} else if cfg.Account != nil {
		protocol = domain.Transport(cfg.Account.Protocol)
	} else if cfg.Static != nil {
		protocol = cfg.Static.DefaultTransport
	}
	if protocol == "" {
		protocol = domain.TransportHTTP
	}
	return string(protocol), []domain.Transport{protocol}
}

// SetFixedChannelEnabled validates every member before atomically changing
// the switch. Existing IDs, private scopes, secrets, inventory and blacklists
// remain attached to their original sources.
func (s *IngestService) SetFixedChannelEnabled(ctx context.Context, id string, enabled bool, version string) *domain.Error {
	all, derr := s.ListSources(ctx)
	if derr != nil {
		return derr
	}
	members := channelSources(all, id)
	if version != channelVersion(members) {
		return domain.E("CHANNEL_VERSION_CONFLICT", 409, "channel configuration changed; reload and retry")
	}
	var template *ChannelTemplate
	for _, t := range s.ChannelTemplates() {
		if t.ID == id {
			template = &t
			break
		}
	}
	if template == nil && len(members) == 0 {
		return domain.NotFound("channel")
	}
	changes := []domain.Source{}
	covered := map[domain.Transport]bool{}
	for _, src := range members {
		var cfg SourceConfig
		if err := json.Unmarshal([]byte(src.ConfigJSON), &cfg); err != nil {
			return domain.AsError(err)
		}
		if _, err := s.buildSource(src.ID, cfg, enabled); err != nil {
			return err
		}
		if enabled && src.AdapterType == AdapterAccount {
			if _, err := s.readAccountToken(ctx, src.ID); err != nil {
				return err
			}
		}
		_, transports := SourceProtocolMetadata(src)
		for _, transport := range transports {
			covered[transport] = true
		}
		if src.Enabled != enabled {
			src.Enabled = enabled
			changes = append(changes, src)
		}
	}
	if enabled && template != nil && !template.RequiresSecret {
		for _, v := range fixedChannelVariants(*template) {
			if covered[v.Transports[0]] {
				continue
			}
			cfg := cloneTemplateConfig(v.Config)
			if len(members) > 0 {
				var original SourceConfig
				_ = json.Unmarshal([]byte(members[0].ConfigJSON), &original)
				// New protocol supply inherits the existing source-side ceiling.
				cfg.Access = original.Access
			}
			src, err := s.buildSource("builtin-"+template.ID+"-"+string(v.Transports[0]), *cfg, true)
			if err != nil {
				return err
			}
			for _, existing := range all {
				if existing.ID == src.ID {
					return domain.E("SOURCE_EXISTS", 409, "internal channel source ID is already in use")
				}
			}
			changes = append(changes, src)
		}
	} else if enabled && template != nil && template.RequiresSecret && len(members) == 0 {
		return domain.E("SOURCE_SECRET_REQUIRED", 409, "account channel has not been connected")
	}
	if len(changes) == 0 {
		return nil
	}
	if err := s.store.ApplyChannelSources(ctx, all, changes, s.Now()); err != nil {
		return domain.AsError(err)
	}
	for _, src := range changes {
		s.static.invalidate(src.ID)
	}
	return nil
}

// ReconcileEnabledChannels supplies missing built-in protocols on upgrade
// and restart. Disabled suppliers stay disabled; no network I/O occurs here.
func (s *IngestService) ReconcileEnabledChannels(ctx context.Context) *domain.Error {
	channels, derr := s.FixedChannels(ctx)
	if derr != nil {
		return derr
	}
	for _, channel := range channels {
		if channel.Enabled && channel.Available && !channel.RequiresSecret {
			if err := s.SetFixedChannelEnabled(ctx, channel.ID, true, channel.Version); err != nil {
				return err
			}
		}
	}
	return nil
}
