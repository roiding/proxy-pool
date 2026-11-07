package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"regexp"
	"strings"

	"proxypool/internal/application"
	"proxypool/internal/domain"
)

var sourceIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

// sourceView keeps the wire contract explicit instead of leaking struct
// field names; config is only ever returned to admin identities.
type sourceView struct {
	SourceID      string          `json:"source_id"`
	AdapterType   string          `json:"adapter_type"`
	ParserType    string          `json:"parser_type"`
	Enabled       bool            `json:"enabled"`
	ConfigVersion int64           `json:"config_version"`
	Config        json.RawMessage `json:"config"`
}

func newSourceView(src domain.Source) sourceView {
	config := json.RawMessage(src.ConfigJSON)
	if len(config) == 0 {
		config = json.RawMessage("{}")
	}
	return sourceView{
		SourceID:      src.ID,
		AdapterType:   src.AdapterType,
		ParserType:    src.ParserType,
		Enabled:       src.Enabled,
		ConfigVersion: src.ConfigVersion,
		Config:        config,
	}
}

func (s *Server) handleListSourcesV1(w http.ResponseWriter, r *http.Request) {
	s.handleSourcesOverview(w, r)
}

func (s *Server) handleCreateSourceV1(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SourceID    string          `json:"source_id"`
		AdapterType string          `json:"adapter_type"`
		ParserType  string          `json:"parser_type"`
		Enabled     *bool           `json:"enabled"`
		Config      json.RawMessage `json:"config"`
	}
	if derr := s.decodeAdminBody(w, r, &body); derr != nil {
		writeError(w, derr)
		return
	}
	id := strings.TrimSpace(body.SourceID)
	if !sourceIDPattern.MatchString(id) {
		writeError(w, domain.InvalidRequest("source_id must match [a-z0-9][a-z0-9_-]{0,63}"))
		return
	}
	// normalizeAdapter validates parser_type per adapter; the stored parser
	// is derived from the validated config by the ingest service.
	adapter, _, derr := normalizeAdapter(body.AdapterType, body.ParserType)
	if derr != nil {
		writeError(w, derr)
		return
	}
	cfg, derr := decodeSourceConfig(body.Config)
	if derr != nil {
		writeError(w, derr)
		return
	}
	// The adapter and the config must agree: buildSource infers the adapter
	// from the config, so a mismatch here would silently store a channel of
	// a different kind than the one the operator selected.
	if inferred := inferAdapter(cfg); inferred != adapter {
		writeError(w, domain.InvalidRequest(
			"config matches adapter %q, not %q", inferred, adapter))
		return
	}
	if err := validateAdapterConfigShape(adapter, cfg); err != nil {
		writeError(w, err)
		return
	}
	enabled := true
	if body.Enabled != nil {
		enabled = *body.Enabled
	}
	// CreateSource validates the config against the adapter (provider
	// protocol, catalogued site, static URL) and inserts inside one
	// transaction: a concurrent create wins, this request reports the
	// conflict instead of overwriting.
	saved, derr := s.ingest.CreateSource(r.Context(), id, cfg, enabled)
	if derr != nil {
		writeError(w, derr)
		return
	}
	writeJSON(w, http.StatusCreated, newSourceView(saved))
}

func (s *Server) handleGetSourceV1(w http.ResponseWriter, r *http.Request) {
	src, err := s.store.GetSource(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, newSourceView(src))
}

// handlePatchSourceV1 saves configuration with optimistic version checking.
// Saving never fetches; imports and downloads are explicit operations.
func (s *Server) handlePatchSourceV1(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var body struct {
		ConfigVersion int64           `json:"config_version"`
		Enabled       *bool           `json:"enabled"`
		Config        json.RawMessage `json:"config"`
	}
	if derr := s.decodeAdminBody(w, r, &body); derr != nil {
		writeError(w, derr)
		return
	}
	// Resolve the effective definition first, then let the ingest service
	// compare-and-swap it against the expected version in one transaction.
	src, err := s.store.GetSource(r.Context(), id)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	effective := src.ConfigJSON
	if body.Config != nil {
		// Reject unknown keys in the submitted config before anything else.
		if _, derr := decodeSourceConfig(body.Config); derr != nil {
			writeError(w, derr)
			return
		}
		effective = string(body.Config)
	}
	enabled := src.Enabled
	if body.Enabled != nil {
		enabled = *body.Enabled
	}
	cfg, derr := decodeSourceConfig(json.RawMessage(effective))
	if derr != nil {
		writeError(w, derr)
		return
	}
	saved, derr := s.ingest.UpdateSource(r.Context(), id, cfg, enabled, body.ConfigVersion)
	if derr != nil {
		writeError(w, derr)
		return
	}
	writeJSON(w, http.StatusOK, newSourceView(saved))
}

func (s *Server) handleDeleteSourceV1(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	removed, derr := s.ingest.DeleteSource(r.Context(), id)
	if derr != nil {
		writeError(w, derr)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "deleted", "source_id": id, "removed_offers": removed,
	})
}

func normalizeAdapter(adapterType, parserType string) (string, string, *domain.Error) {
	switch adapterType {
	case "", application.AdapterCustom:
		if parserType != "" && parserType != application.ParserLines {
			return "", "", domain.InvalidRequest("parser_type %q is not supported for custom; use lines_v1", parserType)
		}
		return application.AdapterCustom, application.ParserLines, nil
	case application.AdapterProvider:
		if parserType != "" && parserType != application.ParserJSONMapping {
			return "", "", domain.InvalidRequest("parser_type %q is not supported for provider_api; use json_mapping_v1", parserType)
		}
		return application.AdapterProvider, application.ParserJSONMapping, nil
	case application.AdapterSite:
		if parserType != "" && parserType != application.ParserSite {
			return "", "", domain.InvalidRequest("parser_type %q is not supported for site_fetch; use site_v1", parserType)
		}
		return application.AdapterSite, application.ParserSite, nil
	case application.AdapterStatic:
		if parserType != "" && parserType != application.ParserLines {
			return "", "", domain.InvalidRequest("parser_type %q is not supported for static_url; use lines_v1", parserType)
		}
		return application.AdapterStatic, application.ParserLines, nil
	case application.AdapterAccount:
		if parserType != "" && parserType != application.ParserWebshare {
			return "", "", domain.InvalidRequest("parser_type %q is not supported for account_list; use webshare_list_v1", parserType)
		}
		return application.AdapterAccount, application.ParserWebshare, nil
	default:
		return "", "", domain.InvalidRequest("unknown adapter_type %q", adapterType)
	}
}

// decodeSourceConfig strictly maps the wire config onto the typed
// SourceConfig: unknown fields are rejected so a typo can never silently
// store a channel the adapters cannot interpret.
func decodeSourceConfig(raw json.RawMessage) (application.SourceConfig, *domain.Error) {
	if len(raw) == 0 {
		return application.SourceConfig{}, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var cfg application.SourceConfig
	if err := dec.Decode(&cfg); err != nil {
		return application.SourceConfig{}, domain.InvalidRequest(
			"config does not match the adapter schema: %v", err)
	}
	return cfg, nil
}

// inferAdapter reports which adapter a config would be stored as, mirroring
// the service's buildSource inference.
func inferAdapter(cfg application.SourceConfig) string {
	switch {
	case cfg.Provider != nil:
		return application.AdapterProvider
	case cfg.Site != nil:
		return application.AdapterSite
	case cfg.Static != nil:
		return application.AdapterStatic
	case cfg.Account != nil:
		return application.AdapterAccount
	default:
		return application.AdapterCustom
	}
}

func validateAdapterConfigShape(adapter string, cfg application.SourceConfig) *domain.Error {
	shapes := 0
	if cfg.Provider != nil {
		shapes++
	}
	if cfg.Site != nil {
		shapes++
	}
	if cfg.Static != nil {
		shapes++
	}
	if cfg.Account != nil {
		shapes++
	}
	if shapes > 1 {
		return domain.InvalidRequest("config must contain exactly one of provider, site, static, or account")
	}
	if adapter != application.AdapterCustom && shapes != 1 {
		return domain.InvalidRequest("adapter %s requires its nested config object", adapter)
	}
	return nil
}
