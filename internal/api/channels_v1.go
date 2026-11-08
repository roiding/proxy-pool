package api

import (
	"net/http"

	"proxypool/internal/domain"
)

func (s *Server) handleFixedChannels(w http.ResponseWriter, r *http.Request) {
	channels, derr := s.ingest.FixedChannels(r.Context())
	if derr != nil {
		writeError(w, derr)
		return
	}
	writeJSON(w, http.StatusOK, channels)
}

func (s *Server) handleFixedChannelSwitch(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Enabled *bool  `json:"enabled"`
		Version string `json:"version"`
	}
	if derr := s.decodeAdminBody(w, r, &body); derr != nil {
		writeError(w, derr)
		return
	}
	if body.Enabled == nil || body.Version == "" {
		writeError(w, domain.InvalidRequest("enabled and version are required"))
		return
	}
	if derr := s.ingest.SetFixedChannelEnabled(r.Context(), r.PathValue("id"), *body.Enabled, body.Version); derr != nil {
		writeError(w, derr)
		return
	}
	s.handleFixedChannels(w, r)
}
