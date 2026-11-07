package api

import "net/http"

func (s *Server) handleSourceSecretStatus(w http.ResponseWriter, r *http.Request) {
	status, derr := s.ingest.SourceSecretStatus(r.Context(), r.PathValue("id"))
	if derr != nil {
		writeError(w, derr)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, status)
}

func (s *Server) handleSetSourceSecret(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ConfigVersion int64  `json:"config_version"`
		Value         string `json:"value"`
	}
	if derr := s.decodeAdminBody(w, r, &body); derr != nil {
		writeError(w, derr)
		return
	}
	status, derr := s.ingest.SetSourceSecret(r.Context(), r.PathValue("id"), body.ConfigVersion, body.Value)
	if derr != nil {
		writeError(w, derr)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, status)
}

func (s *Server) handleClearSourceSecret(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ConfigVersion int64 `json:"config_version"`
	}
	if derr := s.decodeAdminBody(w, r, &body); derr != nil {
		writeError(w, derr)
		return
	}
	status, derr := s.ingest.SetSourceSecret(r.Context(), r.PathValue("id"), body.ConfigVersion, "")
	if derr != nil {
		writeError(w, derr)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, status)
}
