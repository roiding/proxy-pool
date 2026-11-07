package api

import "net/http"

func (s *Server) handleStartSourceRefresh(w http.ResponseWriter, r *http.Request) {
	job, derr := s.refresh.StartJob(r.Context(), r.PathValue("id"))
	if derr != nil {
		writeError(w, derr)
		return
	}
	writeJSON(w, http.StatusAccepted, job)
}

func (s *Server) handleSourceCache(w http.ResponseWriter, r *http.Request) {
	src, err := s.store.GetSource(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, s.ingest.CacheViewForSource(src))
}

func (s *Server) handleSourceFetchState(w http.ResponseWriter, r *http.Request) {
	if _, err := s.store.GetSource(r.Context(), r.PathValue("id")); err != nil {
		writeStoreError(w, err)
		return
	}
	state, err := s.store.SourceFetchState(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, state)
}

func (s *Server) handleChannelTemplates(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"templates": s.ingest.ChannelTemplates()})
}
