package api

import (
	"encoding/json"
	"net/http"
	"time"

	"proxypool/internal/domain"
)

func (s *Server) handleListKeys(w http.ResponseWriter, r *http.Request) {
	keys, err := s.keys.List(r.Context())
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if keys == nil {
		keys = []domain.APIKey{}
	}
	writeJSON(w, http.StatusOK, keys)
}

func (s *Server) handleCreateKey(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 8192)
	var body struct {
		Name             string `json:"name"`
		DefaultCDSeconds int64  `json:"default_cd_seconds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, domain.InvalidRequest("invalid JSON body: %v", err))
		return
	}
	cd, derr := s.keyCooldown(body.DefaultCDSeconds)
	if derr != nil {
		writeError(w, derr)
		return
	}
	key, token, err := s.keys.Create(r.Context(), body.Name, cd)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"key": key, "token": token})
}

func (s *Server) handleUpdateKey(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 8192)
	var body struct {
		Name             *string `json:"name"`
		Enabled          *bool   `json:"enabled"`
		DefaultCDSeconds *int64  `json:"default_cd_seconds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, domain.InvalidRequest("invalid JSON body: %v", err))
		return
	}
	var cd *time.Duration
	if body.DefaultCDSeconds != nil {
		v, derr := s.keyCooldown(*body.DefaultCDSeconds)
		if derr != nil {
			writeError(w, derr)
			return
		}
		cd = &v
	}
	if err := s.keys.Update(r.Context(), r.PathValue("id"), body.Name, body.Enabled, cd); err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "updated"})
}

func (s *Server) handleDeleteKey(w http.ResponseWriter, r *http.Request) {
	if err := s.keys.Delete(r.Context(), r.PathValue("id")); err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

func (s *Server) keyCooldown(seconds int64) (time.Duration, *domain.Error) {
	if seconds < 0 {
		return 0, domain.InvalidRequest("default_cd_seconds must not be negative")
	}
	ms := seconds * 1000
	if ms > s.cfg.MaxDownstreamCooldownMS {
		return 0, domain.InvalidRequest("default_cd_seconds exceeds the maximum of %d seconds",
			s.cfg.MaxDownstreamCooldownMS/1000)
	}
	return time.Duration(ms) * time.Millisecond, nil
}
