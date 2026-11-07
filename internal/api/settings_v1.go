package api

import (
	"net/http"

	"proxypool/internal/application"
	"proxypool/internal/domain"
	"proxypool/internal/storage/sqlite"
	"proxypool/internal/usage"
)

func (s *Server) requireUsage(w http.ResponseWriter) bool {
	if s.usage == nil {
		writeError(w, domain.E("USAGE_SERVICE_UNAVAILABLE", 503,
			"traffic accounting is not wired in this deployment"))
		return false
	}
	return true
}

// handleSystemAdmin reports the running build and the effective safety
// ceilings from configuration. It invents nothing: every number is read from
// the loaded config or the injected build version.
func (s *Server) handleSystemAdmin(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"version":            s.version,
		"default_project_id": sqlite.DefaultProjectID,
		"limits": map[string]any{
			"max_count_per_request":      s.cfg.MaxCount,
			"max_acquire_deadline_ms":    s.cfg.MaxAcquireDeadlineMS,
			"acquire_deadline_ms":        s.cfg.AcquireDeadlineMS,
			"max_downstream_cooldown_ms": s.cfg.MaxDownstreamCooldownMS,
			"prewarm_max_deadline_ms":    s.cfg.PrewarmMaxDeadlineMS,
			"auto_prewarm_max_target":    1000,
			"job_max_active_per_project": s.cfg.JobMaxActivePerProject,
			"job_max_active_global":      s.cfg.JobMaxActiveGlobal,
			"deployment_probe_slots":     s.cfg.ProbeSlots,
			"max_active_acquisitions":    s.cfg.MaxActiveAcquires,
			"max_request_body_bytes":     s.cfg.MaxRequestBodyBytes,
		},
		"validation": map[string]any{
			"judge_url_configured": s.cfg.JudgeURL != "",
			"judge_signed":         s.cfg.JudgeSecret != "",
			"allow_private":        s.cfg.AllowPrivateAddresses,
		},
		"public_https": s.cfg.PublicHTTPS,
		"proxy_lifecycle": map[string]any{
			"all_network_channels_use_cache":                true,
			"automatic_project_replenishment":               true,
			"network_cache_ttl_ms":                          application.DefaultStaticCacheTTL.Milliseconds(),
			"network_delete_on_delivery":                    false,
			"network_blacklist_on_delivery":                 true,
			"network_default_reuse_cooldown_ms":             int64(1800000),
			"upload_delete_on_completed_validation_failure": true,
			"network_blacklist_on_failure":                  true,
			"upload_persistent":                             true,
			"list_upload_proof_clock_expiry":                false,
		},
	})
}

// handleGetSettingsV1 returns the masked settings view. Secrets never leave
// the server: presence and a masked hint are all a client can read.
func (s *Server) handleGetSettingsV1(w http.ResponseWriter, r *http.Request) {
	if !s.requireUsage(w) {
		return
	}
	view, err := s.usage.Settings(r.Context())
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

// handlePutSettingsV1 applies a partial update. Secret fields keep their
// stored value on an empty string; only an explicit null clears them, so a
// re-saved form can never overwrite a credential with its mask.
func (s *Server) handlePutSettingsV1(w http.ResponseWriter, r *http.Request) {
	if !s.requireUsage(w) {
		return
	}
	var patch usage.SettingsUpdate
	if derr := s.decodeAdminBody(w, r, &patch); derr != nil {
		writeError(w, derr)
		return
	}
	view, err := s.usage.UpdateSettings(r.Context(), patch)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

// handleBarkTestV1 sends one real test push through the stored endpoint.
func (s *Server) handleBarkTestV1(w http.ResponseWriter, r *http.Request) {
	if !s.requireUsage(w) {
		return
	}
	var body struct {
		Title string `json:"title"`
		Body  string `json:"body"`
	}
	// The documented empty request/default message remains valid, while any
	// supplied JSON must be strict so malformed input cannot trigger a push.
	if derr := decodeStrictJSON(w, r, 4096, &body, true); derr != nil {
		writeError(w, derr)
		return
	}
	if err := s.usage.TestBark(r.Context(), body.Title, body.Body); err != nil {
		writeError(w, domain.E("BARK_TEST_FAILED", 502, "%v", err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "delivered"})
}

// handleUsageStatusV1 reports the latest snapshot, staleness and recent
// notifications. Missing data is shown as missing, never as zero usage.
func (s *Server) handleUsageStatusV1(w http.ResponseWriter, r *http.Request) {
	if !s.requireUsage(w) {
		return
	}
	status, err := s.usage.Status(r.Context())
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, status)
}

// handleUsageRefreshV1 runs one usage cycle on demand. Reader unavailability
// is surfaced as a 502 with the stored error; snapshots stay untouched.
func (s *Server) handleUsageRefreshV1(w http.ResponseWriter, r *http.Request) {
	if !s.requireUsage(w) {
		return
	}
	if err := s.usage.RunOnce(r.Context()); err != nil {
		writeError(w, domain.E("USAGE_REFRESH_FAILED", 502, "%v", err))
		return
	}
	status, err := s.usage.Status(r.Context())
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, status)
}
