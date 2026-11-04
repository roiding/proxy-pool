package api

import (
	"net/http"
	"strconv"
	"strings"

	"proxypool/internal/application"
	"proxypool/internal/domain"
)

type prewarmJobBody struct {
	Count      *int                `json:"count"`
	TimeoutSec *int64              `json:"timeout"`
	Protocols  []string            `json:"protocols"`
	Countries  []string            `json:"country"`
	SourceIDs  optionalStringSlice `json:"source_ids"`
}

// handlePrewarmV1 creates a durable prewarm job for the caller's project and
// answers 202 with the job id. Supply work runs detached from the request.
func (s *Server) handlePrewarmV1(w http.ResponseWriter, r *http.Request) {
	var body prewarmJobBody
	if derr := s.decodeAdminBody(w, r, &body); derr != nil {
		writeError(w, derr)
		return
	}
	req := application.PrewarmRequest{}
	if body.Count != nil {
		req.Count = *body.Count
		if req.Count > s.cfg.MaxCount {
			writeError(w, domain.InvalidRequest("count %d exceeds the maximum of %d",
				req.Count, s.cfg.MaxCount))
			return
		}
	}
	if body.TimeoutSec != nil {
		if *body.TimeoutSec < 1 {
			writeError(w, domain.InvalidRequest("timeout must be at least 1 second"))
			return
		}
		maxSec := s.cfg.PrewarmMaxDeadlineMS / 1000
		if *body.TimeoutSec > maxSec {
			writeError(w, domain.InvalidRequest("timeout %d seconds exceeds the maximum of %d seconds",
				*body.TimeoutSec, maxSec))
			return
		}
		req.DeadlineMS = *body.TimeoutSec * 1000
	}
	for _, p := range body.Protocols {
		switch domain.Transport(p) {
		case domain.TransportHTTP, domain.TransportSOCKS5:
			req.Filters.Transports = append(req.Filters.Transports, domain.Transport(p))
		default:
			writeError(w, domain.InvalidRequest("unknown protocol %q; supported: http, socks5", p))
			return
		}
	}
	req.Filters.Countries = body.Countries
	ident := identityFrom(r.Context())
	sourceIDs, derr := s.resolveSourceScope(ident, body.SourceIDs)
	if derr != nil {
		writeError(w, derr)
		return
	}
	req.Filters.SourceIDs = sourceIDs
	job, derr := s.prewarm.StartJob(r.Context(), ident.ProjectID, req)
	if derr != nil {
		writeError(w, derr)
		return
	}
	// domain.Job already carries job_id; downstream reads progress and the
	// final result from the same object via GET /api/v1/jobs/{id}.
	writeJSON(w, http.StatusAccepted, job)
}

// handleJobCancelV1 cancels the caller's own active job; other projects see
// a 404 so job ids cannot be probed across projects.
func (s *Server) handleJobCancelV1(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	job, err := s.store.GetJob(r.Context(), id)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	ident := identityFrom(r.Context())
	if !ident.Admin && job.ProjectID != ident.ProjectID {
		writeError(w, domain.NotFound("job"))
		return
	}
	if job.Kind != "prewarm" {
		writeError(w, domain.E("JOB_KIND_UNSUPPORTED", 409,
			"job kind %q cannot be cancelled through this endpoint", job.Kind))
		return
	}
	if derr := s.prewarm.CancelJob(r.Context(), id); derr != nil {
		writeError(w, derr)
		return
	}
	updated, err := s.store.GetJob(r.Context(), id)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, updated)
}

// handleDeleteSessionV1 releases one of the caller's own sticky bindings.
// Cross-project ids and unknown ids are 404; the project CD window is never
// touched by a session release.
func (s *Server) handleDeleteSessionV1(w http.ResponseWriter, r *http.Request) {
	sessionID := strings.TrimSpace(r.PathValue("id"))
	if sessionID == "" || len(sessionID) > 128 {
		writeError(w, domain.InvalidRequest("session id must be 1-128 characters"))
		return
	}
	ident := identityFrom(r.Context())
	if _, err := s.store.GetBinding(r.Context(), ident.ProjectID, sessionID); err != nil {
		writeStoreError(w, err)
		return
	}
	if err := s.store.DeleteBinding(r.Context(), ident.ProjectID, sessionID); err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "released", "session_id": sessionID,
	})
}

// handleListJobsAdmin backs the task center. Masters see every project;
// ?project_id= narrows the list.
func (s *Server) handleListJobsAdmin(w http.ResponseWriter, r *http.Request) {
	limit := parseLimit(r, 50)
	jobs, err := s.store.ListJobs(r.Context(), r.URL.Query().Get("project_id"), limit)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, jobs)
}

// handleListSessionsAdmin lists sticky bindings with their project, state
// and deadline. Sealed connection material is never included.
func (s *Server) handleListSessionsAdmin(w http.ResponseWriter, r *http.Request) {
	limit := parseLimit(r, 50)
	bindings, err := s.store.ListBindings(r.Context(), r.URL.Query().Get("project_id"), limit)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, bindings)
}

// handleAdminDeleteSession releases one project's binding from the console.
// The project CD window itself is never touched.
func (s *Server) handleAdminDeleteSession(w http.ResponseWriter, r *http.Request) {
	projectID := r.PathValue("id")
	sessionID := strings.TrimSpace(r.PathValue("sid"))
	if sessionID == "" || len(sessionID) > 128 {
		writeError(w, domain.InvalidRequest("session id must be 1-128 characters"))
		return
	}
	if _, err := s.store.GetBinding(r.Context(), projectID, sessionID); err != nil {
		writeStoreError(w, err)
		return
	}
	if err := s.store.DeleteBinding(r.Context(), projectID, sessionID); err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "released", "project_id": projectID, "session_id": sessionID,
	})
}

func parseLimit(r *http.Request, def int) int {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return def
	}
	if n > 500 {
		return 500
	}
	return n
}

// handleAdaptersAdmin exposes the closed adapter registry so the console
// renders real capabilities instead of invented forms.
func (s *Server) handleAdaptersAdmin(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"adapters": application.AdapterDescriptors(),
	})
}
