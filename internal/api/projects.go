package api

import (
	"net/http"
	"strings"

	"proxypool/internal/domain"
	"proxypool/internal/validation"
)

const (
	maxProjectNameLen     = 128
	maxProjectSourceScope = 256
)

type projectValidationBody struct {
	Mode   string `json:"mode"`
	Target string `json:"target"`
}

type createProjectBody struct {
	Name             string                 `json:"name"`
	Enabled          *bool                  `json:"enabled"`
	CooldownMS       *int64                 `json:"cooldown_ms"`
	TimeoutMS        *int64                 `json:"timeout_ms"`
	Validation       *projectValidationBody `json:"validation"`
	AllowedSourceIDs optionalStringSlice    `json:"allowed_source_ids"`
}

type patchProjectBody struct {
	ConfigVersion    int64                  `json:"config_version"`
	Name             *string                `json:"name"`
	Enabled          *bool                  `json:"enabled"`
	CooldownMS       *int64                 `json:"cooldown_ms"`
	TimeoutMS        *int64                 `json:"timeout_ms"`
	Validation       *projectValidationBody `json:"validation"`
	AllowedSourceIDs optionalStringSlice    `json:"allowed_source_ids"`
}

func (s *Server) handleListProjects(w http.ResponseWriter, r *http.Request) {
	projects, err := s.store.ListProjects(r.Context())
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if projects == nil {
		projects = []domain.Project{}
	}
	writeJSON(w, http.StatusOK, projects)
}

func (s *Server) handleCreateProject(w http.ResponseWriter, r *http.Request) {
	var body createProjectBody
	if derr := s.decodeAdminBody(w, r, &body); derr != nil {
		writeError(w, derr)
		return
	}
	project, derr := s.projectFromCreate(&body)
	if derr != nil {
		writeError(w, derr)
		return
	}
	if _, err := s.store.GetProject(r.Context(), project.ID); err == nil {
		writeError(w, domain.E("PROJECT_EXISTS", 409, "project %q already exists", project.ID))
		return
	}
	// EnsureProject is insert-only: a concurrent create can never overwrite
	// the winner's configuration.
	if err := s.store.EnsureProject(r.Context(), project, timeNow()); err != nil {
		writeStoreError(w, err)
		return
	}
	saved, err := s.store.GetProject(r.Context(), project.ID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, saved)
}

func (s *Server) handleGetProject(w http.ResponseWriter, r *http.Request) {
	project, err := s.store.GetProject(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, project)
}

func (s *Server) handlePatchProject(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var body patchProjectBody
	if derr := s.decodeAdminBody(w, r, &body); derr != nil {
		writeError(w, derr)
		return
	}
	current, err := s.store.GetProject(r.Context(), id)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if body.ConfigVersion != current.ConfigVersion {
		writeError(w, domain.E("CONFIG_VERSION_CONFLICT", 409,
			"config_version %d does not match current %d", body.ConfigVersion, current.ConfigVersion))
		return
	}
	if body.Name != nil {
		current.Name = strings.TrimSpace(*body.Name)
	}
	if body.Enabled != nil {
		current.Enabled = *body.Enabled
	}
	if body.CooldownMS != nil {
		current.Defaults.CooldownMS = *body.CooldownMS
	}
	if body.TimeoutMS != nil {
		current.Defaults.TimeoutMS = *body.TimeoutMS
	}
	if body.Validation != nil {
		v, derr := s.validationScopeFromBody(body.Validation)
		if derr != nil {
			writeError(w, derr)
			return
		}
		current.Defaults.Validation = v
	}
	if body.AllowedSourceIDs.Present {
		ids, derr := sourceScopeFromOptional(body.AllowedSourceIDs)
		if derr != nil {
			writeError(w, derr)
			return
		}
		current.AllowedSourceIDs = ids
	}
	if derr := s.validateProject(&current); derr != nil {
		writeError(w, derr)
		return
	}
	// UpdateProject performs the compare-and-swap inside one transaction;
	// the read above only provides the current values and expected version.
	saved, err := s.store.UpdateProject(r.Context(), current, body.ConfigVersion, timeNow())
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, saved)
}

// handleDeleteProject soft-disables the project. CD windows and history are
// deliberately retained: a deleted project must never lose a live cooldown,
// and recreation under the same id resumes it instead of resetting state.
func (s *Server) handleDeleteProject(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	project, err := s.store.GetProject(r.Context(), id)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if project.Enabled {
		project.Enabled = false
		saved, err := s.store.UpdateProject(r.Context(), project, project.ConfigVersion, timeNow())
		if err != nil {
			writeStoreError(w, err)
			return
		}
		project = saved
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "disabled", "project": project,
	})
}

func (s *Server) handleListProjectKeys(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := s.store.GetProject(r.Context(), id); err != nil {
		writeStoreError(w, err)
		return
	}
	keys, err := s.keys.List(r.Context())
	if err != nil {
		writeStoreError(w, err)
		return
	}
	owned := make([]domain.APIKey, 0, len(keys))
	for _, k := range keys {
		if k.ProjectID == id {
			owned = append(owned, k)
		}
	}
	writeJSON(w, http.StatusOK, owned)
}

func (s *Server) handleCreateProjectKey(w http.ResponseWriter, r *http.Request) {
	projectID := r.PathValue("id")
	if _, err := s.store.GetProject(r.Context(), projectID); err != nil {
		writeStoreError(w, err)
		return
	}
	var body struct {
		Name string `json:"name"`
	}
	if derr := s.decodeAdminBody(w, r, &body); derr != nil {
		writeError(w, derr)
		return
	}
	key, token, err := s.keys.CreateForProject(r.Context(), projectID, body.Name, 0)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"key": key, "token": token})
}

func (s *Server) handleGetProjectSources(w http.ResponseWriter, r *http.Request) {
	project, err := s.store.GetProject(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"unrestricted":   project.AllowedSourceIDs == nil,
		"source_ids":     project.AllowedSourceIDs,
		"config_version": project.ConfigVersion,
	})
}

func (s *Server) handlePutProjectSources(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var body struct {
		ConfigVersion int64               `json:"config_version"`
		SourceIDs     optionalStringSlice `json:"source_ids"`
	}
	if derr := s.decodeAdminBody(w, r, &body); derr != nil {
		writeError(w, derr)
		return
	}
	if !body.SourceIDs.Present {
		writeError(w, domain.InvalidRequest(
			`source_ids is required: null means unrestricted, [] means no sources, ["id"] means the exact allowlist`))
		return
	}
	project, err := s.store.GetProject(r.Context(), id)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if body.ConfigVersion != project.ConfigVersion {
		writeError(w, domain.E("CONFIG_VERSION_CONFLICT", 409,
			"config_version %d does not match current %d", body.ConfigVersion, project.ConfigVersion))
		return
	}
	ids, derr := sourceScopeFromOptional(body.SourceIDs)
	if derr != nil {
		writeError(w, derr)
		return
	}
	project.AllowedSourceIDs = ids
	saved, err := s.store.UpdateProject(r.Context(), project, body.ConfigVersion, timeNow())
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"unrestricted":   saved.AllowedSourceIDs == nil,
		"source_ids":     saved.AllowedSourceIDs,
		"config_version": saved.ConfigVersion,
	})
}

// handleGetJobV1 exposes job progress to its owning project only; any other
// identity gets a 404 so job IDs cannot be probed across projects.
func (s *Server) handleGetJobV1(w http.ResponseWriter, r *http.Request) {
	ident := identityFrom(r.Context())
	projectID := ident.ProjectID
	if ident.Admin {
		projectID = ""
	}
	job, derr := s.prewarm.GetJob(r.Context(), projectID, r.PathValue("id"))
	if derr != nil {
		writeError(w, derr)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) decodeAdminBody(w http.ResponseWriter, r *http.Request, dst any) *domain.Error {
	return decodeStrictJSON(w, r, s.cfg.MaxRequestBodyBytes, dst, false)
}

func (s *Server) projectFromCreate(body *createProjectBody) (domain.Project, *domain.Error) {
	id := domain.NewID("proj_")
	name := strings.TrimSpace(body.Name)
	if name == "" {
		return domain.Project{}, domain.InvalidRequest("name is required")
	}
	if len(name) > maxProjectNameLen {
		return domain.Project{}, domain.InvalidRequest("name must not exceed %d characters", maxProjectNameLen)
	}
	enabled := true
	if body.Enabled != nil {
		enabled = *body.Enabled
	}
	project := domain.Project{
		ID:      id,
		Name:    name,
		Enabled: enabled,
	}
	if body.CooldownMS != nil {
		project.Defaults.CooldownMS = *body.CooldownMS
	}
	if body.TimeoutMS != nil {
		project.Defaults.TimeoutMS = *body.TimeoutMS
	}
	if body.Validation != nil {
		v, derr := s.validationScopeFromBody(body.Validation)
		if derr != nil {
			return domain.Project{}, derr
		}
		project.Defaults.Validation = v
	}
	ids, derr := sourceScopeFromOptional(body.AllowedSourceIDs)
	if derr != nil {
		return domain.Project{}, derr
	}
	project.AllowedSourceIDs = ids
	if derr := s.validateProject(&project); derr != nil {
		return domain.Project{}, derr
	}
	return project, nil
}

func (s *Server) validationScopeFromBody(body *projectValidationBody) (domain.ValidationScope, *domain.Error) {
	switch body.Mode {
	case "", "basic":
		if body.Target != "" {
			return domain.ValidationScope{}, domain.InvalidRequest(
				"validation.target is not used by basic validation")
		}
		return domain.ValidationScope{}, nil
	case "tls":
		scope, derr := validation.NormalizeOptions("tls", body.Target)
		if derr != nil {
			return domain.ValidationScope{}, derr
		}
		return domain.ValidationScope{Mode: scope.Mode, TargetHostPort: scope.TargetHostPort}, nil
	default:
		return domain.ValidationScope{}, domain.InvalidRequest(
			"unknown validation.mode %q; supported: basic, tls", body.Mode)
	}
}

func normalizeSourceScope(ids []string) ([]string, *domain.Error) {
	if ids == nil {
		return nil, nil
	}
	if len(ids) > maxProjectSourceScope {
		return nil, domain.InvalidRequest("allowed_source_ids must not exceed %d entries", maxProjectSourceScope)
	}
	out := make([]string, 0, len(ids))
	seen := make(map[string]bool, len(ids))
	for _, raw := range ids {
		id := strings.TrimSpace(raw)
		if id == "" || len(id) > 128 {
			return nil, domain.InvalidRequest("source id %q must be 1-128 characters", raw)
		}
		if seen[id] {
			return nil, domain.InvalidRequest("source id %q is duplicated", id)
		}
		seen[id] = true
		out = append(out, id)
	}
	return out, nil
}

// sourceScopeFromOptional maps the tri-state JSON field onto the project
// allowlist: an absent field or explicit null is unrestricted; an array —
// including an empty one — is the exact allowlist.
func sourceScopeFromOptional(f optionalStringSlice) ([]string, *domain.Error) {
	if f.Null {
		return nil, nil
	}
	return normalizeSourceScope(f.Values)
}

func (s *Server) validateProject(p *domain.Project) *domain.Error {
	if strings.TrimSpace(p.Name) == "" || len(p.Name) > maxProjectNameLen {
		return domain.InvalidRequest("project name must be 1-%d characters", maxProjectNameLen)
	}
	if p.Defaults.CooldownMS < 0 || p.Defaults.CooldownMS > s.cfg.MaxDownstreamCooldownMS {
		return domain.InvalidRequest("cooldown_ms must be between 0 and %d", s.cfg.MaxDownstreamCooldownMS)
	}
	if p.Defaults.TimeoutMS < 0 || p.Defaults.TimeoutMS > s.cfg.MaxAcquireDeadlineMS {
		return domain.InvalidRequest("timeout_ms must be between 0 and %d", s.cfg.MaxAcquireDeadlineMS)
	}
	switch p.Defaults.Validation.Mode {
	case "", validation.ModeBasic.String():
	case validation.ModeTLS.String():
		if p.Defaults.Validation.TargetHostPort == "" {
			return domain.InvalidRequest("tls validation requires a target host:port")
		}
	default:
		return domain.InvalidRequest("unknown validation.mode %q", p.Defaults.Validation.Mode)
	}
	return nil
}
