package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"proxypool/internal/application"
	"proxypool/internal/config"
	"proxypool/internal/domain"
	"proxypool/internal/integrations/sites"
	"proxypool/internal/storage/sqlite"
)

type Server struct {
	cfg      config.Config
	log      *slog.Logger
	acquire  *application.AcquireService
	ingest   *application.IngestService
	leases   *application.LeaseService
	store    *sqlite.Store
	nightly  *application.NightlyValidator
	prewarm  *application.PrewarmService
	keys     *application.KeyService
	sessions *sessions
	logins   *loginLimiter
}

func NewServer(cfg config.Config, log *slog.Logger, acquire *application.AcquireService,
	ingest *application.IngestService, leases *application.LeaseService, store *sqlite.Store,
	nightly *application.NightlyValidator, prewarm *application.PrewarmService,
	keys *application.KeyService) *Server {
	return &Server{cfg: cfg, log: log, acquire: acquire, ingest: ingest,
		leases: leases, store: store, nightly: nightly, prewarm: prewarm,
		keys: keys, sessions: newSessions(), logins: newLoginLimiter()}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.Handle("GET /v1/judge", NewJudgeHandler(s.cfg.JudgeSecret, s.cfg.JudgeTrustForwarded))
	mux.HandleFunc("GET /admin", s.handleAdminPage)
	mux.HandleFunc("POST /admin/login", s.handleLogin)
	mux.HandleFunc("POST /admin/logout", s.handleLogout)
	mux.HandleFunc("GET /v1/admin/sources", s.handleSourcesOverview)
	mux.HandleFunc("GET /v1/admin/sites", s.handleSitesCatalog)
	mux.HandleFunc("GET /v1/admin/keys", s.handleListKeys)
	mux.HandleFunc("POST /v1/admin/keys", s.handleCreateKey)
	mux.HandleFunc("PATCH /v1/admin/keys/{id}", s.handleUpdateKey)
	mux.HandleFunc("DELETE /v1/admin/keys/{id}", s.handleDeleteKey)
	mux.HandleFunc("POST /v1/proxies/acquire", s.handleAcquire)
	mux.HandleFunc("POST /v1/leases/{id}/release", s.handleRelease)
	mux.HandleFunc("POST /v1/leases/{id}/feedback", s.handleFeedback)
	mux.HandleFunc("POST /v1/sources", s.handleCreateSource)
	mux.HandleFunc("DELETE /v1/sources/{id}", s.handleDeleteSource)
	mux.HandleFunc("POST /v1/sources/{id}/imports", s.handleImport)
	mux.HandleFunc("POST /v1/sources/{id}/fetch", s.handleFetchSource)
	mux.HandleFunc("POST /v1/sources/{id}/validate", s.handleSourceValidate)
	mux.HandleFunc("POST /v1/prewarm", s.handlePrewarm)
	mux.HandleFunc("GET /v1/admin/stats", s.handleStats)
	mux.HandleFunc("GET /v1/admin/validation", s.handleValidationStatus)
	mux.HandleFunc("GET /v1/admin/prewarm", s.handlePrewarmStatus)
	mux.HandleFunc("GET /v1/admin/daily", s.handleDailyStats)
	return s.middleware(mux)
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (s *Server) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		requestID := domain.NewID("req_")
		w.Header().Set("X-Request-Id", requestID)
		sw := &statusWriter{ResponseWriter: w, status: 200}
		defer func() {
			if p := recover(); p != nil {
				s.log.Error("panic in handler", "request_id", requestID, "panic", p)
				writeError(sw, domain.E("INTERNAL", 500, "internal error"))
			}
			s.log.Info("request", "request_id", requestID, "method", r.Method,
				"path", r.URL.Path, "status", sw.status, "duration_ms", time.Since(start).Milliseconds())
		}()
		public := r.URL.Path == "/healthz" || r.URL.Path == "/admin" || r.URL.Path == "/v1/judge" ||
			r.URL.Path == "/admin/login" || r.URL.Path == "/admin/logout"
		if !public {
			ident, ok, viaCookie := s.authenticate(r)
			if !ok {
				writeError(sw, domain.Unauthenticated())
				return
			}
			if !ident.Admin && !isDownstreamPath(r) {
				writeError(sw, domain.E("ADMIN_REQUIRED", 403,
					"this endpoint requires an admin session or the master API token"))
				return
			}
			// Cookie-authenticated mutations require the fetch-only header so
			// a cross-site form post cannot ride the session cookie.
			if viaCookie && r.Method != http.MethodGet && r.Header.Get("X-PP-Admin") != "1" {
				writeError(sw, domain.E("CSRF_HEADER_REQUIRED", 403,
					"cookie-authenticated mutations must send X-PP-Admin: 1"))
				return
			}
			r = r.WithContext(context.WithValue(r.Context(), authContextKey{}, ident))
		}
		next.ServeHTTP(sw, r)
	})
}

func bearerToken(r *http.Request) (string, bool) {
	const prefix = "Bearer "
	h := r.Header.Get("Authorization")
	if len(h) <= len(prefix) || h[:len(prefix)] != prefix {
		return "", false
	}
	return h[len(prefix):], true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, err *domain.Error) {
	writeJSON(w, err.Status, map[string]any{
		"error": map[string]any{"code": err.Code, "message": err.Message},
	})
}

func writeStoreError(w http.ResponseWriter, err error) {
	var derr *domain.Error
	if !errors.As(err, &derr) {
		derr = domain.AsError(err)
	}
	writeError(w, derr)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleAcquire(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 8192)
	var req application.AcquireRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, domain.InvalidRequest("invalid JSON body: %v", err))
		return
	}
	if raw := r.URL.Query().Get("cd"); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || v < 0 {
			writeError(w, domain.InvalidRequest("cd query parameter must be a non-negative integer (seconds)"))
			return
		}
		req.CDSeconds = &v
	}
	ident := identityFrom(r.Context())
	req.ConsumerID = ident.ConsumerID
	if req.CDSeconds == nil && ident.DefaultCooldownMS > 0 {
		req.CooldownMS = ident.DefaultCooldownMS
	}
	result, derr := s.acquire.Acquire(r.Context(), req)
	if derr != nil {
		writeError(w, derr)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleRelease(w http.ResponseWriter, r *http.Request) {
	ident := identityFrom(r.Context())
	ok, derr := s.leases.Release(r.Context(), r.PathValue("id"), ident.ConsumerID, ident.Admin)
	if derr != nil {
		writeError(w, derr)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"released": ok})
}

func (s *Server) handleFeedback(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 8192)
	var body struct {
		Outcome string `json:"outcome"`
		Stage   string `json:"stage"`
		Note    string `json:"note"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, domain.InvalidRequest("invalid JSON body: %v", err))
		return
	}
	ident := identityFrom(r.Context())
	if derr := s.leases.Feedback(r.Context(), r.PathValue("id"), body.Outcome, body.Stage, body.Note,
		ident.ConsumerID, ident.Admin); derr != nil {
		writeError(w, derr)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "recorded"})
}

func (s *Server) handleCreateSource(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 8192)
	var body struct {
		SourceID           string `json:"source_id"`
		AdapterType        string `json:"adapter_type"`
		ParserType         string `json:"parser_type"`
		DefaultTransport   string `json:"default_transport"`
		MaxPoolLeases      int    `json:"max_pool_leases"`
		LocalRetentionMS   int64  `json:"local_retention_ms"`
		Enabled            *bool  `json:"enabled"`
		Protocol           string `json:"protocol"`
		CountryCode        string `json:"country_code"`
		FetchCount         int    `json:"fetch_count"`
		MinFetchIntervalMS int64  `json:"min_fetch_interval_ms"`
		SiteID             string `json:"site_id"`
		MaxPages           int    `json:"max_pages"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, domain.InvalidRequest("invalid JSON body: %v", err))
		return
	}
	enabled := true
	if body.Enabled != nil {
		enabled = *body.Enabled
	}
	cfg := application.SourceConfig{
		DefaultTransport: domain.Transport(body.DefaultTransport),
		MaxPoolLeases:    body.MaxPoolLeases,
		LocalRetentionMS: body.LocalRetentionMS,
	}
	adapterType := body.AdapterType
	if adapterType == "" {
		adapterType = application.AdapterCustom
	}
	switch body.AdapterType {
	case "", application.AdapterCustom:
		if body.ParserType != "" && body.ParserType != application.ParserLines {
			writeError(w, domain.InvalidRequest("parser_type %q is not supported; use lines_v1", body.ParserType))
			return
		}
		if body.Protocol != "" {
			writeError(w, domain.InvalidRequest("protocol belongs to provider_api channels; set adapter_type %q", application.AdapterProvider))
			return
		}
		if body.SiteID != "" {
			writeError(w, domain.InvalidRequest("site_id belongs to site_fetch channels; set adapter_type %q", application.AdapterSite))
			return
		}
	case application.AdapterProvider:
		if body.ParserType != "" && body.ParserType != application.ParserJSONMapping {
			writeError(w, domain.InvalidRequest("parser_type %q is not supported for provider_api; use json_mapping_v1", body.ParserType))
			return
		}
		if body.SiteID != "" || body.MaxPages != 0 {
			writeError(w, domain.InvalidRequest("site_id/max_pages belong to site_fetch channels; set adapter_type %q", application.AdapterSite))
			return
		}
		cfg.Provider = &application.ProviderConfig{
			Protocol:           body.Protocol,
			CountryCode:        body.CountryCode,
			FetchCount:         body.FetchCount,
			MinFetchIntervalMS: body.MinFetchIntervalMS,
		}
	case application.AdapterSite:
		if body.ParserType != "" && body.ParserType != application.ParserSite {
			writeError(w, domain.InvalidRequest("parser_type %q is not supported for site_fetch; use site_v1", body.ParserType))
			return
		}
		if body.SiteID == "" {
			writeError(w, domain.InvalidRequest("site_id is required for site_fetch channels"))
			return
		}
		if body.Protocol != "" || body.CountryCode != "" || body.FetchCount != 0 {
			writeError(w, domain.InvalidRequest("protocol/country_code/fetch_count belong to provider_api channels"))
			return
		}
		cfg.Site = &application.SiteConfig{SiteID: body.SiteID, MaxPages: body.MaxPages}
	default:
		writeError(w, domain.InvalidRequest("adapter_type %q is not supported; use custom, provider_api, or site_fetch", body.AdapterType))
		return
	}
	if err := s.ingest.EnsureSource(r.Context(), body.SourceID, cfg, enabled); err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{
		"source_id": body.SourceID, "adapter_type": adapterType, "status": "created"})
}

func (s *Server) handleSitesCatalog(w http.ResponseWriter, _ *http.Request) {
	type catalogSite struct {
		SiteID    string `json:"site_id"`
		Name      string `json:"name"`
		Anonymity string `json:"anonymity"`
		Parser    string `json:"parser"`
		Pages     int    `json:"pages"`
		AutoPages bool   `json:"auto_pages"`
		Note      string `json:"note"`
	}
	out := make([]catalogSite, 0, len(sites.Catalog))
	for _, site := range sites.Catalog {
		out = append(out, catalogSite{
			SiteID:    site.ID,
			Name:      site.Name,
			Anonymity: string(site.Anonymity),
			Parser:    string(site.Kind),
			Pages:     site.Pages,
			AutoPages: site.AutoPages,
			Note:      site.Note,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleFetchSource(w http.ResponseWriter, r *http.Request) {
	result, derr := s.ingest.FetchSource(r.Context(), r.PathValue("id"))
	if derr != nil {
		writeError(w, derr)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleImport(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 8<<20)
	content, err := readAll(r)
	if err != nil {
		writeError(w, domain.InvalidRequest("read import body: %v", err))
		return
	}
	result, derr := s.ingest.ImportLines(r.Context(), r.PathValue("id"), content)
	if derr != nil {
		writeError(w, derr)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.store.Stats(r.Context())
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, stats)
}
