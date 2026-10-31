package api

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"proxypool/internal/application"
	"proxypool/internal/config"
	"proxypool/internal/domain"
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
	sessions *sessions
}

func NewServer(cfg config.Config, log *slog.Logger, acquire *application.AcquireService,
	ingest *application.IngestService, leases *application.LeaseService, store *sqlite.Store,
	nightly *application.NightlyValidator) *Server {
	return &Server{cfg: cfg, log: log, acquire: acquire, ingest: ingest,
		leases: leases, store: store, nightly: nightly, sessions: newSessions()}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.Handle("GET /v1/judge", NewJudgeHandler(s.cfg.JudgeSecret, s.cfg.JudgeTrustForwarded))
	mux.HandleFunc("GET /admin", s.handleAdminPage)
	mux.HandleFunc("POST /admin/login", s.handleLogin)
	mux.HandleFunc("POST /admin/logout", s.handleLogout)
	mux.HandleFunc("GET /v1/admin/sources", s.handleSourcesOverview)
	mux.HandleFunc("POST /v1/proxies/acquire", s.handleAcquire)
	mux.HandleFunc("POST /v1/leases/{id}/release", s.handleRelease)
	mux.HandleFunc("POST /v1/leases/{id}/feedback", s.handleFeedback)
	mux.HandleFunc("POST /v1/sources", s.handleCreateSource)
	mux.HandleFunc("DELETE /v1/sources/{id}", s.handleDeleteSource)
	mux.HandleFunc("POST /v1/sources/{id}/imports", s.handleImport)
	mux.HandleFunc("POST /v1/sources/{id}/validate", s.handleSourceValidate)
	mux.HandleFunc("GET /v1/admin/stats", s.handleStats)
	mux.HandleFunc("GET /v1/admin/validation", s.handleValidationStatus)
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
			_, hasBearer := bearerToken(r)
			if !s.authorized(r) {
				writeError(sw, domain.Unauthenticated())
				return
			}
			// Cookie-authenticated mutations require the fetch-only header so a
			// cross-site form post cannot ride the session cookie (CSRF guard).
			if !hasBearer && r.Method != http.MethodGet && r.Header.Get("X-PP-Admin") != "1" {
				writeError(sw, domain.E("CSRF_HEADER_REQUIRED", 403,
					"cookie-authenticated mutations must send X-PP-Admin: 1"))
				return
			}
		}
		next.ServeHTTP(sw, r)
	})
}

func (s *Server) authorized(r *http.Request) bool {
	if token, ok := bearerToken(r); ok {
		return subtle.ConstantTimeCompare([]byte(token), []byte(s.cfg.APIToken)) == 1
	}
	if cookie, err := r.Cookie(sessionCookie); err == nil {
		return s.sessions.valid(cookie.Value)
	}
	return false
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
	result, derr := s.acquire.Acquire(r.Context(), req)
	if derr != nil {
		writeError(w, derr)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleRelease(w http.ResponseWriter, r *http.Request) {
	ok, derr := s.leases.Release(r.Context(), r.PathValue("id"))
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
	if derr := s.leases.Feedback(r.Context(), r.PathValue("id"), body.Outcome, body.Stage, body.Note); derr != nil {
		writeError(w, derr)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "recorded"})
}

func (s *Server) handleCreateSource(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 8192)
	var body struct {
		SourceID         string `json:"source_id"`
		ParserType       string `json:"parser_type"`
		DefaultTransport string `json:"default_transport"`
		MaxPoolLeases    int    `json:"max_pool_leases"`
		LocalRetentionMS int64  `json:"local_retention_ms"`
		Enabled          *bool  `json:"enabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, domain.InvalidRequest("invalid JSON body: %v", err))
		return
	}
	if body.ParserType != "" && body.ParserType != "lines_v1" {
		writeError(w, domain.InvalidRequest("parser_type %q is not supported; use lines_v1", body.ParserType))
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
	if err := s.ingest.EnsureSource(r.Context(), body.SourceID, cfg, enabled); err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"source_id": body.SourceID, "status": "created"})
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
