package api

import (
	crand "crypto/rand"
	"crypto/subtle"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"proxypool/internal/domain"
	"proxypool/internal/storage/sqlite"
)

//go:embed admin.html
var adminPage string

func (s *Server) handleAdminPage(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write([]byte(adminPage))
}

func (s *Server) handleSourcesOverview(w http.ResponseWriter, r *http.Request) {
	overviews, err := s.store.SourcesOverview(r.Context())
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if overviews == nil {
		overviews = []sqlite.SourceOverview{}
	}
	writeJSON(w, http.StatusOK, overviews)
}

func (s *Server) handleSourceValidate(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := s.store.GetSource(r.Context(), id); err != nil {
		writeStoreError(w, err)
		return
	}
	started, err := s.nightly.StartSourceRun(id)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if !started {
		writeError(w, domain.E("VALIDATION_IN_PROGRESS", 409,
			"another validation pass is already running"))
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "started", "source_id": id})
}

func (s *Server) handleValidationStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.nightly.Status())
}

const (
	sessionCookie = "pp_admin"
	sessionTTL    = 12 * time.Hour
)

// sessions holds in-memory admin sessions. The deployment invariant is one
// active process, so logout and restart both invalidate every session.
type sessions struct {
	mu     sync.Mutex
	tokens map[string]time.Time
}

func newSessions() *sessions {
	return &sessions{tokens: map[string]time.Time{}}
}

func (s *sessions) issue(ttl time.Duration) string {
	b := make([]byte, 32)
	if _, err := crand.Read(b); err != nil {
		panic(err)
	}
	token := hex.EncodeToString(b)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tokens[token] = time.Now().Add(ttl)
	return token
}

func (s *sessions) valid(token string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	expiry, ok := s.tokens[token]
	if !ok {
		return false
	}
	if time.Now().After(expiry) {
		delete(s.tokens, token)
		return false
	}
	return true
}

func (s *sessions) revoke(token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tokens, token)
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 2048)
	var body struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, domain.InvalidRequest("invalid JSON body: %v", err))
		return
	}
	if subtle.ConstantTimeCompare([]byte(body.Token), []byte(s.cfg.APIToken)) != 1 {
		writeError(w, domain.Unauthenticated())
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    s.sessions.issue(sessionTTL),
		Path:     "/",
		Expires:  time.Now().Add(sessionTTL),
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		Secure:   r.TLS != nil,
	})
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	if cookie, err := r.Cookie(sessionCookie); err == nil {
		s.sessions.revoke(cookie.Value)
	}
	http.SetCookie(w, &http.Cookie{
		Name: sessionCookie, Value: "", Path: "/", MaxAge: -1,
		HttpOnly: true, SameSite: http.SameSiteStrictMode,
	})
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleDailyStats(w http.ResponseWriter, r *http.Request) {
	now := time.Now()
	from := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	stats, err := s.store.DailySourceStats(r.Context(), from, from.Add(24*time.Hour))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if stats == nil {
		stats = []sqlite.DailyStat{}
	}
	writeJSON(w, http.StatusOK, stats)
}
