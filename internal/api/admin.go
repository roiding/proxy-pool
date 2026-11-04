package api

import (
	crand "crypto/rand"
	"crypto/subtle"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"proxypool/internal/application"
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
	src, err := s.store.GetSource(r.Context(), id)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if src.AdapterType != application.AdapterCustom {
		writeError(w, domain.InvalidRequest("source %q is a provider channel; it is validated at request time, use fetch instead", id))
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

const sessionCookie = "pp_admin"

// sessions holds in-memory admin sessions. The deployment invariant is one
// active process, so logout and restart both invalidate every session.
// sessions holds in-memory admin sessions with a hard capacity bound. The
// deployment invariant is one active process, so logout and restart both
// invalidate every session.
type sessions struct {
	mu     sync.Mutex
	tokens map[string]time.Time
	limit  int
}

func newSessions(limit int) *sessions {
	if limit < 1 {
		limit = 1
	}
	return &sessions{tokens: map[string]time.Time{}, limit: limit}
}

func (s *sessions) issue(ttl time.Duration) string {
	b := make([]byte, 32)
	if _, err := crand.Read(b); err != nil {
		panic(err)
	}
	token := hex.EncodeToString(b)
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.tokens) >= s.limit {
		// Evict the soonest-expiring session so the newest login wins.
		oldest := ""
		var oldestExpiry time.Time
		for t, exp := range s.tokens {
			if oldest == "" || exp.Before(oldestExpiry) {
				oldest, oldestExpiry = t, exp
			}
		}
		delete(s.tokens, oldest)
	}
	s.tokens[token] = now.Add(ttl)
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
	client := s.clientHost(r)
	now := time.Now()
	if s.logins.blocked(client, now) {
		writeError(w, domain.E("LOGIN_RATE_LIMITED", 429,
			"too many failed login attempts; retry in a minute"))
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 2048)
	var body struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, domain.InvalidRequest("invalid JSON body: %v", err))
		return
	}
	expected := s.cfg.AdminPassword
	if expected == "" {
		expected = s.cfg.APIToken
	}
	if subtle.ConstantTimeCompare([]byte(body.Token), []byte(expected)) != 1 {
		s.logins.fail(client, now)
		writeError(w, domain.Unauthenticated())
		return
	}
	s.logins.clear(client)
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    s.sessions.issue(s.cfg.AdminSessionTTL()),
		Path:     "/",
		Expires:  time.Now().Add(s.cfg.AdminSessionTTL()),
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		Secure:   s.secureCookie(r),
	})
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// secureCookie marks the session cookie Secure when the deployment is
// explicitly public-HTTPS or the request arrives through a trusted proxy
// that reports HTTPS. An arbitrary X-Forwarded-Proto is never trusted.
func (s *Server) secureCookie(r *http.Request) bool {
	if r.TLS != nil || s.cfg.PublicHTTPS {
		return true
	}
	peer := s.peerHost(r)
	for _, n := range s.cfg.TrustedProxies() {
		if n.Contains(peer) && r.Header.Get("X-Forwarded-Proto") == "https" {
			return true
		}
	}
	return false
}

// peerHost returns the direct TCP peer, or the rightmost X-Forwarded-For
// entry when that peer is itself a trusted proxy.
func (s *Server) peerHost(r *http.Request) net.IP {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return nil
	}
	for _, n := range s.cfg.TrustedProxies() {
		if n.Contains(ip) {
			if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
				parts := strings.Split(xff, ",")
				if last := strings.TrimSpace(parts[len(parts)-1]); last != "" {
					if forwarded := net.ParseIP(last); forwarded != nil {
						return forwarded
					}
				}
			}
			break
		}
	}
	return ip
}

func (s *Server) clientHost(r *http.Request) string {
	if ip := s.peerHost(r); ip != nil {
		return ip.String()
	}
	return r.RemoteAddr
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	if cookie, err := r.Cookie(sessionCookie); err == nil {
		s.sessions.revoke(cookie.Value)
	}
	http.SetCookie(w, &http.Cookie{
		Name: sessionCookie, Value: "", Path: "/", MaxAge: -1,
		HttpOnly: true, SameSite: http.SameSiteStrictMode, Secure: s.secureCookie(r),
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
