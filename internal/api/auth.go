package api

import (
	"context"
	"crypto/subtle"
	"net/http"
	"strings"
	"sync"
	"time"
)

type authIdentity struct {
	Admin             bool
	ConsumerID        string
	KeyID             string
	Name              string
	DefaultCooldownMS int64
}

type authContextKey struct{}

func identityFrom(ctx context.Context) authIdentity {
	if v, ok := ctx.Value(authContextKey{}).(authIdentity); ok {
		return v
	}
	return authIdentity{}
}

// authenticate resolves the request to either the master/admin identity or
// an enabled downstream API key. Downstream tokens are accepted through
// Authorization: Bearer, X-API-Key, or the key query parameter.
func (s *Server) authenticate(r *http.Request) (authIdentity, bool, bool) {
	if cookie, err := r.Cookie(sessionCookie); err == nil && s.sessions.valid(cookie.Value) {
		return authIdentity{Admin: true, ConsumerID: "admin", Name: "admin"}, true, true
	}
	token, ok := requestToken(r)
	if !ok {
		return authIdentity{}, false, false
	}
	if subtle.ConstantTimeCompare([]byte(token), []byte(s.cfg.APIToken)) == 1 {
		return authIdentity{Admin: true, ConsumerID: "master", Name: "master"}, true, false
	}
	if s.keys != nil {
		if key, found := s.keys.Authenticate(r.Context(), token); found {
			return authIdentity{
				ConsumerID:        key.ID,
				KeyID:             key.ID,
				Name:              key.Name,
				DefaultCooldownMS: key.DefaultCooldownMS,
			}, true, false
		}
	}
	return authIdentity{}, false, false
}

func requestToken(r *http.Request) (string, bool) {
	if token, ok := bearerToken(r); ok {
		return token, true
	}
	if token := strings.TrimSpace(r.Header.Get("X-API-Key")); token != "" {
		return token, true
	}
	if token := strings.TrimSpace(r.URL.Query().Get("key")); token != "" {
		return token, true
	}
	return "", false
}

// isDownstreamPath limits API-key identities to the delivery surface. Admin
// and source-management endpoints require the master token or a session.
func isDownstreamPath(r *http.Request) bool {
	path := r.URL.Path
	if r.Method == http.MethodPost {
		if path == "/v1/proxies/acquire" {
			return true
		}
		if strings.HasPrefix(path, "/v1/leases/") &&
			(strings.HasSuffix(path, "/release") || strings.HasSuffix(path, "/feedback")) {
			return true
		}
	}
	return false
}

// loginLimiter bounds password attempts per client address. It is in-memory
// because the deployment invariant is one active process.
type loginLimiter struct {
	mu       sync.Mutex
	attempts map[string][]time.Time
}

func newLoginLimiter() *loginLimiter {
	return &loginLimiter{attempts: map[string][]time.Time{}}
}

func (l *loginLimiter) blocked(key string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	recent := l.attempts[key][:0]
	for _, t := range l.attempts[key] {
		if now.Sub(t) < time.Minute {
			recent = append(recent, t)
		}
	}
	l.attempts[key] = recent
	return len(recent) >= 10
}

func (l *loginLimiter) fail(key string, now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.attempts[key] = append(l.attempts[key], now)
}

func (l *loginLimiter) clear(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.attempts, key)
}
