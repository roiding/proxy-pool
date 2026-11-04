package api

import (
	"context"
	"crypto/subtle"
	"net/http"
	"strings"
	"sync"
	"time"

	"proxypool/internal/storage/sqlite"
)

type authIdentity struct {
	Admin      bool
	ConsumerID string
	ProjectID  string
	KeyID      string
	Name       string
	// UploadKeyID is set for import credentials; UploadSources is the exact
	// source allowlist those credentials may push into and read from.
	UploadKeyID   string
	UploadSources []string
	// Defaults resolved from the project the identity belongs to; requests
	// may override them, the server ceiling still applies on top.
	DefaultCooldownMS     int64
	DefaultTimeoutMS      int64
	AllowedSourceIDs      []string
	SourceScopeRestricted bool
}

// ownershipID is the lease owner recorded at delivery. For a project key
// that is the key itself; an admin identity is already authorized and falls
// back to its consumer identity for legacy rows.
func (i authIdentity) ownershipID() string {
	if i.Admin {
		return i.ConsumerID
	}
	return i.KeyID
}

type authContextKey struct{}

func identityFrom(ctx context.Context) authIdentity {
	if v, ok := ctx.Value(authContextKey{}).(authIdentity); ok {
		return v
	}
	return authIdentity{}
}

// authenticate resolves the request to either the admin session or a
// downstream credential. Explicit header credentials win: once Bearer or
// X-API-Key is present it must authenticate, with no cookie fallback. The
// old key query parameter is no longer accepted.
func (s *Server) authenticate(r *http.Request) (authIdentity, bool, bool) {
	if token, ok := headerToken(r); ok {
		ident, valid := s.identifyToken(r.Context(), token)
		return ident, valid, false
	}
	if cookie, err := r.Cookie(sessionCookie); err == nil && s.sessions.valid(cookie.Value) {
		// Cookie identities share the default project's CD subject and
		// defaults; being an admin never bypasses project cooldown rules.
		ident := authIdentity{Admin: true, ProjectID: sqlite.DefaultProjectID,
			ConsumerID: sqlite.DefaultProjectID, Name: "admin"}
		if !s.applyProjectDefaults(r.Context(), &ident) {
			return authIdentity{}, false, false
		}
		return ident, true, true
	}
	return authIdentity{}, false, false
}

// headerToken reports an explicit credential header. Presence alone counts,
// even with an empty value: a request carrying a header must authenticate by
// that header and never falls back to the admin session cookie. The bearer
// scheme is case-insensitive; any other Authorization scheme is still an
// explicit credential attempt and fails authentication below.
func headerToken(r *http.Request) (string, bool) {
	if _, present := r.Header["Authorization"]; present {
		h := strings.TrimSpace(r.Header.Get("Authorization"))
		if len(h) >= 7 && strings.EqualFold(h[:7], "Bearer ") {
			return strings.TrimSpace(h[7:]), true
		}
		if strings.EqualFold(h, "Bearer") {
			return "", true
		}
		return h, true
	}
	if _, present := r.Header["X-Api-Key"]; present {
		return strings.TrimSpace(r.Header.Get("X-API-Key")), true
	}
	return "", false
}

func (s *Server) identifyToken(ctx context.Context, token string) (authIdentity, bool) {
	if subtle.ConstantTimeCompare([]byte(token), []byte(s.cfg.APIToken)) == 1 {
		ident := authIdentity{Admin: true, ProjectID: sqlite.DefaultProjectID,
			ConsumerID: sqlite.DefaultProjectID, Name: "master"}
		if !s.applyProjectDefaults(ctx, &ident) {
			return authIdentity{}, false
		}
		return ident, true
	}
	if s.keys != nil {
		if key, found := s.keys.Authenticate(ctx, token); found {
			projectID := key.ProjectID
			if projectID == "" {
				// Pre-migration keys always get a project during migration;
				// this mirrors the legacy mapping as a defensive fallback.
				projectID = "proj_legacy_" + key.ID
			}
			ident := authIdentity{
				ConsumerID:        projectID,
				ProjectID:         projectID,
				KeyID:             key.ID,
				Name:              key.Name,
				DefaultCooldownMS: key.DefaultCooldownMS,
			}
			if !s.applyProjectDefaults(ctx, &ident) {
				return authIdentity{}, false
			}
			return ident, true
		}
	}
	if s.uploadKeys != nil {
		if key, found := s.uploadKeys.Authenticate(ctx, token); found {
			return authIdentity{
				UploadKeyID:   key.ID,
				Name:          key.Name,
				UploadSources: key.SourceIDs,
			}, true
		}
	}
	return authIdentity{}, false
}

// applyProjectDefaults replaces identity defaults with the project's
// configuration. Authorization data must fail closed: when the project row
// cannot be read, the identity is rejected instead of degrading to an
// unrestricted scope. The project is the CD subject, so its defaults
// outrank any key-level fallback.
func (s *Server) applyProjectDefaults(ctx context.Context, ident *authIdentity) bool {
	if ident.ProjectID == "" || s.store == nil {
		return false
	}
	p, err := s.store.GetProject(ctx, ident.ProjectID)
	if err != nil {
		return false
	}
	ident.DefaultCooldownMS = p.Defaults.CooldownMS
	ident.DefaultTimeoutMS = p.Defaults.TimeoutMS
	ident.AllowedSourceIDs = p.AllowedSourceIDs
	ident.SourceScopeRestricted = p.AllowedSourceIDs != nil
	return true
}

// isDownstreamPath limits API-key identities to the delivery surface. Admin
// and source-management endpoints require the master token or a session.
func isDownstreamPath(r *http.Request) bool {
	path := r.URL.Path
	switch r.Method {
	case http.MethodPost:
		if path == "/api/v1/proxies/acquire" || path == "/api/v1/prewarm" {
			return true
		}
		if strings.HasPrefix(path, "/api/v1/leases/") &&
			(strings.HasSuffix(path, "/release") || strings.HasSuffix(path, "/feedback")) {
			return true
		}
		if strings.HasPrefix(path, "/api/v1/jobs/") && strings.HasSuffix(path, "/cancel") {
			return true
		}
	case http.MethodGet:
		// Job reads are additionally narrowed to the owning project inside
		// the handler; other projects get a 404.
		if strings.HasPrefix(path, "/api/v1/jobs/") {
			return true
		}
	case http.MethodDelete:
		if strings.HasPrefix(path, "/api/v1/sessions/") {
			return true
		}
	}
	return false
}

// isUploadKeyPath grants upload credentials exactly two surfaces: pushing an
// import into a source in their allowlist and reading their own batch
// results. The handlers re-check source scoping and ownership, so a path
// match alone never grants access.
func isUploadKeyPath(ident authIdentity, r *http.Request) bool {
	if ident.UploadKeyID == "" {
		return false
	}
	const prefix = "/api/v1/sources/"
	if !strings.HasPrefix(r.URL.Path, prefix) {
		return false
	}
	rest := strings.TrimPrefix(r.URL.Path, prefix)
	switch r.Method {
	case http.MethodPost:
		parts := strings.Split(rest, "/")
		return len(parts) == 2 && parts[0] != "" && parts[1] == "imports"
	case http.MethodGet:
		parts := strings.Split(rest, "/")
		return len(parts) == 3 && parts[0] != "" && parts[1] == "imports" && parts[2] != ""
	}
	return false
}

// loginLimiter bounds password attempts per client address. It is in-memory
// because the deployment invariant is one active process.
type loginLimiter struct {
	mu       sync.Mutex
	attempts map[string][]time.Time
	window   time.Duration
	limit    int
}

func newLoginLimiter(window time.Duration, limit int) *loginLimiter {
	if window <= 0 {
		window = time.Minute
	}
	if limit < 1 {
		limit = 1
	}
	return &loginLimiter{attempts: map[string][]time.Time{}, window: window, limit: limit}
}

func (l *loginLimiter) blocked(key string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	recent := l.attempts[key][:0]
	for _, t := range l.attempts[key] {
		if now.Sub(t) < l.window {
			recent = append(recent, t)
		}
	}
	if recent == nil {
		delete(l.attempts, key)
	} else {
		l.attempts[key] = recent
	}
	return len(recent) >= l.limit
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
