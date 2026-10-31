package api

import (
	"encoding/json"
	"net"
	"net/http"
	"net/netip"
	"regexp"
	"strings"
	"sync"
	"time"

	"proxypool/internal/validation"
)

const (
	judgeRatePerMinute = 600
	judgeRateBurst     = 120
)

var judgeNoncePattern = regexp.MustCompile(`^[0-9a-f]{16,64}$`)

// NewJudgeHandler serves the owned judge endpoint. exit_ip comes from the
// actual TCP peer (r.RemoteAddr); trustForwarded switches it to trusted
// forwarded sources for deployments where the judge is only reachable through
// a trusted chain (for example Traefik behind Cloudflare): CF-Connecting-IP is
// preferred (Cloudflare overwrites it, so a proxied client cannot spoof it),
// then the rightmost X-Forwarded-For entry (the one the trusted gateway
// appended), ignoring anything spoofed ahead of it.
func NewJudgeHandler(secret string, trustForwarded bool) http.Handler {
	limiter := newRateLimiter(judgeRatePerMinute, judgeRateBurst)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := ""
		if trustForwarded {
			if cfip := strings.TrimSpace(r.Header.Get("CF-Connecting-IP")); cfip != "" {
				if addr, err := netip.ParseAddr(cfip); err == nil && addr.IsValid() {
					ip = cfip
				}
			}
		}
		if ip == "" && trustForwarded {
			if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
				parts := strings.Split(xff, ",")
				cand := strings.TrimSpace(parts[len(parts)-1])
				if addr, err := netip.ParseAddr(cand); err == nil && addr.IsValid() {
					ip = cand
				}
			}
		}
		if ip == "" {
			host, _, err := net.SplitHostPort(r.RemoteAddr)
			if err != nil || host == "" {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad peer address"})
				return
			}
			ip = host
		}
		if !limiter.allow(ip) {
			writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "rate limited"})
			return
		}
		nonce := r.URL.Query().Get("nonce")
		if !judgeNoncePattern.MatchString(nonce) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad nonce"})
			return
		}
		body := map[string]string{"nonce": nonce, "exit_ip": ip}
		if secret != "" {
			body["signature"] = validation.JudgeSignature(secret, nonce, ip)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(body)
	})
}

type bucket struct {
	tokens float64
	last   time.Time
}

// rateLimiter is a tiny per-key token bucket, enough to keep the public judge
// endpoint from being hammered without pulling in a dependency.
type rateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	perSec  float64
	burst   float64
}

func newRateLimiter(perMinute, burst float64) *rateLimiter {
	return &rateLimiter{
		buckets: map[string]*bucket{},
		perSec:  perMinute / 60,
		burst:   burst,
	}
}

func (l *rateLimiter) allow(key string) bool {
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.buckets) > 16384 {
		for k, b := range l.buckets {
			if now.Sub(b.last) > time.Hour {
				delete(l.buckets, k)
			}
		}
	}
	b, ok := l.buckets[key]
	if !ok {
		l.buckets[key] = &bucket{tokens: l.burst, last: now}
		return true
	}
	b.tokens += now.Sub(b.last).Seconds() * l.perSec
	if b.tokens > l.burst {
		b.tokens = l.burst
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}
