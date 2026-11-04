// Package staticdl downloads generic static proxy-list URLs with hard
// security bounds: http/https only, no environment proxy, one-shot DNS
// pinning per hop, per-hop redirect revalidation, response size caps, and
// conditional-request support (ETag / If-Modified-Since).
package staticdl

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"

	"proxypool/internal/domain"
)

const (
	// MaxBodyBytes caps one list body; static lists are small text files.
	MaxBodyBytes = 4 << 20
	// MaxRedirects bounds the redirect chain; every hop is revalidated.
	MaxRedirects = 5
	// DefaultTimeout bounds one attempt including dial and body read.
	DefaultTimeout = 15 * time.Second
	userAgent      = "proxy-pool-static/1"
)

type Options struct {
	Timeout time.Duration
	// MaxBytes caps the response body; 0 means MaxBodyBytes.
	MaxBytes int64
	// MaxRedirects caps hops; 0 means MaxRedirects.
	MaxRedirects int
	// AllowPrivate relaxes the address policy for loopback fixtures only.
	AllowPrivate bool
}

// Conditional carries the previous response's validators for a revalidation
// request. An empty struct fetches unconditionally.
type Conditional struct {
	ETag            string
	IfModifiedSince string
}

type Result struct {
	Body         []byte
	Status       int
	ETag         string
	LastModified string
	FinalURL     string
	NotModified  bool
}

func (o Options) timeout() time.Duration {
	if o.Timeout > 0 {
		return o.Timeout
	}
	return DefaultTimeout
}

func (o Options) maxBytes() int64 {
	if o.MaxBytes > 0 {
		return o.MaxBytes
	}
	return MaxBodyBytes
}

func (o Options) maxRedirects() int {
	if o.MaxRedirects > 0 {
		return o.MaxRedirects
	}
	return MaxRedirects
}

// Download fetches one list URL through the pinned-dial client. Redirects
// are followed manually so every hop revalidates scheme and re-resolves DNS.
func Download(ctx context.Context, rawURL string, cond Conditional, opts Options) (Result, error) {
	if opts.MaxBytes < 0 || opts.MaxBytes > MaxBodyBytes {
		return Result{}, fmt.Errorf("max_bytes %d outside 0..%d", opts.MaxBytes, MaxBodyBytes)
	}
	current, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return Result{}, fmt.Errorf("parse list URL: %w", err)
	}
	for hop := 0; ; hop++ {
		if hop > opts.maxRedirects() {
			return Result{}, fmt.Errorf("redirect chain exceeds %d hops", opts.maxRedirects())
		}
		if err := validateHopURL(current); err != nil {
			return Result{}, err
		}
		result, redirect, err := attempt(ctx, current, cond, opts)
		if err != nil {
			return Result{}, err
		}
		if redirect == "" {
			result.FinalURL = current.String()
			return result, nil
		}
		next, err := current.Parse(redirect)
		if err != nil {
			return Result{}, fmt.Errorf("resolve redirect %q: %w", redirect, err)
		}
		current = next
	}
}

func validateHopURL(u *url.URL) error {
	if u == nil || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("list URL scheme must be http or https, got %q", schemeOf(u))
	}
	if u.Host == "" {
		return fmt.Errorf("list URL has no host")
	}
	if u.User != nil {
		return fmt.Errorf("list URL must not carry credentials")
	}
	return nil
}

func schemeOf(u *url.URL) string {
	if u == nil {
		return ""
	}
	return u.Scheme
}

// attempt performs one validated request. A 3xx response returns its
// Location as the redirect target without reading the body.
func attempt(ctx context.Context, u *url.URL, cond Conditional, opts Options) (Result, string, error) {
	ip, err := resolvePinned(ctx, u.Hostname(), opts.timeout(), opts.AllowPrivate)
	if err != nil {
		return Result{}, "", err
	}
	port := u.Port()
	if port == "" {
		if u.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	dialer := &net.Dialer{Timeout: opts.timeout()}
	pinned := ip.String()
	transport := &http.Transport{
		Proxy: nil, // never consult environment proxy settings
		DialContext: func(ctx context.Context, _, address string) (net.Conn, error) {
			// The dialer always replaces the resolved host with the pinned
			// IP; the port must be the hop's own port.
			_, dialPort, err := net.SplitHostPort(address)
			if err != nil {
				return nil, err
			}
			return dialer.DialContext(ctx, "tcp", net.JoinHostPort(pinned, dialPort))
		},
		TLSClientConfig: &tls.Config{
			ServerName: u.Hostname(), MinVersion: tls.VersionTLS12,
		},
		TLSHandshakeTimeout:   opts.timeout(),
		DisableCompression:    true,
		ForceAttemptHTTP2:     false,
		MaxIdleConns:          1,
		IdleConnTimeout:       time.Millisecond,
		ExpectContinueTimeout: time.Second,
	}
	client := &http.Client{Transport: transport, Timeout: opts.timeout()}
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return Result{}, "", fmt.Errorf("build list request: %w", err)
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "text/plain, */*")
	if cond.ETag != "" {
		req.Header.Set("If-None-Match", cond.ETag)
	}
	if cond.IfModifiedSince != "" {
		req.Header.Set("If-Modified-Since", cond.IfModifiedSince)
	}
	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return Result{}, "", fmt.Errorf("list download canceled: %w", ctx.Err())
		}
		return Result{}, "", fmt.Errorf("list request %s: %w", u.Host, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode < 400 &&
		resp.StatusCode != http.StatusNotModified {
		location := resp.Header.Get("Location")
		if location == "" {
			return Result{}, "", fmt.Errorf("list server %s returned redirect %d without Location",
				u.Host, resp.StatusCode)
		}
		return Result{}, location, nil
	}
	result := Result{Status: resp.StatusCode, ETag: resp.Header.Get("ETag"),
		LastModified: resp.Header.Get("Last-Modified")}
	if resp.StatusCode == http.StatusNotModified {
		result.NotModified = true
		return result, "", nil
	}
	if resp.StatusCode != http.StatusOK {
		return Result{}, "", fmt.Errorf("list server %s returned HTTP %d", u.Host, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, opts.maxBytes()+1))
	if err != nil {
		if ctx.Err() != nil {
			return Result{}, "", fmt.Errorf("list download canceled: %w", ctx.Err())
		}
		return Result{}, "", fmt.Errorf("read list body: %w", err)
	}
	if int64(len(body)) > opts.maxBytes() {
		return Result{}, "", fmt.Errorf("list body exceeds %d bytes", opts.maxBytes())
	}
	result.Body = body
	return result, "", nil
}

// resolvePinned resolves the hop host exactly once and returns the literal
// IP under the address policy. Mapped IPv4 answers are always skipped.
func resolvePinned(ctx context.Context, host string, timeout time.Duration,
	allowPrivate bool) (netip.Addr, error) {
	host = strings.TrimSuffix(strings.TrimPrefix(strings.ToLower(strings.TrimSpace(host)),
		"["), "]")
	if host == "" {
		return netip.Addr{}, fmt.Errorf("list host is empty")
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		if !domain.AddrAllowed(ip.Unmap(), allowPrivate) {
			return netip.Addr{}, fmt.Errorf("list address %s rejected by address policy", host)
		}
		return ip.Unmap(), nil
	}
	resCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	addrs, err := resolveHost(resCtx, host)
	if err != nil {
		return netip.Addr{}, fmt.Errorf("resolve list host %q: %w", host, err)
	}
	for _, a := range addrs {
		ip, err := netip.ParseAddr(strings.TrimSpace(a))
		if err != nil || ip.Is4In6() {
			continue
		}
		if domain.AddrAllowed(ip, allowPrivate) {
			return ip, nil
		}
	}
	return netip.Addr{}, fmt.Errorf("list host %q resolved to no allowed address", host)
}

// resolveHost is the one-shot DNS hook; tests substitute deterministic maps.
var resolveHost = func(ctx context.Context, host string) ([]string, error) {
	return (&net.Resolver{PreferGo: true}).LookupHost(ctx, host)
}
