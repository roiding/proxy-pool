// Package sourcehttp is the common pinned-DNS HTTP boundary for source adapters.
// It never uses environment proxies or returns raw URLs/bodies in errors.
package sourcehttp

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"proxypool/internal/domain"
	"proxypool/internal/integrations/custom"
)

const (
	MaxBodyBytes   = 4 << 20
	MaxRedirects   = 5
	DefaultTimeout = 15 * time.Second
)

type Options struct {
	Timeout      time.Duration
	MaxBytes     int64
	MaxRedirects int
	AllowPrivate bool
	NoRedirects  bool
	// ResolveHost is a test seam, never source-configurable. All answers
	// remain subject to address policy before a pinned dial.
	ResolveHost func(context.Context, string) ([]string, error)
}
type Error struct {
	Code           string
	Status         int
	RetryAfter     time.Duration
	Detail         string
	UpstreamStatus int
	cause          error
}

func (e *Error) Error() string { return e.Code + ": " + e.Detail }
func (e *Error) Unwrap() error { return e.cause }
func failure(code string, status int, detail string) *Error {
	return &Error{Code: code, Status: status, Detail: detail}
}
func (o Options) normalized() (Options, *Error) {
	if o.Timeout < 0 || o.Timeout > 2*time.Minute || o.MaxBytes < 0 || o.MaxBytes > MaxBodyBytes || o.MaxRedirects < 0 || o.MaxRedirects > MaxRedirects {
		return o, failure("SOURCE_CONFIG_ERROR", 400, "source HTTP limits are outside their supported bounds")
	}
	if o.Timeout == 0 {
		o.Timeout = DefaultTimeout
	}
	if o.MaxBytes == 0 {
		o.MaxBytes = MaxBodyBytes
	}
	if o.MaxRedirects == 0 {
		o.MaxRedirects = MaxRedirects
	}
	if o.ResolveHost == nil {
		o.ResolveHost = func(ctx context.Context, host string) ([]string, error) {
			return (&net.Resolver{PreferGo: true}).LookupHost(ctx, host)
		}
	}
	return o, nil
}

// NewHTTPClient preserves non-200 status/headers for vendor adapters. A 429
// is not erased by eagerly reading a slow or oversized error body. Successful
// responses must finish inside the total deadline and decoded byte cap.
func NewHTTPClient(options Options) *http.Client {
	opts, err := options.normalized()
	return &http.Client{Transport: &roundTripper{opts: opts, configErr: err}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

type roundTripper struct {
	opts      Options
	configErr *Error
	// Package-local test trust seam. NewHTTPClient never overrides OS roots.
	rootCAs *x509.CertPool
}

func (t *roundTripper) RoundTrip(original *http.Request) (*http.Response, error) {
	if t.configErr != nil {
		return nil, t.configErr
	}
	ctx, cancel := context.WithTimeout(original.Context(), t.opts.Timeout)
	current := original.Clone(ctx)
	current.Header = original.Header.Clone()
	for hop := 0; ; hop++ {
		if err := validateURL(current.URL); err != nil {
			cancel()
			return nil, err
		}
		if current.Host != "" && !strings.EqualFold(current.Host, current.URL.Host) {
			cancel()
			return nil, failure("SOURCE_INVALID_URL", 400, "source Host header does not match the request origin")
		}
		resp, transport, err := t.attempt(ctx, current)
		if err != nil {
			cancel()
			return nil, err
		}
		if !isRedirect(resp.StatusCode) || t.opts.NoRedirects {
			resp.Request = current
			encoding := resp.Header.Get("Content-Encoding")
			if current.Method == http.MethodHead || resp.StatusCode == http.StatusNotModified || resp.StatusCode == http.StatusNoContent {
				encoding = ""
			}
			body := newBoundedBody(resp.Body, transport, cancel, ctx, t.opts.MaxBytes, encoding)
			if resp.StatusCode != http.StatusOK {
				resp.Body = body
				return resp, nil
			}
			data, readErr := io.ReadAll(body)
			_ = body.Close()
			if readErr != nil {
				return nil, readErr
			}
			resp.Body = io.NopCloser(bytes.NewReader(data))
			resp.ContentLength = int64(len(data))
			if resp.Header.Get("Content-Encoding") != "" {
				resp.Uncompressed = true
				resp.Header.Del("Content-Encoding")
				resp.Header.Del("Content-Length")
			}
			return resp, nil
		}
		location := resp.Header.Get("Location")
		_ = resp.Body.Close()
		transport.CloseIdleConnections()
		// Do not silently rewrite POST on 301/302/303 or lose its body on 307/308.
		if current.Method != http.MethodGet && current.Method != http.MethodHead || current.Body != nil && current.Body != http.NoBody {
			cancel()
			return nil, failure("SOURCE_REDIRECT_BLOCKED", 502, "redirects of source requests with a method/body are not supported")
		}
		if hop >= t.opts.MaxRedirects {
			cancel()
			return nil, failure("SOURCE_REDIRECT_LIMIT", 502, "source redirect chain exceeds its hop limit")
		}
		if location == "" {
			cancel()
			return nil, failure("SOURCE_REDIRECT_INVALID", 502, "source redirect has no Location")
		}
		next, err := current.URL.Parse(location)
		if err != nil {
			cancel()
			return nil, failure("SOURCE_REDIRECT_INVALID", 502, "source redirect target is invalid")
		}
		if err := validateURL(next); err != nil {
			cancel()
			return nil, err
		}
		if current.URL.Scheme == "https" && next.Scheme != "https" {
			cancel()
			return nil, failure("SOURCE_REDIRECT_BLOCKED", 502, "source HTTPS redirect downgrade is not permitted")
		}
		request := current.Clone(ctx)
		request.URL = next
		request.Host = ""
		request.Header = current.Header.Clone()
		if origin(current.URL) != origin(next) {
			// An allowlist also removes unknown vendor-key headers, Cookie,
			// Proxy-Authorization, validators and Referer on origin changes.
			request.Header = make(http.Header)
			for _, name := range []string{"Accept", "Accept-Encoding", "User-Agent"} {
				if values := current.Header.Values(name); len(values) > 0 {
					request.Header[name] = append([]string(nil), values...)
				}
			}
		}
		current = request
	}
}
func validateURL(u *url.URL) *Error {
	if u == nil || u.Scheme != "http" && u.Scheme != "https" {
		return failure("SOURCE_INVALID_URL", 400, "source URL scheme must be http or https")
	}
	if u.Host == "" || u.Opaque != "" || u.User != nil || u.Fragment != "" {
		return failure("SOURCE_INVALID_URL", 400, "source URL must have a host and no credentials or fragment")
	}
	if _, err := custom.NormalizeHost(u.Hostname(), true); err != nil {
		return failure("SOURCE_INVALID_URL", 400, "source URL host is invalid")
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return failure("SOURCE_INVALID_URL", 400, "source URL port is invalid")
		}
	} else if strings.HasSuffix(u.Host, ":") {
		return failure("SOURCE_INVALID_URL", 400, "source URL port is empty")
	}
	return nil
}
func origin(u *url.URL) string {
	return u.Scheme + "://" + strings.ToLower(strings.TrimSuffix(u.Hostname(), ".")) + ":" + portOf(u)
}
func portOf(u *url.URL) string {
	if p := u.Port(); p != "" {
		return p
	}
	if u.Scheme == "https" {
		return "443"
	}
	return "80"
}
func isRedirect(s int) bool { return s == 301 || s == 302 || s == 303 || s == 307 || s == 308 }
func (t *roundTripper) resolvePinned(ctx context.Context, host string) (netip.Addr, error) {
	if ip, err := netip.ParseAddr(host); err == nil {
		if ip.Zone() != "" || !domain.AddrAllowed(ip.Unmap(), t.opts.AllowPrivate) {
			return netip.Addr{}, failure("SOURCE_ADDRESS_REJECTED", 502, "source address rejected by address policy")
		}
		return ip.Unmap(), nil
	}
	answers, err := t.opts.ResolveHost(ctx, host)
	if err != nil {
		return netip.Addr{}, requestError(ctx, err)
	}
	if ip, ok := domain.SelectPinnedAddress(answers, t.opts.AllowPrivate, false); ok {
		return ip, nil
	}
	return netip.Addr{}, failure("SOURCE_ADDRESS_REJECTED", 502, "source host resolved to no allowed address")
}
func (t *roundTripper) attempt(ctx context.Context, request *http.Request) (*http.Response, *http.Transport, error) {
	ip, err := t.resolvePinned(ctx, request.URL.Hostname())
	if err != nil {
		return nil, nil, err
	}
	port := portOf(request.URL)
	dialer := &net.Dialer{Timeout: t.opts.Timeout}
	transport := &http.Transport{
		Proxy: nil,
		DialContext: func(ctx context.Context, _ string, address string) (net.Conn, error) {
			_, dialPort, err := net.SplitHostPort(address)
			if err != nil || dialPort != port {
				return nil, failure("SOURCE_INVALID_URL", 400, "source dial port does not match its origin")
			}
			return dialer.DialContext(ctx, "tcp", net.JoinHostPort(ip.String(), port))
		},
		TLSClientConfig: &tls.Config{ServerName: request.URL.Hostname(), RootCAs: t.rootCAs, MinVersion: tls.VersionTLS12}, TLSHandshakeTimeout: t.opts.Timeout,
		DisableCompression: true, ForceAttemptHTTP2: false, MaxIdleConns: 1, IdleConnTimeout: time.Second,
	}
	if request.Header.Get("Accept-Encoding") == "" {
		request.Header.Set("Accept-Encoding", "gzip")
	}
	resp, err := transport.RoundTrip(request)
	if err != nil {
		transport.CloseIdleConnections()
		return nil, nil, requestError(ctx, err)
	}
	return resp, transport, nil
}
func requestError(ctx context.Context, err error) *Error {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return &Error{Code: "SOURCE_TIMEOUT", Status: 504, Detail: "source request timed out", cause: context.DeadlineExceeded}
	}
	if errors.Is(err, context.Canceled) || ctx.Err() != nil {
		return &Error{Code: "REQUEST_CANCELED", Status: 499, Detail: "source request was cancelled", cause: context.Canceled}
	}
	var ferr *Error
	if errors.As(err, &ferr) {
		return ferr
	}
	var verification *tls.CertificateVerificationError
	if errors.As(err, &verification) {
		return failure("SOURCE_TLS_ERROR", 502, "source TLS certificate verification failed")
	}
	var nerr net.Error
	if errors.As(err, &nerr) && nerr.Timeout() {
		return &Error{Code: "SOURCE_TIMEOUT", Status: 504, Detail: "source request timed out", cause: context.DeadlineExceeded}
	}
	return failure("SOURCE_NETWORK_ERROR", 503, "source request or response could not be read")
}

// Error bodies are decoded lazily: status is available even if their body
// never arrives. Close/read failure releases the hop and total deadline.
type boundedBody struct {
	body        io.ReadCloser
	reader      io.Reader
	encoding    string
	remaining   int64
	initialized bool
	terminal    error
	ctx         context.Context
	transport   *http.Transport
	cancel      context.CancelFunc
	once        sync.Once
}

func newBoundedBody(body io.ReadCloser, transport *http.Transport, cancel context.CancelFunc, ctx context.Context, limit int64, encoding string) *boundedBody {
	return &boundedBody{body: body, transport: transport, cancel: cancel, ctx: ctx, remaining: limit, encoding: strings.ToLower(strings.TrimSpace(encoding))}
}
func (b *boundedBody) Read(p []byte) (int, error) {
	if b.terminal != nil {
		return 0, b.terminal
	}
	if !b.initialized {
		b.initialized = true
		b.reader = b.body
		switch b.encoding {
		case "", "identity":
		case "gzip":
			reader, err := gzip.NewReader(b.body)
			if err != nil {
				if b.ctx.Err() != nil {
					err = requestError(b.ctx, err)
				} else {
					err = failure("SOURCE_FORMAT_ERROR", 502, "source gzip response is invalid")
				}
				b.terminal = err
				_ = b.Close()
				return 0, err
			}
			b.reader = reader
		default:
			b.terminal = failure("SOURCE_FORMAT_ERROR", 502, "source content encoding is not supported")
			_ = b.Close()
			return 0, b.terminal
		}
	}
	if int64(len(p)) > b.remaining+1 {
		p = p[:b.remaining+1]
	}
	n, err := b.reader.Read(p)
	b.remaining -= int64(n)
	if b.remaining < 0 {
		b.terminal = failure("SOURCE_RESPONSE_TOO_LARGE", 502, "source body exceeds its decoded byte limit")
		_ = b.Close()
		return 0, b.terminal
	}
	if err != nil {
		// A clean EOF means the transfer framing completed, so a local
		// timer that expires between the last byte and EOF must not turn
		// a fully received body into a timeout. Short Content-Length and
		// missing chunk terminators surface as ErrUnexpectedEOF instead.
		if err != io.EOF {
			err = requestError(b.ctx, err)
		}
		b.terminal = err
		_ = b.Close()
	}
	return n, err
}
func (b *boundedBody) Close() error {
	var err error
	b.once.Do(func() { err = b.body.Close(); b.transport.CloseIdleConnections(); b.cancel() })
	return err
}

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

func Download(ctx context.Context, rawURL string, cond Conditional, opts Options) (Result, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSpace(rawURL), nil)
	if err != nil {
		return Result{}, failure("SOURCE_INVALID_URL", 400, "source request URL is invalid")
	}
	req.Header.Set("User-Agent", "proxy-pool-source/1")
	req.Header.Set("Accept", "text/plain, */*")
	if cond.ETag != "" {
		req.Header.Set("If-None-Match", cond.ETag)
	}
	if cond.IfModifiedSince != "" {
		req.Header.Set("If-Modified-Since", cond.IfModifiedSince)
	}
	resp, err := NewHTTPClient(opts).Do(req)
	if err != nil {
		return Result{}, requestError(ctx, err)
	}
	defer resp.Body.Close()
	result := Result{Status: resp.StatusCode, ETag: resp.Header.Get("ETag"), LastModified: resp.Header.Get("Last-Modified"), FinalURL: resp.Request.URL.String()}
	if resp.StatusCode == http.StatusNotModified {
		result.NotModified = true
		return result, nil
	}
	if resp.StatusCode != http.StatusOK {
		err := &Error{Code: "SOURCE_UPSTREAM_FAILED", Status: 502, Detail: "source returned HTTP " + strconv.Itoa(resp.StatusCode), UpstreamStatus: resp.StatusCode}
		switch resp.StatusCode {
		case 401, 403:
			err.Code = "SOURCE_AUTH_FAILED"
		case 429:
			err.Code = "SOURCE_RATE_LIMITED"
			err.Status = 429
		case 301, 302, 303, 307, 308:
			err.Code = "SOURCE_REDIRECT_BLOCKED"
		}
		err.RetryAfter = ParseRetryAfter(resp.Header.Get("Retry-After"), time.Now())
		return Result{}, err
	}
	result.Body, err = io.ReadAll(resp.Body)
	if err != nil {
		return Result{}, requestError(ctx, err)
	}
	return result, nil
}
func ParseRetryAfter(raw string, now time.Time) time.Duration {
	raw = strings.TrimSpace(raw)
	if seconds, err := strconv.ParseInt(raw, 10, 64); err == nil && seconds > 0 {
		return time.Duration(min(seconds, int64(86400))) * time.Second
	}
	if at, err := http.ParseTime(raw); err == nil && at.After(now) {
		return min(at.Sub(now), 24*time.Hour)
	}
	return 0
}
