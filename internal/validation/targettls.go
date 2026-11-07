package validation

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/proxy"
	"proxypool/internal/domain"
)

const ProfileTargetTLSV1 = "target_tls_v1"

// SecurityHashV1 derives the per-scope security configuration component of
// evidence keys. Changing it forces a full revalidation under the new rules.
const SecurityHashV1 = "sec-v1"

type ScopeMode string

const (
	ModeBasic ScopeMode = "basic"
	ModeTLS   ScopeMode = "tls"
)

func (m ScopeMode) String() string { return string(m) }

// Failure kinds. TARGET/JUDGE/BUDGET are inconclusive for the proxy itself;
// only PROXY/AUTH are attributed to the candidate connection.
const (
	FailTarget = "TARGET"
)

type ProbeLimits struct {
	// DialTimeout bounds TCP connect, TLS handshake, and proxy tunnel setup.
	DialTimeout time.Duration
	// ProbeTotal bounds the whole probe, including DNS and queue waits.
	ProbeTotal time.Duration
	// AllowPrivate relaxes the guard for loopback test fixtures only; the
	// proxy address uses this policy, tunneled target addresses never do.
	AllowPrivate bool
	// RootCAs overrides system roots. Judge probes may use a dedicated CA;
	// target TLS defaults to system roots unless a test injects one.
	RootCAs *x509.CertPool
	// JudgeSecret enables HMAC verification for plain-HTTP judges.
	JudgeSecret string
	// CAFingerprint is the digest of the PEM the caller used to build
	// RootCAs. x509.CertPool cannot enumerate its certificates, so cache-key
	// callers pass the fingerprint of the source material here.
	CAFingerprint string
}

func DefaultProbeLimits() ProbeLimits {
	return ProbeLimits{DialTimeout: 5 * time.Second, ProbeTotal: 10 * time.Second}
}

type ProbeOptions struct {
	// JudgeURL is required for basic mode and ignored for TLS mode.
	JudgeURL string
	Limits   ProbeLimits
}

// NormalizeOptions parses the downstream validation contract into an exact
// scope. A TLS target must be host:port (IP or hostname); URL forms carry a
// business path and are rejected because target TLS validates host reachability
// and certificate identity, not URL content. Hostname targets resolve exactly
// once at probe time and dial the pinned IP with the original host as SNI.
func NormalizeOptions(mode string, target string) (domain.ValidationScope, *domain.Error) {
	mode = strings.ToLower(strings.TrimSpace(mode))
	switch ScopeMode(mode) {
	case ModeBasic:
		return domain.ValidationScope{Mode: ModeBasic.String(), Profile: ProfileHTTPSBasicV1,
			SecurityHash: SecurityHashV1}, nil
	case ModeTLS:
		target = strings.TrimSpace(target)
		if target == "" {
			return domain.ValidationScope{}, domain.InvalidRequest(
				"validation mode tls requires target host:port")
		}
		if strings.Contains(target, "://") || strings.HasPrefix(target, "/") {
			return domain.ValidationScope{}, domain.InvalidRequest(
				"validation target must be host:port, not a URL: %q", target)
		}
		host, portText, err := net.SplitHostPort(target)
		if err != nil {
			return domain.ValidationScope{}, domain.InvalidRequest(
				"validation target must be host:port: %v", err)
		}
		if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
			host = host[1 : len(host)-1]
		}
		host = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(host), "."))
		if host == "" || len(host) > 253 {
			return domain.ValidationScope{}, domain.InvalidRequest(
				"validation target host is empty or too long: %q", host)
		}
		port, perr := strconv.Atoi(portText)
		if perr != nil || port < 1 || port > 65535 {
			return domain.ValidationScope{}, domain.InvalidRequest(
				"validation target port %q is invalid", portText)
		}
		return domain.ValidationScope{Mode: ModeTLS.String(), Profile: ProfileTargetTLSV1,
			TargetHostPort: net.JoinHostPort(host, strconv.Itoa(port)),
			SecurityHash:   SecurityHashV1}, nil
	default:
		if mode == "" {
			return domain.ValidationScope{Mode: ModeBasic.String(), Profile: ProfileHTTPSBasicV1,
				SecurityHash: SecurityHashV1}, nil
		}
		return domain.ValidationScope{}, domain.UnsupportedProfile(mode)
	}
}

// ProbeSecurityHash extends the scope's structural SecurityHash with the
// runtime validation configuration (judge URL, judge secret, CA material).
// Evidence cache keys must use this digest: two scopes with the same
// mode/target but different judge secrets or CAs must never share evidence.
func ProbeSecurityHash(opts ProbeOptions) string {
	ca := opts.Limits.CAFingerprint
	if ca == "" {
		ca = "system"
	}
	h := sha256.New()
	for _, part := range []string{SecurityHashV1, opts.JudgeURL, opts.Limits.JudgeSecret,
		ca, strconv.FormatBool(opts.Limits.AllowPrivate)} {
		h.Write([]byte(part))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Probe validates one candidate under the given scope: basic fetches the
// signed judge through the proxy; tls tunnels to the target and completes a
// certificate-verified TLS 1.2+ handshake without sending application bytes.
func Probe(ctx context.Context, scope domain.ValidationScope, conn domain.Connection,
	opts ProbeOptions, requestID string) (domain.Evidence, *Failure) {
	switch ScopeMode(scope.Mode) {
	case ModeBasic:
		ev, fail := ProbeHTTPSBasic(ctx, conn, opts.JudgeURL, legacyLimits(opts.Limits), requestID)
		ev.Mode = scope.Mode
		ev.Target = scope.TargetHostPort
		ev.SecurityHash = scope.SecurityHash
		return ev, fail
	case ModeTLS:
		return probeTargetTLS(ctx, scope, conn, opts.Limits, requestID)
	default:
		return domain.Evidence{RequestID: requestID, StartedAt: time.Now()}, &Failure{
			Kind: FailJudge, Detail: fmt.Sprintf("unsupported validation mode %q", scope.Mode)}
	}
}

func legacyLimits(l ProbeLimits) Limits {
	return Limits{
		DialTimeout:  l.DialTimeout,
		ProbeTotal:   l.ProbeTotal,
		AllowPrivate: l.AllowPrivate,
		RootCAs:      l.RootCAs,
		JudgeSecret:  l.JudgeSecret,
	}
}

// probeTargetTLS dials the pinned target through the candidate proxy, then
// performs a TLS 1.2+ handshake with standard certificate and hostname
// verification. Success closes immediately: no HTTP request, no body, no
// redirects. Evidence only passes when the tunnel AND the handshake succeed.
func probeTargetTLS(ctx context.Context, scope domain.ValidationScope, conn domain.Connection,
	limits ProbeLimits, requestID string) (domain.Evidence, *Failure) {
	started := time.Now()
	ev := domain.Evidence{
		AttemptID:    domain.NewID("att_"),
		RequestID:    requestID,
		Profile:      ProfileTargetTLSV1,
		Mode:         scope.Mode,
		Target:       scope.TargetHostPort,
		SecurityHash: scope.SecurityHash,
		StartedAt:    started,
	}
	fail := func(kind, format string, args ...any) (domain.Evidence, *Failure) {
		ev.CompletedAt = time.Now()
		ev.LatencyMS = ev.CompletedAt.Sub(started).Milliseconds()
		return ev, &Failure{Kind: kind, Detail: fmt.Sprintf(format, args...)}
	}
	if limits.ProbeTotal > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, limits.ProbeTotal)
		defer cancel()
	}
	host, portText, err := net.SplitHostPort(scope.TargetHostPort)
	if err != nil {
		return fail(FailTarget, "scope target %q is not host:port", scope.TargetHostPort)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return fail(FailTarget, "scope target %q has an invalid port", scope.TargetHostPort)
	}
	dialer, derr := guardedProxyDialer(ctx, conn, limits)
	if derr != nil {
		return fail(FailProxy, "%v", derr)
	}
	pinned, rerr := resolveTargetPinned(ctx, host, dnsResolveTimeout(limits), limits.AllowPrivate)
	if rerr != nil {
		if ctx.Err() != nil {
			return fail(FailBudget, "request deadline reached during target DNS: %v", ctx.Err())
		}
		return fail(FailTarget, "%v", rerr)
	}
	target := net.JoinHostPort(pinned.String(), strconv.Itoa(port))
	tunnelCtx, cancelTunnel := context.WithTimeout(ctx, limits.DialTimeout)
	nc, err := dialer.DialContext(tunnelCtx, "tcp", target)
	cancelTunnel()
	if err != nil {
		if ctx.Err() != nil {
			return fail(FailBudget, "request deadline reached during tunnel: %v", ctx.Err())
		}
		return fail(tunnelFailureKind(err), "%v", err)
	}
	defer nc.Close()
	if err := nc.SetDeadline(time.Now().Add(limits.DialTimeout)); err != nil {
		return fail(FailProxy, "arm tunnel deadline: %v", err)
	}
	cfg := &tls.Config{
		ServerName: host, MinVersion: tls.VersionTLS12, RootCAs: limits.RootCAs,
	}
	tc := tls.Client(nc, cfg)
	if err := tc.HandshakeContext(ctx); err != nil {
		if ctx.Err() != nil {
			return fail(FailBudget, "request deadline reached during TLS handshake: %v", ctx.Err())
		}
		return fail(FailTarget, "target TLS handshake: %v", err)
	}
	_ = tc.Close()
	_ = nc.Close()
	ev.CompletedAt = time.Now()
	ev.LatencyMS = ev.CompletedAt.Sub(started).Milliseconds()
	ev.Outcome = "PASS"
	return ev, nil
}

func dnsResolveTimeout(limits ProbeLimits) time.Duration {
	if limits.DialTimeout > 0 {
		return limits.DialTimeout
	}
	return 5 * time.Second
}

var errProxyAuthRequired = errors.New("proxy rejected credentials (HTTP 407)")

// tunnelFailureKind classifies proxy-tunnel errors. A CONNECT status from the
// proxy (502/503/504/etc.) is target-scoped: the proxy answered, and its
// upstream reachability is not evidence that this proxy connection is dead.
// Only 407 (AUTH) and failures to speak a valid CONNECT response are
// attributed to the proxy itself.
func tunnelFailureKind(err error) string {
	msg := err.Error()
	switch {
	case errors.Is(err, errProxyAuthRequired):
		return FailAuth
	case strings.Contains(msg, "proxy CONNECT failed for "):
		return FailTarget
	case strings.Contains(msg, "rejected by address policy"):
		return FailTarget
	case strings.Contains(msg, "socks connect") && strings.Contains(msg, "authentication failed"):
		return FailAuth
	case strings.Contains(msg, "connect to proxy"), strings.Contains(msg, "read CONNECT response"),
		strings.Contains(msg, "write CONNECT"):
		return FailProxy
	default:
		return FailProxy
	}
}

// guardedProxyDialer builds the proxy dialer for one probe. Proxy hosts may be
// hostnames: they are resolved exactly once and dialed by the pinned IP under
// the operator address policy. Tunneled target dials always go through the
// pinned-IP guard and can never be non-public or a mapped IPv4.
func guardedProxyDialer(ctx context.Context, conn domain.Connection,
	limits ProbeLimits) (proxy.ContextDialer, error) {
	proxyIP, err := resolveProxyPinned(ctx, conn.Host, dnsResolveTimeout(limits), limits.AllowPrivate)
	if err != nil {
		return nil, err
	}
	proxyAddr := net.JoinHostPort(proxyIP.String(), strconv.Itoa(conn.Port))
	guard := &pinGuardDialer{inner: &net.Dialer{Timeout: limits.DialTimeout},
		allowPrivate: limits.AllowPrivate}
	switch conn.Transport {
	case domain.TransportHTTP:
		proxyURL := &url.URL{Scheme: "http", Host: proxyAddr}
		if conn.Username != "" || conn.Password != "" {
			proxyURL.User = url.UserPassword(conn.Username, conn.Password)
		}
		return &httpConnectDialer{proxyURL: proxyURL, dialer: guard,
			timeout: limits.DialTimeout}, nil
	case domain.TransportSOCKS5:
		var auth *proxy.Auth
		if conn.Username != "" || conn.Password != "" {
			auth = &proxy.Auth{User: conn.Username, Password: conn.Password}
		}
		d, err := proxy.SOCKS5("tcp", proxyAddr, auth, guard)
		if err != nil {
			return nil, err
		}
		cd, ok := d.(proxy.ContextDialer)
		if !ok {
			return nil, fmt.Errorf("socks5 dialer lacks context support")
		}
		return cd, nil
	default:
		return nil, fmt.Errorf("transport %q unsupported by probe", conn.Transport)
	}
}

// resolveProxyPinned resolves the proxy endpoint once. IP literals only need
// the policy check; hostnames resolve through the same one-shot lookup as
// targets, so a rebinding DNS answer cannot change the dialed proxy.
func resolveProxyPinned(ctx context.Context, host string, timeout time.Duration,
	allowPrivate bool) (netip.Addr, error) {
	host = strings.TrimSuffix(strings.TrimPrefix(strings.ToLower(strings.TrimSpace(host)),
		"["), "]")
	if host == "" {
		return netip.Addr{}, fmt.Errorf("proxy host is empty")
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		if !domain.AddrAllowed(ip.Unmap(), allowPrivate) {
			return netip.Addr{}, fmt.Errorf("proxy address %s rejected by address policy", host)
		}
		return ip.Unmap(), nil
	}
	resCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	addrs, err := lookupHost(resCtx, host)
	if err != nil {
		return netip.Addr{}, fmt.Errorf("resolve proxy %q: %w", host, err)
	}
	if ip, ok := domain.SelectPinnedAddress(addrs, allowPrivate, false); ok {
		return ip, nil
	}
	return netip.Addr{}, fmt.Errorf("proxy %q resolved to no allowed address", host)
}

// pinGuardDialer rejects any dial target that is not a literal IP allowed by
// the address policy. Mapped IPv4 forms are always suspect and rejected; the
// dialers above only ever pass pinned addresses, so a hostname slipping
// through is a programming error and must fail closed.
type pinGuardDialer struct {
	inner        *net.Dialer
	allowPrivate bool
}

func (d *pinGuardDialer) Dial(network, address string) (net.Conn, error) {
	return d.DialContext(context.Background(), network, address)
}

func (d *pinGuardDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("dial address %q: %w", address, err)
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return nil, fmt.Errorf("dial address %q is not a pinned IP: %w", address, err)
	}
	if ip.Is4In6() || !domain.AddrAllowed(ip.Unmap(), d.allowPrivate) {
		return nil, fmt.Errorf("dial address %s rejected by address policy", address)
	}
	return d.inner.DialContext(ctx, network, address)
}

// resolveTargetPinned resolves a target hostname exactly once and returns the
// literal IP the probe will dial. IP literals pass through the address policy
// immediately. The first allowed address wins; resolutions that only yield
// disallowed or mapped addresses fail closed instead of dialing anything else.
func resolveTargetPinned(ctx context.Context, host string, timeout time.Duration,
	allowPrivate bool) (netip.Addr, error) {
	if ip, err := netip.ParseAddr(host); err == nil {
		unmapped := ip.Unmap()
		if !domain.AddrAllowed(unmapped, allowPrivate) {
			return netip.Addr{}, fmt.Errorf("target %q rejected by address policy", host)
		}
		return unmapped, nil
	}
	resCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	addrs, err := lookupHost(resCtx, host)
	if err != nil {
		return netip.Addr{}, fmt.Errorf("resolve target %q: %w", host, err)
	}
	if ip, ok := domain.SelectPinnedAddress(addrs, allowPrivate, true); ok {
		return ip, nil
	}
	return netip.Addr{}, fmt.Errorf("target %q resolved to no allowed address", host)
}

// lookupHost is the one-shot DNS hook. Production uses the pure-Go resolver
// with no environment fallbacks; tests substitute a deterministic mapping.
var lookupHost = func(ctx context.Context, host string) ([]string, error) {
	return (&net.Resolver{PreferGo: true}).LookupHost(ctx, host)
}

// maxProxyResponseHeader bounds the CONNECT response header block. A hostile
// proxy must not be able to exhaust memory with an endless header.
const maxProxyResponseHeader = 32 << 10

// ctxDialer is the minimal dialing surface the CONNECT dialer needs.
type ctxDialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

// httpConnectDialer speaks HTTP CONNECT with Proxy-Authorization credentials
// through the guarded dialer. It never consults environment proxy settings.
type httpConnectDialer struct {
	proxyURL *url.URL
	dialer   ctxDialer
	timeout  time.Duration
}

func (d *httpConnectDialer) Dial(network, address string) (net.Conn, error) {
	return d.DialContext(context.Background(), network, address)
}

func (d *httpConnectDialer) DialContext(ctx context.Context, _, address string) (net.Conn, error) {
	nc, err := d.dialer.DialContext(ctx, "tcp", d.proxyURL.Host)
	if err != nil {
		return nil, fmt.Errorf("connect to proxy %s: %w", d.proxyURL.Host, err)
	}
	// Cancel watching: closing the socket unblocks the response read even
	// though http.ReadResponse itself is not context-aware.
	stop := context.AfterFunc(ctx, func() { _ = nc.Close() })
	if deadline, ok := ctx.Deadline(); ok {
		_ = nc.SetDeadline(deadline)
	} else if d.timeout > 0 {
		_ = nc.SetDeadline(time.Now().Add(d.timeout))
	}
	cleanup := func() { stop(); _ = nc.Close() }
	req := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Opaque: address},
		Host:   address,
		Header: make(http.Header),
	}
	if user := d.proxyURL.User; user != nil {
		password, _ := user.Password()
		// Proxy credentials go in Proxy-Authorization, not Authorization.
		req.Header.Set("Proxy-Authorization", "Basic "+
			base64.StdEncoding.EncodeToString([]byte(user.Username()+":"+password)))
	}
	if err := req.Write(nc); err != nil {
		cleanup()
		return nil, fmt.Errorf("write CONNECT %s: %w", address, err)
	}
	lr := &io.LimitedReader{R: nc, N: maxProxyResponseHeader}
	br := bufio.NewReader(lr)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		cleanup()
		if ctx.Err() != nil {
			return nil, fmt.Errorf("CONNECT %s canceled: %w", address, ctx.Err())
		}
		if lr.N <= 0 {
			return nil, fmt.Errorf("proxy CONNECT response headers exceed %d bytes",
				maxProxyResponseHeader)
		}
		return nil, fmt.Errorf("read CONNECT response for %s: %w", address, err)
	}
	if resp.StatusCode != http.StatusOK {
		// Close without draining any claimed body.
		cleanup()
		if resp.StatusCode == http.StatusProxyAuthRequired {
			return nil, errProxyAuthRequired
		}
		return nil, fmt.Errorf("proxy CONNECT failed for %s: HTTP %d", address, resp.StatusCode)
	}
	if br.Buffered() > 0 {
		cleanup()
		return nil, fmt.Errorf("proxy CONNECT for %s returned unsolicited body bytes", address)
	}
	// A CONNECT tunnel has no body. A response claiming Content-Length or
	// Transfer-Encoding is hostile (unbounded-drain bait) and is rejected
	// without reading anything further.
	if resp.ContentLength > 0 || resp.Header.Get("Content-Length") != "" ||
		len(resp.TransferEncoding) > 0 {
		cleanup()
		return nil, fmt.Errorf(
			"proxy CONNECT for %s response must not carry a body (malicious)", address)
	}
	stop() // disarm the cancel watcher: tunnel ownership passes to the caller
	_ = nc.SetDeadline(time.Time{})
	return nc, nil
}
