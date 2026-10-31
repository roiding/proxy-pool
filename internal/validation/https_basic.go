package validation

import (
	"context"
	"crypto/hmac"
	crand "crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/net/proxy"
	"proxypool/internal/domain"
)

const ProfileHTTPSBasicV1 = "https_basic_v1"

const (
	FailProxy  = "PROXY"
	FailAuth   = "AUTH"
	FailJudge  = "JUDGE"
	FailBudget = "BUDGET"
)

// Failure classes: PROXY/AUTH are attributed to the candidate and trigger
// cooldown; JUDGE is inconclusive infrastructure trouble; BUDGET is the
// request deadline, not evidence about the proxy.
type Failure struct {
	Kind   string
	Detail string
}

func (f *Failure) Attributed() bool { return f.Kind == FailProxy || f.Kind == FailAuth }

type Limits struct {
	DialTimeout  time.Duration
	ProbeTotal   time.Duration
	AllowPrivate bool
	RootCAs      *x509.CertPool
	// JudgeSecret enables HMAC verification of the judge response. Required
	// for plain-HTTP judges; optional (but recommended) for HTTPS judges.
	JudgeSecret string
}

type judgeResponse struct {
	Nonce     string `json:"nonce"`
	ExitIP    string `json:"exit_ip"`
	Signature string `json:"signature,omitempty"`
}

// JudgeSignature is the HMAC the owned judge computes over nonce|exit_ip. A
// plain-HTTP judge response cannot be trusted without it (a malicious proxy
// could otherwise fabricate a passing response).
func JudgeSignature(secret, nonce, exitIP string) string {
	mac := hmac.New(sha256.New, []byte("proxy-pool/judge/v1\x00"+secret))
	mac.Write([]byte(nonce))
	mac.Write([]byte("|"))
	mac.Write([]byte(exitIP))
	return hex.EncodeToString(mac.Sum(nil))
}

func addrGuard(allowPrivate bool) func(ctx context.Context, network, address string, conn syscall.RawConn) error {
	return func(ctx context.Context, network, address string, _ syscall.RawConn) error {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return fmt.Errorf("bad dial address %q: %w", address, err)
		}
		ip, err := netip.ParseAddr(host)
		if err != nil {
			return fmt.Errorf("dial address %q is not an IP: %w", address, err)
		}
		if !domain.AddrAllowed(ip, allowPrivate) {
			return fmt.Errorf("dial address %q is not allowed by address policy", address)
		}
		return nil
	}
}

// ProbeHTTPSBasic validates one candidate under profile https_basic_v1 by
// fetching the owned judge through a fresh proxy connection with verified TLS.
func ProbeHTTPSBasic(ctx context.Context, conn domain.Connection, judgeURL string, limits Limits, requestID string) (domain.Evidence, *Failure) {
	started := time.Now()
	ev := domain.Evidence{
		AttemptID: domain.NewID("att_"),
		RequestID: requestID,
		Profile:   ProfileHTTPSBasicV1,
		StartedAt: started,
	}
	fail := func(kind, format string, args ...any) (domain.Evidence, *Failure) {
		return ev, &Failure{Kind: kind, Detail: fmt.Sprintf(format, args...)}
	}
	nonce := newNonce()
	transport, tfail := probeTransport(conn, limits)
	if tfail != nil {
		return ev, tfail
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: limits.ProbeTotal}
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, judgeURL, nil)
	if err != nil {
		return fail(FailJudge, "build judge request: %v", err)
	}
	q := req.URL.Query()
	q.Set("nonce", nonce)
	req.URL.RawQuery = q.Encode()
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "proxy-pool-probe/1")
	resp, err := client.Do(req)
	ev.CompletedAt = time.Now()
	ev.LatencyMS = ev.CompletedAt.Sub(started).Milliseconds()
	if err != nil {
		if ctx.Err() != nil {
			return fail(FailBudget, "request deadline reached during probe: %v", ctx.Err())
		}
		msg := err.Error()
		lower := strings.ToLower(msg)
		switch {
		case strings.Contains(lower, "proxyconnect"):
			return fail(FailProxy, "proxy connection failed: %v", err)
		case strings.Contains(lower, "proxy authentication required"):
			// The proxy answered the CONNECT with 407 even though the
			// configured credentials were sent: wrong/expired credentials.
			return fail(FailAuth, "proxy rejected credentials (407): %v", err)
		case strings.Contains(lower, "socks connect") && strings.Contains(lower, "authentication failed"):
			// The SOCKS5 server refused the offered username/password.
			return fail(FailAuth, "socks5 rejected credentials: %v", err)
		case strings.Contains(lower, "socks connect"):
			return fail(FailProxy, "socks5 connection failed: %v", err)
		case isTLSVerifyError(err):
			return fail(FailProxy, "TLS verification through proxy failed: %v", err)
		default:
			// Tunnel established but the judge path failed; cannot attribute
			// this safely to the proxy, so treat it as inconclusive.
			return fail(FailJudge, "probe request failed: %v", err)
		}
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusProxyAuthRequired {
		return fail(FailAuth, "proxy demanded authentication (407)")
	}
	if resp.StatusCode != http.StatusOK {
		return fail(FailJudge, "judge returned status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return fail(FailJudge, "read judge body: %v", err)
	}
	var jr judgeResponse
	if err := json.Unmarshal(body, &jr); err != nil || jr.Nonce == "" {
		return fail(FailProxy, "judge response is not valid probe JSON (possible interception)")
	}
	if jr.Nonce != nonce {
		return fail(FailProxy, "judge nonce mismatch (possible interception)")
	}
	if limits.JudgeSecret != "" {
		expected := JudgeSignature(limits.JudgeSecret, nonce, strings.TrimSpace(jr.ExitIP))
		if subtle.ConstantTimeCompare([]byte(jr.Signature), []byte(expected)) != 1 {
			return fail(FailProxy, "judge signature mismatch (possible interception)")
		}
	}
	ip, err := netip.ParseAddr(strings.TrimSpace(jr.ExitIP))
	if err != nil || !ip.IsValid() {
		return fail(FailJudge, "judge returned invalid exit_ip %q", jr.ExitIP)
	}
	ev.Outcome = "PASS"
	ev.ObservedExitIP = ip.String()
	return ev, nil
}

// probeTransport builds the HTTP transport that reaches the judge through the
// candidate proxy: HTTP proxies are dialed with CONNECT, SOCKS5 proxies speak
// RFC 1928 with optional username/password auth.
func probeTransport(conn domain.Connection, limits Limits) (*http.Transport, *Failure) {
	dialer := &net.Dialer{Timeout: limits.DialTimeout}
	t := &http.Transport{
		TLSClientConfig:       &tls.Config{RootCAs: limits.RootCAs, MinVersion: tls.VersionTLS12},
		TLSHandshakeTimeout:   limits.DialTimeout,
		DisableCompression:    true,
		ForceAttemptHTTP2:     false,
		MaxIdleConns:          1,
		IdleConnTimeout:       time.Millisecond,
		ExpectContinueTimeout: time.Second,
	}
	addr := net.JoinHostPort(conn.Host, strconv.Itoa(conn.Port))
	switch conn.Transport {
	case domain.TransportHTTP:
		proxyURL := &url.URL{Scheme: "http", Host: addr}
		if conn.Username != "" || conn.Password != "" {
			proxyURL.User = url.UserPassword(conn.Username, conn.Password)
		}
		t.Proxy = http.ProxyURL(proxyURL)
		t.DialContext = dialer.DialContext
		return t, nil
	case domain.TransportSOCKS5:
		var auth *proxy.Auth
		if conn.Username != "" || conn.Password != "" {
			auth = &proxy.Auth{User: conn.Username, Password: conn.Password}
		}
		d, err := proxy.SOCKS5("tcp", addr, auth, dialer)
		if err != nil {
			return nil, &Failure{Kind: FailProxy, Detail: fmt.Sprintf("build socks5 dialer: %v", err)}
		}
		cd, ok := d.(proxy.ContextDialer)
		if !ok {
			return nil, &Failure{Kind: FailProxy, Detail: "socks5 dialer lacks context support"}
		}
		t.DialContext = cd.DialContext
		return t, nil
	default:
		return nil, &Failure{Kind: FailProxy, Detail: fmt.Sprintf("transport %q unsupported by probe", conn.Transport)}
	}
}

func isTLSVerifyError(err error) bool {
	var x509Err *x509.CertificateInvalidError
	var hostErr *x509.HostnameError
	var unknownAuth x509.UnknownAuthorityError
	if errors.As(err, &x509Err) || errors.As(err, &hostErr) || errors.As(err, &unknownAuth) {
		return true
	}
	return strings.Contains(err.Error(), "certificate is valid for") ||
		strings.Contains(err.Error(), "certificate signed by unknown authority")
}

func newNonce() string {
	b := make([]byte, 16)
	if _, err := crand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
