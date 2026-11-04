package validation

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"proxypool/internal/domain"
)

func genCert(t *testing.T, dnsNames []string, ips []net.IP) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "probe-cert"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              dnsNames,
		IPAddresses:           ips,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(certPEM)
	return cert, pool
}

type tlsTarget struct {
	addr     string
	sni      chan string
	appBytes atomic.Int64
}

func startTLSTarget(t *testing.T, cert tls.Certificate) *tlsTarget {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	target := &tlsTarget{sni: make(chan string, 4)}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
		GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			select {
			case target.sni <- hello.ServerName:
			default:
			}
			return nil, nil
		},
	}
	tlsLn := tls.NewListener(ln, cfg)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := tlsLn.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
				tc := conn.(*tls.Conn)
				if err := tc.Handshake(); err != nil {
					return
				}
				buf := make([]byte, 2048)
				n, _ := tc.Read(buf)
				target.appBytes.Add(int64(n))
			}()
		}
	}()
	t.Cleanup(func() { _ = tlsLn.Close(); <-done })
	target.addr = ln.Addr().String()
	return target
}

// startConnectProxy is a minimal HTTP CONNECT proxy. behavior: ok, refuse,
// authfail. Correct proxy credentials are user/pass when both are set.
func startConnectProxy(t *testing.T, user, pass, behavior string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
				req, err := http.ReadRequest(bufio.NewReader(conn))
				if err != nil {
					return
				}
				if req.Method != http.MethodConnect {
					return
				}
				authOK := user == "" && req.Header.Get("Proxy-Authorization") == "" ||
					req.Header.Get("Proxy-Authorization") ==
						"Basic "+base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
				if !authOK || behavior == "authfail" {
					_, _ = io.WriteString(conn, "HTTP/1.1 407 Proxy Authentication Required\r\n\r\n")
					return
				}
				if behavior == "refuse" {
					_, _ = io.WriteString(conn, "HTTP/1.1 403 Forbidden\r\n\r\n")
					return
				}
				switch behavior {
				case "silent":
					// Accept the CONNECT request and never answer.
					_, _ = conn.Read(make([]byte, 1))
					return
				case "headerflood":
					_, _ = io.WriteString(conn, "HTTP/1.1 200 Connection Established\r\n")
					for i := 0; i < 4096; i++ {
						_, _ = io.WriteString(conn, "X-Flood: "+strings.Repeat("a", 64)+"\r\n")
					}
					_, _ = io.WriteString(conn, "\r\n")
					return
				case "clsniff":
					_, _ = io.WriteString(conn,
						"HTTP/1.1 200 Connection Established\r\nContent-Length: 999999999\r\n\r\n")
					_, _ = conn.Write(make([]byte, 64<<10))
					time.Sleep(2 * time.Second)
					return
				}
				_, _ = io.WriteString(conn, "HTTP/1.1 200 Connection Established\r\n\r\n")
				_ = conn.SetDeadline(time.Time{})
				upstream, err := net.Dial("tcp", req.Host)
				if err != nil {
					return
				}
				defer upstream.Close()
				_ = upstream.SetDeadline(time.Now().Add(3 * time.Second))
				go func() { _, _ = io.Copy(upstream, conn); upstream.Close() }()
				_, _ = io.Copy(conn, upstream)
			}()
		}
	}()
	t.Cleanup(func() { _ = ln.Close(); <-done })
	return ln.Addr().String()
}

// startSocks5Proxy implements RFC 1928 greeting, optional RFC 1929 auth, and
// CONNECT relaying. behavior: ok or authfail.
func startSocks5Proxy(t *testing.T, user, pass, behavior string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
				head := make([]byte, 2)
				if _, err := io.ReadFull(conn, head); err != nil || head[0] != 5 {
					return
				}
				methods := make([]byte, head[1])
				if _, err := io.ReadFull(conn, methods); err != nil {
					return
				}
				needAuth := user != ""
				if _, err := conn.Write([]byte{5, map[bool]byte{true: 2, false: 0}[needAuth]}); err != nil {
					return
				}
				if needAuth {
					ver := make([]byte, 1)
					if _, err := io.ReadFull(conn, ver); err != nil || ver[0] != 1 {
						return
					}
					ulen := make([]byte, 1)
					if _, err := io.ReadFull(conn, ulen); err != nil {
						return
					}
					u := make([]byte, ulen[0])
					if _, err := io.ReadFull(conn, u); err != nil {
						return
					}
					plen := make([]byte, 1)
					if _, err := io.ReadFull(conn, plen); err != nil {
						return
					}
					p := make([]byte, plen[0])
					if _, err := io.ReadFull(conn, p); err != nil {
						return
					}
					if behavior == "authfail" || string(u) != user || string(p) != pass {
						_, _ = conn.Write([]byte{1, 0xff})
						return
					}
					if _, err := conn.Write([]byte{1, 0}); err != nil {
						return
					}
				}
				req := make([]byte, 4)
				if _, err := io.ReadFull(conn, req); err != nil || req[1] != 1 {
					return
				}
				var host string
				switch req[3] {
				case 1:
					raw := make([]byte, 4)
					if _, err := io.ReadFull(conn, raw); err != nil {
						return
					}
					host = net.IP(raw).String()
				case 3:
					l := make([]byte, 1)
					if _, err := io.ReadFull(conn, l); err != nil {
						return
					}
					raw := make([]byte, l[0])
					if _, err := io.ReadFull(conn, raw); err != nil {
						return
					}
					host = string(raw)
				default:
					return
				}
				port := make([]byte, 2)
				if _, err := io.ReadFull(conn, port); err != nil {
					return
				}
				if _, err := conn.Write([]byte{5, 0, 0, 1, 127, 0, 0, 1, 0, 0}); err != nil {
					return
				}
				upstream, err := net.Dial("tcp", net.JoinHostPort(host,
					strconv.Itoa(int(port[0])<<8|int(port[1]))))
				if err != nil {
					return
				}
				defer upstream.Close()
				_ = upstream.SetDeadline(time.Now().Add(3 * time.Second))
				go func() { _, _ = io.Copy(upstream, conn); upstream.Close() }()
				_, _ = io.Copy(conn, upstream)
			}()
		}
	}()
	t.Cleanup(func() { _ = ln.Close(); <-done })
	return ln.Addr().String()
}

func probeLimitsFor(pool *x509.CertPool) ProbeLimits {
	return ProbeLimits{
		DialTimeout:   time.Second,
		ProbeTotal:    5 * time.Second,
		AllowPrivate:  true,
		RootCAs:       pool,
		CAFingerprint: "test-ca",
	}
}

// installDNSHook substitutes a deterministic resolver mapping and returns the
// per-hostname lookup counter for pinning assertions.
func installDNSHook(t *testing.T, addrs map[string][]string) *atomic.Int64 {
	t.Helper()
	orig := lookupHost
	var lookups atomic.Int64
	lookupHost = func(ctx context.Context, host string) ([]string, error) {
		if a, ok := addrs[host]; ok {
			lookups.Add(1)
			return a, nil
		}
		return orig(ctx, host)
	}
	t.Cleanup(func() { lookupHost = orig })
	return &lookups
}

func targetConn(proxyAddr string, transport domain.Transport, withAuth bool) domain.Connection {
	conn := domain.Connection{Transport: transport, Host: "127.0.0.1"}
	_, portText, _ := net.SplitHostPort(proxyAddr)
	conn.Port, _ = strconv.Atoi(portText)
	if withAuth {
		conn.Username, conn.Password = "u", "p"
	}
	return conn
}

func TestNormalizeOptions(t *testing.T) {
	scope, derr := NormalizeOptions("", "")
	if derr != nil || scope.Mode != "basic" || scope.Profile != ProfileHTTPSBasicV1 {
		t.Fatalf("empty mode must default to basic: %+v err=%v", scope, derr)
	}
	scope, derr = NormalizeOptions("TLS", " Example.COM. :8443 ")
	if derr != nil {
		t.Fatalf("normalize tls: %v", derr)
	}
	if scope.Mode != "tls" || scope.Profile != ProfileTargetTLSV1 ||
		scope.TargetHostPort != "example.com:8443" {
		t.Fatalf("tls scope mismatch: %+v", scope)
	}
	if _, derr = NormalizeOptions("tls", "https://example.com/path"); derr == nil {
		t.Fatal("URL target must be rejected")
	}
	if _, derr = NormalizeOptions("tls", "example.com"); derr == nil {
		t.Fatal("target without port must be rejected")
	}
	if _, derr = NormalizeOptions("tls", ""); derr == nil {
		t.Fatal("empty tls target must be rejected")
	}
	if _, derr = NormalizeOptions("tls", "example.com:notaport"); derr == nil {
		t.Fatal("bad port must be rejected")
	}
	if _, derr = NormalizeOptions("quic", "example.com:443"); derr == nil {
		t.Fatal("unknown mode must be rejected")
	}
	scope, derr = NormalizeOptions("tls", "[2001:DB8::1]:443")
	if derr != nil || scope.TargetHostPort != "[2001:db8::1]:443" {
		t.Fatalf("ipv6 target normalize: %+v err=%v", scope, derr)
	}
}

func TestProbeSecurityHashVariesWithRuntimeConfig(t *testing.T) {
	base := ProbeOptions{JudgeURL: "https://judge.test/ip", Limits: probeLimitsFor(nil)}
	same := base
	if ProbeSecurityHash(base) != ProbeSecurityHash(same) {
		t.Fatal("identical config produced different hash")
	}
	secret := base
	secret.Limits.JudgeSecret = "different-secret"
	if ProbeSecurityHash(base) == ProbeSecurityHash(secret) {
		t.Fatal("judge secret change must change hash")
	}
	url := base
	url.JudgeURL = "https://other.test/ip"
	if ProbeSecurityHash(base) == ProbeSecurityHash(url) {
		t.Fatal("judge URL change must change hash")
	}
	ca := base
	ca.Limits.CAFingerprint = "other-ca"
	if ProbeSecurityHash(base) == ProbeSecurityHash(ca) {
		t.Fatal("CA change must change hash")
	}
	allow := base
	allow.Limits.AllowPrivate = !base.Limits.AllowPrivate
	if ProbeSecurityHash(base) == ProbeSecurityHash(allow) {
		t.Fatal("address policy change must change hash")
	}
}

// TestProbeTargetTLSValidProxyCredentials proves the HTTP CONNECT path
// forwards credentials as Proxy-Authorization: a proxy that requires auth
// accepts the probe instead of rejecting it with 407.
func TestProbeTargetTLSValidProxyCredentials(t *testing.T) {
	cert, pool := genCert(t, []string{"probe.example"}, []net.IP{net.ParseIP("127.0.0.1")})
	target := startTLSTarget(t, cert)
	_, portText, _ := net.SplitHostPort(target.addr)
	proxyAddr := startConnectProxy(t, "u", "p", "ok")
	installDNSHook(t, map[string][]string{"probe.example": {"127.0.0.1"}})
	ev, fail := Probe(context.Background(), mustScope(t, "tls", "probe.example:"+portText),
		targetConn(proxyAddr, domain.TransportHTTP, true),
		ProbeOptions{Limits: probeLimitsFor(pool)}, "req-auth")
	if fail != nil {
		t.Fatalf("authenticated CONNECT must pass: %+v", fail)
	}
	if ev.Outcome != "PASS" {
		t.Fatalf("unexpected evidence: %+v", ev)
	}
}

// TestProbeCancelResponsiveDuringConnect asserts a cancelled request does not
// wait out the dial deadline while a silent proxy holds the CONNECT open.
func TestProbeCancelResponsiveDuringConnect(t *testing.T) {
	_, pool := genCert(t, []string{"probe.example"}, nil)
	proxyAddr := startConnectProxy(t, "", "", "silent")
	installDNSHook(t, map[string][]string{"probe.example": {"127.0.0.1"}})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()
	started := time.Now()
	ev, fail := Probe(ctx, mustScope(t, "tls", "probe.example:443"),
		targetConn(proxyAddr, domain.TransportHTTP, false),
		ProbeOptions{Limits: probeLimitsFor(pool)}, "req-cancel")
	elapsed := time.Since(started)
	if fail == nil || fail.Kind != FailBudget {
		t.Fatalf("cancelled probe must return BUDGET, got %+v", fail)
	}
	if elapsed > 1500*time.Millisecond {
		t.Fatalf("cancel did not interrupt CONNECT within 1.5s: %s", elapsed)
	}
	if ev.CompletedAt.IsZero() || ev.LatencyMS < 0 {
		t.Fatalf("cancelled failure evidence lacks completion stamp: %+v", ev)
	}
}

// TestMaliciousProxyConnectHeaders bounds a hostile proxy's influence: endless
// response headers hit the size cap, and a bogus Content-Length with pipelined
// body bytes is rejected without draining.
func TestMaliciousProxyConnectHeaders(t *testing.T) {
	_, pool := genCert(t, []string{"probe.example"}, nil)
	cases := []struct {
		name      string
		behavior  string
		wantParts []string
	}{
		{"header_flood", "headerflood", []string{"headers exceed"}},
		// The pipelined garbage may be buffered with the headers or arrive
		// right after them; both rejections are correct.
		{"bogus_content_length", "clsniff", []string{"unsolicited body bytes", "must not carry a body"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			proxyAddr := startConnectProxy(t, "", "", tc.behavior)
			installDNSHook(t, map[string][]string{"probe.example": {"127.0.0.1"}})
			started := time.Now()
			ev, fail := Probe(context.Background(), mustScope(t, "tls", "probe.example:443"),
				targetConn(proxyAddr, domain.TransportHTTP, false),
				ProbeOptions{Limits: probeLimitsFor(pool)}, "req-evil")
			matched := false
			for _, want := range tc.wantParts {
				if fail != nil && strings.Contains(fail.Detail, want) {
					matched = true
					break
				}
			}
			if !matched {
				t.Fatalf("malicious proxy %s must fail with one of %v, got %+v",
					tc.behavior, tc.wantParts, fail)
			}
			if fail.Kind != FailProxy {
				t.Fatalf("malicious proxy behavior kind %q, want PROXY", fail.Kind)
			}
			if elapsed := time.Since(started); elapsed > 2*time.Second {
				t.Fatalf("malicious proxy stalled the probe: %s", elapsed)
			}
			if ev.CompletedAt.IsZero() {
				t.Fatalf("failure evidence lacks completion stamp: %+v", ev)
			}
		})
	}
}

// TestProbeTargetTLSViaHTTPConnect covers the pass path end to end: CONNECT
// tunnel, one-shot DNS pinning, SNI = original host, certificate verification
// against the test CA, and zero application bytes to the target.
func TestProbeTargetTLSViaHTTPConnect(t *testing.T) {
	cert, pool := genCert(t, []string{"probe.example"}, []net.IP{net.ParseIP("127.0.0.1")})
	target := startTLSTarget(t, cert)
	_, portText, _ := net.SplitHostPort(target.addr)
	proxyAddr := startConnectProxy(t, "", "", "ok")
	lookups := installDNSHook(t, map[string][]string{"probe.example": {"127.0.0.1"}})

	scope, derr := NormalizeOptions("tls", "probe.example:"+portText)
	if derr != nil {
		t.Fatal(derr)
	}
	ev, fail := Probe(context.Background(), scope,
		targetConn(proxyAddr, domain.TransportHTTP, false),
		ProbeOptions{Limits: probeLimitsFor(pool)}, "req-1")
	if fail != nil {
		t.Fatalf("target TLS probe failed: %+v", fail)
	}
	if ev.Outcome != "PASS" || ev.Profile != ProfileTargetTLSV1 || ev.Mode != "tls" ||
		ev.Target != "probe.example:"+portText || ev.SecurityHash == "" ||
		ev.AttemptID == "" || ev.RequestID != "req-1" || ev.LatencyMS < 0 {
		t.Fatalf("evidence incomplete: %+v", ev)
	}
	if n := lookups.Load(); n != 1 {
		t.Fatalf("target hostname resolved %d times, want exactly 1", n)
	}
	select {
	case sni := <-target.sni:
		if sni != "probe.example" {
			t.Fatalf("SNI %q, want probe.example", sni)
		}
	default:
		t.Fatal("target never observed a TLS ClientHello")
	}
	if n := target.appBytes.Load(); n != 0 {
		t.Fatalf("TLS-only probe sent %d application bytes", n)
	}
}

func TestProbeTargetTLSClassifiesFailures(t *testing.T) {
	cert, pool := genCert(t, []string{"probe.example"}, []net.IP{net.ParseIP("127.0.0.1")})
	installDNSHook(t, map[string][]string{"probe.example": {"127.0.0.1"}})
	target := startTLSTarget(t, cert)
	_, portText, _ := net.SplitHostPort(target.addr)
	_, targetPort, _ := net.SplitHostPort(target.addr)
	wrongCert, wrongPool := genCert(t, []string{"other.example"}, nil)
	_ = wrongCert
	plainLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { plainLn.Close() })
	go func() {
		for {
			c, err := plainLn.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()

	cases := []struct {
		name      string
		scope     domain.ValidationScope
		conn      domain.Connection
		pool      *x509.CertPool
		wantKind  string
		wantParts []string
	}{
		{
			name:     "proxy 407 is auth",
			scope:    mustScope(t, "tls", "probe.example:"+portText),
			conn:     targetConn(startConnectProxy(t, "u", "p", "ok"), domain.TransportHTTP, false),
			pool:     pool,
			wantKind: FailAuth,
		},
		{
			name:     "proxy CONNECT status is target-scope",
			scope:    mustScope(t, "tls", "probe.example:"+portText),
			conn:     targetConn(startConnectProxy(t, "", "", "refuse"), domain.TransportHTTP, false),
			pool:     pool,
			wantKind: FailTarget,
		},
		{
			name:     "plain tcp target is target-scope",
			scope:    mustScope(t, "tls", plainLn.Addr().String()),
			conn:     targetConn(startConnectProxy(t, "", "", "ok"), domain.TransportHTTP, false),
			pool:     pool,
			wantKind: FailTarget,
		},
		{
			name:     "wrong CA fails target verification",
			scope:    mustScope(t, "tls", "probe.example:"+portText),
			conn:     targetConn(startConnectProxy(t, "", "", "ok"), domain.TransportHTTP, false),
			pool:     wrongPool,
			wantKind: FailTarget,
		},
		{
			name:     "private endpoints rejected when allow_private off",
			scope:    mustScope(t, "tls", "127.0.0.1:"+targetPort),
			conn:     targetConn(startConnectProxy(t, "", "", "ok"), domain.TransportHTTP, false),
			pool:     pool,
			wantKind: FailProxy,
		},
		{
			name:     "socks5 wrong password is auth",
			scope:    mustScope(t, "tls", "probe.example:"+portText),
			conn:     targetConn(startSocks5Proxy(t, "u2", "p2", "ok"), domain.TransportSOCKS5, true),
			pool:     pool,
			wantKind: FailAuth,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			limits := probeLimitsFor(tc.pool)
			if tc.name == "private endpoints rejected when allow_private off" {
				limits.AllowPrivate = false
			}
			ev, fail := Probe(context.Background(), tc.scope, tc.conn,
				ProbeOptions{Limits: limits}, "req-x")
			if fail == nil {
				t.Fatalf("expected %s failure, got PASS evidence %+v", tc.wantKind, ev)
			}
			if fail.Kind != tc.wantKind {
				t.Fatalf("failure kind %q (%s), want %q", fail.Kind, fail.Detail, tc.wantKind)
			}
			if ev.CompletedAt.IsZero() || ev.LatencyMS < 0 {
				t.Fatalf("failure evidence lacks completion stamp: %+v", ev)
			}
			for _, part := range tc.wantParts {
				if !strings.Contains(fail.Detail, part) {
					t.Fatalf("detail %q missing %q", fail.Detail, part)
				}
			}
			if ev.Outcome == "PASS" {
				t.Fatalf("failed probe must not pass: %+v", ev)
			}
		})
	}
}

// TestProbeTargetTLSViaSocks5 verifies the SOCKS5 pass path including
// credentials forwarded to the proxy.
func TestProbeTargetTLSViaSocks5(t *testing.T) {
	cert, pool := genCert(t, []string{"probe.example"}, []net.IP{net.ParseIP("127.0.0.1")})
	target := startTLSTarget(t, cert)
	_, portText, _ := net.SplitHostPort(target.addr)
	proxyAddr := startSocks5Proxy(t, "u", "p", "ok")
	installDNSHook(t, map[string][]string{"probe.example": {"127.0.0.1"}})
	scope := mustScope(t, "tls", "probe.example:"+portText)
	ev, fail := Probe(context.Background(), scope,
		targetConn(proxyAddr, domain.TransportSOCKS5, true),
		ProbeOptions{Limits: probeLimitsFor(pool)}, "req-s")
	if fail != nil {
		t.Fatalf("socks5 target TLS probe failed: %+v", fail)
	}
	if ev.Outcome != "PASS" {
		t.Fatalf("unexpected evidence: %+v", ev)
	}
	if n := target.appBytes.Load(); n != 0 {
		t.Fatalf("TLS-only probe sent %d application bytes", n)
	}
}

// TestProxyHostnameResolvedOncePinned pins a hostname proxy: the resolve
// happens exactly once and the dialed proxy address is the pinned IP even
// though the connection keeps its hostname identity for credentials.
func TestProxyHostnameResolvedOncePinned(t *testing.T) {
	cert, pool := genCert(t, []string{"probe.example"}, []net.IP{net.ParseIP("127.0.0.1")})
	target := startTLSTarget(t, cert)
	_, portText, _ := net.SplitHostPort(target.addr)
	proxyAddr := startConnectProxy(t, "", "", "ok")
	_, proxyPort, _ := net.SplitHostPort(proxyAddr)

	lookups := installDNSHook(t, map[string][]string{
		"proxy.example": {"127.0.0.1"}, "probe.example": {"127.0.0.1"},
	})

	conn := targetConn("127.0.0.1:"+proxyPort, domain.TransportHTTP, false)
	conn.Host = "proxy.example"
	ev, fail := Probe(context.Background(), mustScope(t, "tls", "probe.example:"+portText),
		conn, ProbeOptions{Limits: probeLimitsFor(pool)}, "req-p")
	if fail != nil {
		t.Fatalf("hostname proxy probe failed: %+v", fail)
	}
	if ev.Outcome != "PASS" {
		t.Fatalf("unexpected evidence: %+v", ev)
	}
	if n := lookups.Load(); n != 2 {
		t.Fatalf("proxy+target lookups = %d, want exactly 2 (one each)", n)
	}
}

func TestResolveTargetPinnedPolicy(t *testing.T) {
	lookups := installDNSHook(t, map[string][]string{
		"mixed.example":   {"10.0.0.5", "8.8.8.8"},
		"private.example": {"127.0.0.1"},
		"mapped.example":  {"::ffff:203.0.113.10"},
	})
	ip, err := resolveTargetPinned(context.Background(), "mixed.example", time.Second, false)
	if err != nil || ip.String() != "8.8.8.8" {
		t.Fatalf("mixed resolution: ip=%s err=%v", ip, err)
	}
	if _, err := resolveTargetPinned(context.Background(), "private.example", time.Second, false); err == nil {
		t.Fatal("private-only resolution must fail with allow_private off")
	}
	ip, err = resolveTargetPinned(context.Background(), "private.example", time.Second, true)
	if err != nil || ip.String() != "127.0.0.1" {
		t.Fatalf("private fixture resolution: ip=%s err=%v", ip, err)
	}
	if _, err := resolveTargetPinned(context.Background(), "mapped.example", time.Second, true); err == nil {
		t.Fatal("IPv4-mapped-only resolution must be rejected")
	}
	if _, err := resolveTargetPinned(context.Background(), "127.0.0.1", time.Second, false); err == nil {
		t.Fatal("private literal target must fail the policy check at resolve")
	}
	if n := lookups.Load(); n != 4 {
		t.Fatalf("hostname lookups = %d, want 4 (literals bypass DNS)", n)
	}
}

func mustScope(t *testing.T, mode, target string) domain.ValidationScope {
	t.Helper()
	scope, derr := NormalizeOptions(mode, target)
	if derr != nil {
		t.Fatalf("normalize %q %q: %v", mode, target, derr)
	}
	return scope
}
