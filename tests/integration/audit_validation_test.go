package integration

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"proxypool/internal/domain"
	"proxypool/internal/validation"
)

func TestAuditPublicAddressPolicyRejectsMetadataAndReservedRanges(t *testing.T) {
	for _, text := range []string{
		"127.0.0.1", "10.0.0.1", "169.254.169.254", "100.100.100.200",
		"0.1.2.3", "198.18.0.1", "240.0.0.1", "255.255.255.255",
		"::ffff:127.0.0.1", "::ffff:10.0.0.1",
	} {
		t.Run(text, func(t *testing.T) {
			if domain.AddrAllowed(netip.MustParseAddr(text), false) {
				t.Fatalf("non-public address %s was allowed for real network I/O", text)
			}
		})
	}
	if !domain.AddrAllowed(netip.MustParseAddr("8.8.8.8"), false) {
		t.Fatal("ordinary public unicast address was incorrectly rejected")
	}
}

func auditTLSOptions(j *judge) validation.ProbeOptions {
	digest := sha256.Sum256([]byte(j.certPEM()))
	return validation.ProbeOptions{Limits: validation.ProbeLimits{
		DialTimeout: time.Second, ProbeTotal: 2 * time.Second,
		AllowPrivate: true, RootCAs: j.certPool(), CAFingerprint: hex.EncodeToString(digest[:]),
	}}
}

func auditTLSScope(t *testing.T, j *judge) domain.ValidationScope {
	t.Helper()
	scope, err := validation.NormalizeOptions("tls", strings.TrimPrefix(j.srv.URL, "https://"))
	if err != nil {
		t.Fatal(err)
	}
	return scope
}

func TestAuditTLSUsesProxyAuthorizationHeader(t *testing.T) {
	j := startJudge(t)
	inner := startFakeProxy(t)
	expected := "Basic " + base64.StdEncoding.EncodeToString([]byte("audit-user:audit-password"))
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Proxy-Authorization") != expected {
			http.Error(w, "proxy credentials required", http.StatusProxyAuthRequired)
			return
		}
		inner.Config.Handler.ServeHTTP(w, r)
	}))
	t.Cleanup(proxy.Close)
	ev, failure := validation.Probe(context.Background(), auditTLSScope(t, j), domain.Connection{
		Transport: domain.TransportHTTP, Host: "127.0.0.1", Port: portOf(proxy.URL),
		Username: "audit-user", Password: "audit-password",
	}, auditTLSOptions(j), "audit-proxy-auth")
	if failure != nil || ev.Outcome != "PASS" {
		t.Fatalf("valid proxy credentials were not sent in Proxy-Authorization: failure=%+v", failure)
	}
}

func TestAuditTLSCacheSecurityHashIncludesAddressPolicy(t *testing.T) {
	strict := validation.ProbeOptions{Limits: validation.ProbeLimits{AllowPrivate: false}}
	permissive := strict
	permissive.Limits.AllowPrivate = true
	if validation.ProbeSecurityHash(strict) == validation.ProbeSecurityHash(permissive) {
		t.Fatal("a stricter address policy would still reuse permissive validation evidence")
	}
}

func TestAuditTLSFailuresHaveCompletionTimestamps(t *testing.T) {
	j := startJudge(t)
	proxy := startFakeProxy(t)
	opts := auditTLSOptions(j)
	opts.Limits.RootCAs = x509.NewCertPool()
	opts.Limits.CAFingerprint = "deliberately-empty-test-trust"
	ev, failure := validation.Probe(context.Background(), auditTLSScope(t, j), domain.Connection{
		Transport: domain.TransportHTTP, Host: "127.0.0.1", Port: portOf(proxy.URL),
	}, opts, "audit-failed-timestamp")
	if failure == nil || failure.Kind != validation.FailTarget {
		t.Fatalf("untrusted target certificate must fail: %+v", failure)
	}
	if ev.CompletedAt.IsZero() || ev.CompletedAt.Before(ev.StartedAt) {
		t.Fatalf("failed probe would disappear from time-filtered metrics: %+v", ev)
	}
}

func TestAuditTLSUpstreamConnectFailureIsNotProxyAuthenticationOrDeath(t *testing.T) {
	j := startJudge(t)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	t.Cleanup(proxy.Close)
	_, failure := validation.Probe(context.Background(), auditTLSScope(t, j), domain.Connection{
		Transport: domain.TransportHTTP, Host: "127.0.0.1", Port: portOf(proxy.URL),
	}, validation.ProbeOptions{Limits: validation.ProbeLimits{
		DialTimeout: time.Second, ProbeTotal: 2 * time.Second, AllowPrivate: true,
	}}, "audit-upstream-connect-failure")
	if failure == nil || failure.Kind != validation.FailTarget || failure.Attributed() {
		t.Fatalf("target-scoped CONNECT failure was attributed to the proxy: %+v", failure)
	}
}

func TestAuditTLSConnectWaitRespondsToCancellation(t *testing.T) {
	j := startJudge(t)
	arrived, resume := make(chan struct{}), make(chan struct{})
	var arrival, release sync.Once
	unblock := func() { release.Do(func() { close(resume) }) }
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		arrival.Do(func() { close(arrived) })
		<-resume
		http.Error(w, "cancelled fixture", http.StatusBadGateway)
	}))
	t.Cleanup(proxy.Close)
	t.Cleanup(unblock)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := auditTLSOptions(j)
	opts.Limits.DialTimeout = 2 * time.Second
	opts.Limits.ProbeTotal = 4 * time.Second
	done := make(chan *validation.Failure, 1)
	scope := auditTLSScope(t, j)
	go func() {
		_, failure := validation.Probe(ctx, scope, domain.Connection{
			Transport: domain.TransportHTTP, Host: "127.0.0.1", Port: portOf(proxy.URL),
		}, opts, "audit-cancel")
		done <- failure
	}()
	select {
	case <-arrived:
	case <-time.After(time.Second):
		t.Fatal("fixture did not reach CONNECT")
	}
	cancel()
	select {
	case failure := <-done:
		if failure == nil || failure.Kind != validation.FailBudget {
			t.Fatalf("cancellation must not be attributed to a proxy: %+v", failure)
		}
	case <-time.After(400 * time.Millisecond):
		t.Fatal("CONNECT read ignored cancellation and would delay early-stop requests")
	}
}
