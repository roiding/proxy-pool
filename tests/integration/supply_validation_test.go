package integration

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"proxypool/internal/application"
	"proxypool/internal/domain"
	"proxypool/internal/validation"
)

func TestSupplyTLSUpstreamCONNECTFailureIsTargetScoped(t *testing.T) {
	j := startJudge(t)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	t.Cleanup(proxy.Close)
	_, failure := validation.Probe(context.Background(), auditTLSScope(t, j),
		domain.Connection{Transport: domain.TransportHTTP, Host: "127.0.0.1",
			Port: portOf(proxy.URL)},
		validation.ProbeOptions{Limits: validation.ProbeLimits{
			DialTimeout: time.Second, ProbeTotal: 2 * time.Second, AllowPrivate: true,
		}}, "supply-connect-502")
	if failure == nil || failure.Kind != validation.FailTarget || failure.Attributed() {
		t.Fatalf("CONNECT 502 must be an uncertain target failure, not proxy death: %+v", failure)
	}
}

func TestSupplyBatchesSkipDisabledSourceAndKeepBoundedFallbacks(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	good := startFakeProxy(t)
	var calls atomic.Int32
	list := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_, _ = fmt.Fprintf(w, "%s\n", good.URL)
	}))
	t.Cleanup(list.Close)
	for _, id := range []string{"supply-static-a", "supply-static-b", "supply-static-c"} {
		if _, derr := e.ingest.CreateSource(context.Background(), id,
			application.SourceConfig{MaxPoolLeases: 1,
				Static: &application.StaticConfig{URL: list.URL,
					DefaultTransport: domain.TransportHTTP}}, true); derr != nil {
			t.Fatal(derr)
		}
	}
	// Downloaded candidates may fail validation. Keep bounded fallback
	// sources instead of treating the first raw row as a successful proxy.
	if _, derr := e.ingest.SetSourceEnabled(context.Background(), "supply-static-a", false, 1); derr != nil {
		t.Fatal(derr)
	}
	got, derr := e.ingest.Batches(context.Background(),
		[]string{"supply-static-a", "supply-static-b", "supply-static-c"}, 1,
		application.RefillScope{Transports: []domain.Transport{domain.TransportHTTP}})
	if derr != nil || len(got) != 1 {
		t.Fatalf("failed first source aborted the bounded batch: got=%d err=%v", len(got), derr)
	}
	if calls.Load() != 2 {
		t.Fatalf("enabled fallback sources were not loaded once: downloads=%d", calls.Load())
	}
	if _, derr := e.ingest.Batches(context.Background(),
		[]string{"supply-static-b", "supply-static-c"}, 1,
		application.RefillScope{Transports: []domain.Transport{domain.TransportHTTP}}); derr != nil || calls.Load() != 2 {
		t.Fatalf("fresh fallback cache was downloaded again: downloads=%d err=%v", calls.Load(), derr)
	}
}
