package integration

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"proxypool/internal/application"
	"proxypool/internal/domain"
)

func TestAuditStaticCacheInvalidationReleasesByteBudget(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	if err := e.ingest.ConfigureStaticCache(time.Minute, 1, 4096); err != nil {
		t.Fatal(err)
	}
	list := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		for _, port := range []int{18001, 18002} {
			_, _ = fmt.Fprintf(w, "http://%s:%s@127.0.0.1:%d\n",
				strings.Repeat("u", 64), strings.Repeat("p", 64), port)
		}
	}))
	t.Cleanup(list.Close)
	ctx := context.Background()
	clock := time.Now().UTC()
	e.ingest.Now = func() time.Time { return clock }
	sourceConfig := application.SourceConfig{
		MaxPoolLeases: 1,
		Static:        &application.StaticConfig{URL: list.URL, DefaultTransport: domain.TransportHTTP},
	}
	source, derr := e.ingest.CreateSource(ctx, "audit-static-byte-budget", sourceConfig, true)
	if derr != nil {
		t.Fatal(derr)
	}
	for revision := 0; revision < 12; revision++ {
		if revision > 0 {
			clock = clock.Add(2 * time.Second)
			sourceConfig.Static.URL = fmt.Sprintf("%s?audit_revision=%d", list.URL, revision)
			source, derr = e.ingest.UpdateSource(ctx, source.ID, sourceConfig, true, source.ConfigVersion)
			if derr != nil {
				t.Fatal(derr)
			}
		}
		refreshed, derr := e.ingest.RefreshStaticSource(ctx, source.ID)
		if derr != nil || refreshed.Fetched != 2 {
			t.Fatalf("fixture refresh %d: result=%+v err=%v", revision, refreshed, derr)
		}
		cached, batch, ok := e.ingest.CachedStaticCandidates(source.ID)
		if !ok || len(cached) != 2 || batch != refreshed.BatchID {
			t.Fatalf("invalidated entries still consumed byte budget at revision %d: present=%t count=%d batch=%s refreshed=%+v",
				revision, ok, len(cached), batch, refreshed)
		}
	}
	stats, err := e.store.Stats(ctx)
	if err != nil || stats.Resources != 0 || stats.EnabledOffers != 0 {
		t.Fatalf("cache budget checks populated inventory: stats=%+v err=%v", stats, err)
	}
}

func TestAuditStaticStateToggleDoesNotEraseFetchThrottle(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	clock := time.Now().UTC()
	e.ingest.Now = func() time.Time { return clock }
	var downloads atomic.Int32
	list := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		downloads.Add(1)
		_, _ = fmt.Fprintln(w, "http://127.0.0.1:18001")
	}))
	t.Cleanup(list.Close)
	ctx := context.Background()
	source, derr := e.ingest.CreateSource(ctx, "audit-static-toggle", application.SourceConfig{
		MaxPoolLeases: 1,
		Static:        &application.StaticConfig{URL: list.URL, DefaultTransport: domain.TransportHTTP},
	}, true)
	if derr != nil {
		t.Fatal(derr)
	}
	if _, derr := e.ingest.RefreshStaticSource(ctx, source.ID); derr != nil {
		t.Fatal(derr)
	}
	source, derr = e.ingest.SetSourceEnabled(ctx, source.ID, false, source.ConfigVersion)
	if derr != nil {
		t.Fatal(derr)
	}
	source, derr = e.ingest.SetSourceEnabled(ctx, source.ID, true, source.ConfigVersion)
	if derr != nil {
		t.Fatal(derr)
	}
	_, derr = e.ingest.RefreshStaticSource(ctx, source.ID)
	if derr == nil || derr.Code != "SOURCE_RATE_LIMITED" || downloads.Load() != 1 {
		t.Fatalf("same-source state toggle erased an unexpired fetch window: downloads=%d err=%v", downloads.Load(), derr)
	}
}
