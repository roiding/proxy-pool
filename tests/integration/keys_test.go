package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"proxypool/internal/api"
	"proxypool/internal/application"
	"proxypool/internal/config"
	"proxypool/internal/domain"
	"proxypool/internal/storage/sqlite"
)

type keyAPI struct {
	srv   *httptest.Server
	env   *env
	token string
}

func newKeyAPI(t *testing.T, e *env) *keyAPI {
	t.Helper()
	return &keyAPI{
		srv: httptest.NewServer(api.NewServer(e.cfg, testLogger(),
			e.acquire, e.ingest, e.leases, e.store, e.nightly, e.prewarm, e.keys).Handler()),
		env:   e,
		token: e.cfg.APIToken,
	}
}

func (k *keyAPI) do(t *testing.T, method, path, token, body string) (int, []byte) {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, _ := http.NewRequest(method, k.srv.URL+path, reader)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	data, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp.StatusCode, data
}

func (k *keyAPI) createKey(t *testing.T, name string, defaultCD int64) string {
	t.Helper()
	code, data := k.do(t, http.MethodPost, "/v1/admin/keys", k.token,
		fmt.Sprintf(`{"name":%q,"default_cd_seconds":%d}`, name, defaultCD))
	if code != http.StatusCreated {
		t.Fatalf("create key status=%d body=%s", code, data)
	}
	var body struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(data, &body); err != nil || body.Token == "" {
		t.Fatalf("create key response: %s (%v)", data, err)
	}
	return body.Token
}

func TestAPIKeyManagementAndAuth(t *testing.T) {
	j := startJudge(t)
	e := newEnv(t, j, func(c *config.Config) {
		c.AdminPassword = "admin-password-0123456789abcdef"
	})
	e.importProxy(t, "manual", "http://127.0.0.1:1")
	k := newKeyAPI(t, e)
	defer k.srv.Close()

	// Console login uses admin_password, not the legacy api_token.
	code, _ := k.do(t, http.MethodPost, "/admin/login", "", `{"token":"`+e.cfg.APIToken+`"}`)
	if code != http.StatusUnauthorized {
		t.Fatalf("login with api_token should fail when admin_password is set: %d", code)
	}
	code, _ = k.do(t, http.MethodPost, "/admin/login", "", `{"token":"`+e.cfg.AdminPassword+`"}`)
	if code != http.StatusOK {
		t.Fatalf("login with admin_password status=%d", code)
	}

	token := k.createKey(t, "biz-a", 300)
	// A downstream key cannot manage keys or sources.
	code, data := k.do(t, http.MethodGet, "/v1/admin/keys", token, "")
	if code != http.StatusForbidden {
		t.Fatalf("downstream key admin access status=%d body=%s", code, data)
	}
	// The key works for the delivery surface.
	req, _ := http.NewRequest(http.MethodPost, k.srv.URL+"/v1/proxies/acquire",
		strings.NewReader(`{"count":1}`))
	req.Header.Set("X-API-Key", token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("key acquire: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable && resp.StatusCode != http.StatusOK {
		t.Fatalf("key acquire status=%d", resp.StatusCode)
	}

	// Disabling the key invalidates it immediately.
	var keys []domain.APIKey
	code, data = k.do(t, http.MethodGet, "/v1/admin/keys", k.token, "")
	if code != http.StatusOK || json.Unmarshal(data, &keys) != nil || len(keys) != 1 {
		t.Fatalf("list keys: %d %s", code, data)
	}
	code, data = k.do(t, http.MethodPatch, "/v1/admin/keys/"+keys[0].ID, k.token, `{"enabled":false}`)
	if code != http.StatusOK {
		t.Fatalf("disable key: %d %s", code, data)
	}
	req, _ = http.NewRequest(http.MethodPost, k.srv.URL+"/v1/proxies/acquire",
		strings.NewReader(`{"count":1}`))
	req.Header.Set("X-API-Key", token)
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("disabled key acquire: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("disabled key status=%d, want 401", resp.StatusCode)
	}
}

func TestConsumerCooldownIsolationAndDefault(t *testing.T) {
	j := startJudge(t)
	good1 := startFakeProxy(t)
	good2 := startFakeProxy(t)
	e := newEnv(t, j, nil)
	if err := e.ingest.EnsureSource(context.Background(), "manual",
		application.SourceConfig{DefaultTransport: domain.TransportHTTP, MaxPoolLeases: 1}, true); err != nil {
		t.Fatalf("ensure source: %v", err)
	}
	if _, derr := e.ingest.ImportLines(context.Background(), "manual",
		fmt.Sprintf("http://127.0.0.1:%d\nhttp://127.0.0.1:%d\n",
			portOf(good1.URL), portOf(good2.URL))); derr != nil {
		t.Fatalf("import: %v", derr)
	}
	k := newKeyAPI(t, e)
	defer k.srv.Close()
	keyA := k.createKey(t, "biz-a", 300)
	keyB := k.createKey(t, "biz-b", 0)

	acquire := func(key string, query string) (int, []byte) {
		req, _ := http.NewRequest(http.MethodPost,
			k.srv.URL+"/v1/proxies/acquire"+query, strings.NewReader(`{"count":1}`))
		req.Header.Set("X-API-Key", key)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return resp.StatusCode, data
	}
	release := func(key, leaseID string) {
		code, data := k.do(t, http.MethodPost, "/v1/leases/"+leaseID+"/release", key, "")
		if code != http.StatusOK {
			t.Fatalf("release: %d %s", code, data)
		}
	}
	parse := func(data []byte) (leaseID, resourceID string) {
		var out struct {
			Items []struct {
				LeaseID    string `json:"lease_id"`
				ResourceID string `json:"resource_id"`
			} `json:"items"`
		}
		if err := json.Unmarshal(data, &out); err != nil || len(out.Items) != 1 {
			t.Fatalf("acquire response: %s (%v)", data, err)
		}
		return out.Items[0].LeaseID, out.Items[0].ResourceID
	}

	// Key A's default CD is 300s: two deliveries must be different proxies.
	code, data := acquire(keyA, "")
	if code != http.StatusOK {
		t.Fatalf("A first acquire: %d %s", code, data)
	}
	lease1, resource1 := parse(data)
	release(keyA, lease1)
	code, data = acquire(keyA, "")
	if code != http.StatusOK {
		t.Fatalf("A second acquire: %d %s", code, data)
	}
	lease2, resource2 := parse(data)
	if resource2 == resource1 {
		t.Fatalf("same proxy repeated inside default CD: %s", data)
	}
	release(keyA, lease2)
	// Third delivery has no eligible proxy left for A.
	code, data = acquire(keyA, "")
	if code == http.StatusOK {
		t.Fatalf("A third acquire should be blocked by CD: %s", data)
	}
	// Explicit cd=0 overrides the key default and disables the window.
	code, data = acquire(keyA, "?cd=0")
	if code != http.StatusOK {
		t.Fatalf("A explicit cd=0 should disable CD: %d %s", code, data)
	}
	lease3, _ := parse(data)
	release(keyA, lease3)

	// Key B is a different channel and can still receive proxy 1.
	code, data = acquire(keyB, "?cd=300")
	if code != http.StatusOK {
		t.Fatalf("B acquire with explicit cd: %d %s", code, data)
	}
	lease4, _ := parse(data)
	release(keyB, lease4)

	// Query-string key authentication also works.
	req, _ := http.NewRequest(http.MethodPost,
		k.srv.URL+"/v1/proxies/acquire?cd=0&key="+keyB, strings.NewReader(`{"count":1}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("query-key acquire: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("query-key acquire status=%d", resp.StatusCode)
	}
}

func TestConsumerCooldownWindow(t *testing.T) {
	j := startJudge(t)
	e := newEnv(t, j, nil)
	e.importProxy(t, "manual", "http://127.0.0.1:1")
	ctx := context.Background()
	cands, err := e.store.SelectCandidates(ctx, sqlite.SelectionFilters{
		Profile: "https_basic_v1", Now: time.Now(), UsefulCutoff: time.Now(), Limit: 1,
	})
	if err != nil || len(cands) != 1 {
		t.Fatalf("select: %v %+v", err, cands)
	}
	now := time.Now()
	if err := e.store.RecordConsumerDeliveries(ctx, "c1", []string{cands[0].Resource.ID}, now.Add(-5*time.Second)); err != nil {
		t.Fatalf("record delivery: %v", err)
	}
	inWindow, err := e.store.SelectCandidates(ctx, sqlite.SelectionFilters{
		Profile: "https_basic_v1", Now: now, UsefulCutoff: now,
		ConsumerID: "c1", ConsumerCooldown: 10 * time.Second, Limit: 1,
	})
	if err != nil || len(inWindow) != 0 {
		t.Fatalf("inside CD window: %v %+v", err, inWindow)
	}
	outside, err := e.store.SelectCandidates(ctx, sqlite.SelectionFilters{
		Profile: "https_basic_v1", Now: now, UsefulCutoff: now,
		ConsumerID: "c1", ConsumerCooldown: 4 * time.Second, Limit: 1,
	})
	if err != nil || len(outside) != 1 {
		t.Fatalf("outside CD window: %v %+v", err, outside)
	}
}

func TestConsumerCooldownConcurrentSingleWinner(t *testing.T) {
	j := startJudge(t)
	good := startFakeProxy(t)
	e := newEnv(t, j, nil)
	if err := e.ingest.EnsureSource(context.Background(), "manual",
		application.SourceConfig{DefaultTransport: domain.TransportHTTP, MaxPoolLeases: 2}, true); err != nil {
		t.Fatalf("ensure source: %v", err)
	}
	if _, derr := e.ingest.ImportLines(context.Background(), "manual",
		fmt.Sprintf("http://127.0.0.1:%d\n", portOf(good.URL))); derr != nil {
		t.Fatalf("import: %v", derr)
	}
	k := newKeyAPI(t, e)
	defer k.srv.Close()
	key := k.createKey(t, "biz-a", 300)

	var wg sync.WaitGroup
	results := make(chan int, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := http.NewRequest(http.MethodPost,
				k.srv.URL+"/v1/proxies/acquire?cd=300", strings.NewReader(`{"count":1}`))
			req.Header.Set("X-API-Key", key)
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				results <- 0
				return
			}
			results <- resp.StatusCode
			resp.Body.Close()
		}()
	}
	wg.Wait()
	close(results)
	okCount := 0
	for code := range results {
		if code == http.StatusOK {
			okCount++
		}
	}
	if okCount != 1 {
		t.Fatalf("concurrent same-key acquires succeeded %d times, want exactly 1", okCount)
	}
}
