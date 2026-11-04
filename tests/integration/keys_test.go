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
	code, data := k.do(t, http.MethodPost, "/api/v1/admin/keys", k.token,
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
	code, data := k.do(t, http.MethodGet, "/api/v1/admin/keys", token, "")
	if code != http.StatusForbidden {
		t.Fatalf("downstream key admin access status=%d body=%s", code, data)
	}
	// The key works for the delivery surface.
	req, _ := http.NewRequest(http.MethodPost, k.srv.URL+"/api/v1/proxies/acquire",
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
	code, data = k.do(t, http.MethodGet, "/api/v1/admin/keys", k.token, "")
	if code != http.StatusOK || json.Unmarshal(data, &keys) != nil || len(keys) != 1 {
		t.Fatalf("list keys: %d %s", code, data)
	}
	code, data = k.do(t, http.MethodPatch, "/api/v1/admin/keys/"+keys[0].ID, k.token, `{"enabled":false}`)
	if code != http.StatusOK {
		t.Fatalf("disable key: %d %s", code, data)
	}
	req, _ = http.NewRequest(http.MethodPost, k.srv.URL+"/api/v1/proxies/acquire",
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
	keyA, _, err := e.keys.Create(context.Background(), "biz-a", 300*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	keyB, _, err := e.keys.Create(context.Background(), "biz-b", 0)
	if err != nil {
		t.Fatal(err)
	}

	acquire := func(key domain.APIKey, cdSeconds *int64) application.AcquireResult {
		t.Helper()
		result, derr := e.acquire.Acquire(context.Background(), application.AcquireRequest{
			Count: 1, ConsumerID: key.ID, CDSeconds: cdSeconds,
		})
		if derr != nil {
			t.Fatalf("acquire: %v", derr)
		}
		return result
	}
	release := func(key domain.APIKey, leaseID string) {
		if ok, derr := e.leases.Release(context.Background(), leaseID, key.ID, false); derr != nil || !ok {
			t.Fatalf("release: ok=%v err=%v", ok, derr)
		}
	}

	// Project CD is the CD subject: both keys share the default project's
	// fixed windows, and the current request's cd cannot shorten them.
	first := acquire(keyA, nil)
	if first.Delivered != 1 {
		t.Fatalf("A first acquire: %s", first.ShortfallReason)
	}
	release(keyA, first.Items[0].LeaseID)
	second := acquire(keyA, nil)
	if second.Delivered != 1 {
		t.Fatalf("A second acquire: %s", second.ShortfallReason)
	}
	if second.Items[0].ResourceID == first.Items[0].ResourceID {
		t.Fatal("same proxy repeated inside default CD")
	}
	release(keyA, second.Items[0].LeaseID)
	// Both proxies are inside the project window now: further requests are
	// explained business results, and cd=0 cannot shorten the live windows.
	noCD, zeroCD := (*int64)(nil), int64(0)
	for _, cd := range []*int64{noCD, &zeroCD} {
		result := acquire(keyA, cd)
		if result.Delivered != 0 || result.ShortfallReason == "" {
			t.Fatalf("blocked acquire must be an explained zero delivery: %+v", result)
		}
	}
	// Key B is another credential in the same project: it cannot bypass the
	// project's fixed CD either.
	if blocked := acquire(keyB, nil); blocked.Delivered != 0 {
		t.Fatalf("B must not bypass project CD: %+v", blocked)
	}
}

func TestProjectCooldownWindow(t *testing.T) {
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
	fp := e.acquire.FingerprintOf(cands[0].Resource.Conn)
	now := time.Now()
	if err := e.store.RecordProjectDeliveries(ctx, "proj-cd", 10*time.Second,
		now.Add(-5*time.Second), []string{fp}); err != nil {
		t.Fatalf("record delivery: %v", err)
	}
	blocked, err := e.store.ProjectCooldownBlocked(ctx, "proj-cd", now, []string{fp})
	if err != nil || len(blocked) != 1 {
		t.Fatalf("inside CD window must block: %v %+v", err, blocked)
	}
	blocked, err = e.store.ProjectCooldownBlocked(ctx, "proj-cd",
		now.Add(6*time.Second), []string{fp})
	if err != nil || len(blocked) != 0 {
		t.Fatalf("outside CD window must release: %v %+v", err, blocked)
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
	results := make(chan string, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := http.NewRequest(http.MethodPost,
				k.srv.URL+"/api/v1/proxies/acquire?cd=300", strings.NewReader(`{"count":1}`))
			req.Header.Set("X-API-Key", key)
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				results <- err.Error()
				return
			}
			data, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<10))
			resp.Body.Close()
			results <- string(data)
		}()
	}
	wg.Wait()
	close(results)
	delivered := 0
	for body := range results {
		var result application.AcquireResult
		if json.Unmarshal([]byte(body), &result) == nil {
			delivered += result.Delivered
		}
	}
	// Zero deliveries are explained business results (HTTP 200) now; the
	// invariant is that exactly one connection is delivered in total.
	if delivered != 1 {
		t.Fatalf("concurrent same-project acquires delivered %d connections, want exactly 1", delivered)
	}
}
