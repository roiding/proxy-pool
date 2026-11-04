package integration

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"proxypool/internal/usage"
)

func auditUsageService(t *testing.T) (*env, *usage.Service) {
	t.Helper()
	e := newEnv(t, startJudge(t), nil)
	if err := usage.EnsureSchema(e.store.DB()); err != nil {
		t.Fatal(err)
	}
	svc, err := usage.New(e.store.DB(), usage.Options{
		MasterKey:      "audit-usage-master-key-not-for-production-0123456789",
		MonthlyTotalGB: 1024, ThresholdPercent: 80, CheckTime: "09:00", Timezone: "Asia/Shanghai",
		BarkTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return e, svc
}

func auditUsagePatch(t *testing.T, text string) usage.SettingsUpdate {
	t.Helper()
	var patch usage.SettingsUpdate
	if err := json.Unmarshal([]byte(text), &patch); err != nil {
		t.Fatal(err)
	}
	return patch
}

func TestAuditUsageSecretNullClearsAndEmptyPreserves(t *testing.T) {
	_, svc := auditUsageService(t)
	ctx := context.Background()
	if _, err := svc.UpdateSettings(ctx, auditUsagePatch(t,
		`{"huawei_access_key_id":"audit-ak-sentinel","huawei_secret_access_key":"audit-sk-sentinel"}`)); err != nil {
		t.Fatal(err)
	}
	view, err := svc.UpdateSettings(ctx, auditUsagePatch(t,
		`{"huawei_access_key_id":"","huawei_secret_access_key":""}`))
	if err != nil || !view.Huawei.Configured {
		t.Fatalf("empty input must preserve existing secrets: err=%v", err)
	}
	data, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "audit-ak-sentinel") || strings.Contains(string(data), "audit-sk-sentinel") {
		t.Fatal("settings view disclosed plaintext credentials")
	}
	view, err = svc.UpdateSettings(ctx, auditUsagePatch(t,
		`{"huawei_access_key_id":null,"huawei_secret_access_key":null}`))
	if err != nil || view.Huawei.Configured {
		t.Fatalf("explicit null did not clear credentials: err=%v", err)
	}
}

func TestAuditUsageSettingsFailureRollsBackEveryField(t *testing.T) {
	e, svc := auditUsageService(t)
	ctx := context.Background()
	if _, err := svc.UpdateSettings(ctx, auditUsagePatch(t,
		`{"huawei_access_key_id":"original-audit-ak","huawei_secret_access_key":"original-audit-sk"}`)); err != nil {
		t.Fatal(err)
	}
	var original string
	if err := e.store.DB().QueryRow(`SELECT value FROM usage_settings WHERE key='huawei_access_key_id'`).Scan(&original); err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.DB().Exec(`CREATE TRIGGER reject_audit_sk BEFORE INSERT ON usage_settings
		WHEN NEW.key='huawei_secret_access_key' BEGIN SELECT RAISE(ABORT, 'audit simulated persistence failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.UpdateSettings(ctx, auditUsagePatch(t,
		`{"huawei_access_key_id":"changed-audit-ak","huawei_secret_access_key":"changed-audit-sk"}`)); err == nil {
		t.Fatal("simulated persistence failure was ignored")
	}
	var after string
	if err := e.store.DB().QueryRow(`SELECT value FROM usage_settings WHERE key='huawei_access_key_id'`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if original != after {
		t.Fatal("failed settings patch partially changed the encrypted access key")
	}
}

func TestAuditBarkFailureDoesNotDiscloseDeviceKey(t *testing.T) {
	const sentinel = "audit-bark-device-key-sentinel"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "upstream echoed "+r.URL.Path, http.StatusBadGateway)
	}))
	t.Cleanup(srv.Close)
	client, err := usage.NewBarkClient(srv.URL+"/"+sentinel, time.Second, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	err = client.Push(context.Background(), "audit", "test fixture")
	if err == nil {
		t.Fatal("failing Bark endpoint was accepted")
	}
	if strings.Contains(err.Error(), sentinel) {
		t.Fatal("Bark failure exposed the device key through its returned error")
	}
}

func TestAuditBarkDefaultPolicyDoesNotDialPrivateAddress(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	accepted := make(chan struct{}, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- struct{}{}
			_ = conn.Close()
		}
	}()
	client, err := usage.NewBarkClient("https://"+listener.Addr().String()+"/audit-device",
		200*time.Millisecond, 0, false)
	if err == nil {
		if err := client.Push(context.Background(), "audit", "no private dial"); err == nil {
			t.Fatal("private Bark address unexpectedly accepted a push")
		}
	}
	select {
	case <-accepted:
		t.Fatal("default Bark policy reached a private TCP listener before refusing the request")
	default:
	}
}

func TestAuditBarkHTTPHostPrefixIsNotLoopbackException(t *testing.T) {
	for _, host := range []string{"127.0.0.1.example.com", "127.not-loopback.example"} {
		if _, err := usage.NewBarkClient("http://"+host+"/audit-device", time.Second, 0); err == nil {
			t.Errorf("a hostname prefix bypassed the HTTPS requirement: host=%s", host)
		}
	}
}

func TestAuditClearedUsageSecretDoesNotReturnFromBootstrapOnRestart(t *testing.T) {
	e, _ := auditUsageService(t)
	opts := usage.Options{
		MasterKey: "audit-usage-restart-key-0123456789abcdef",
		HuaweiAK:  "audit-bootstrap-ak", HuaweiSK: "audit-bootstrap-sk",
		MonthlyTotalGB: 1024, ThresholdPercent: 80, CheckTime: "09:00", Timezone: "Asia/Shanghai",
	}
	svc, err := usage.New(e.store.DB(), opts)
	if err != nil {
		t.Fatal(err)
	}
	view, err := svc.UpdateSettings(context.Background(), auditUsagePatch(t,
		`{"huawei_access_key_id":null,"huawei_secret_access_key":null}`))
	if err != nil || view.Huawei.Configured {
		t.Fatalf("clearing bootstrapped secrets: configured=%t err=%v", view.Huawei.Configured, err)
	}
	restarted, err := usage.New(e.store.DB(), opts)
	if err != nil {
		t.Fatal(err)
	}
	view, err = restarted.Settings(context.Background())
	if err != nil || view.Huawei.Configured {
		t.Fatalf("restart silently restored an explicitly cleared cloud credential: configured=%t err=%v", view.Huawei.Configured, err)
	}
}

func TestAuditUsagePlainNullOverridesBootstrapDefault(t *testing.T) {
	e, _ := auditUsageService(t)
	opts := usage.Options{
		MasterKey: "audit-usage-region-key-0123456789abcdef", HuaweiRegion: "audit-original-region",
		MonthlyTotalGB: 1024, ThresholdPercent: 80, CheckTime: "09:00", Timezone: "Asia/Shanghai",
	}
	svc, err := usage.New(e.store.DB(), opts)
	if err != nil {
		t.Fatal(err)
	}
	view, err := svc.UpdateSettings(context.Background(), auditUsagePatch(t, `{"huawei_region":null}`))
	if err != nil || view.Huawei.Region != "" {
		t.Fatalf("explicit null immediately restored the deployment default: region=%q err=%v", view.Huawei.Region, err)
	}
	restarted, err := usage.New(e.store.DB(), opts)
	if err != nil {
		t.Fatal(err)
	}
	view, err = restarted.Settings(context.Background())
	if err != nil || view.Huawei.Region != "" {
		t.Fatalf("restart lost the explicit clear: region=%q err=%v", view.Huawei.Region, err)
	}
}

type auditUsageReaderFunc func(context.Context) (usage.Snapshot, error)

func (f auditUsageReaderFunc) ReadMonthlyUsage(ctx context.Context) (usage.Snapshot, error) {
	return f(ctx)
}

func TestAuditSkippedNotificationsDoNotConsumeActualRetryBudget(t *testing.T) {
	e, _ := auditUsageService(t)
	now := time.Now().UTC()
	period := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	svc, err := usage.New(e.store.DB(), usage.Options{
		MasterKey:      "audit-usage-notification-key-0123456789abcdef",
		MonthlyTotalGB: 1024, ThresholdPercent: 80, CheckTime: "09:00", Timezone: "Asia/Shanghai",
		BarkTimeout: 50 * time.Millisecond, BarkRetries: 0,
		Reader: auditUsageReaderFunc(func(context.Context) (usage.Snapshot, error) {
			return usage.Snapshot{
				PeriodStart: period, PeriodEnd: period.AddDate(0, 1, 0), AsOf: now,
				UsedGB: 900, TotalGB: 1024, Source: "audit fixture, not a cloud measurement",
			}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		if err := svc.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	// The default dial guard refuses this loopback address before TCP: this
	// exercises failed-send accounting without actually sending any push.
	if _, err := svc.UpdateSettings(context.Background(), auditUsagePatch(t,
		`{"bark_url":"https://127.0.0.1:1/audit-device","bark_enabled":true}`)); err != nil {
		t.Fatal(err)
	}
	_ = svc.RunOnce(context.Background())
	first, err := svc.Notifications(context.Background(), 20)
	if err != nil || len(first) != 1 || first[0].State != "FAILED" {
		t.Fatalf("fixture did not record a failed actual attempt: notes=%+v err=%v", first, err)
	}
	_ = svc.RunOnce(context.Background())
	second, err := svc.Notifications(context.Background(), 20)
	if err != nil || len(second) != 1 || second[0].Attempts <= first[0].Attempts {
		t.Fatalf("earlier skipped checks exhausted retries after only one real send attempt: before=%+v after=%+v err=%v", first, second, err)
	}
}

func TestAuditFailedDailyUsageReaderDoesNotHotLoop(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	now := time.Date(2026, time.November, 2, 12, 0, 0, 0, time.UTC)
	var calls atomic.Int32
	svc, err := usage.New(e.store.DB(), usage.Options{
		MonthlyTotalGB: 1024, ThresholdPercent: 80, CheckTime: "09:00", Timezone: "UTC",
		Now: func() time.Time { return now },
		Reader: auditUsageReaderFunc(func(context.Context) (usage.Snapshot, error) {
			calls.Add(1)
			return usage.Snapshot{}, errors.New("audit unavailable reader, no cloud call")
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	svc.Run(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if got := calls.Load(); got != 1 {
		t.Fatalf("a due daily usage check must attempt once and back off after failure, got %d reader calls in 150ms", got)
	}
}

func TestAuditConcurrentUsageNotificationsRespectRetryLimit(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	now := time.Now().UTC()
	period := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	const runners = 32
	ready := make(chan struct{})
	var entered atomic.Int32
	svc, err := usage.New(e.store.DB(), usage.Options{
		MasterKey:      "audit-concurrent-notification-key-0123456789abcdef",
		MonthlyTotalGB: 1024, ThresholdPercent: 80, CheckTime: "09:00", Timezone: "UTC",
		BarkURL: "https://127.0.0.1:1/audit-device", BarkEnabled: true, BarkTimeout: 50 * time.Millisecond,
		Reader: auditUsageReaderFunc(func(ctx context.Context) (usage.Snapshot, error) {
			if entered.Add(1) == runners {
				close(ready)
			}
			select {
			case <-ready:
			case <-ctx.Done():
				return usage.Snapshot{}, ctx.Err()
			}
			return usage.Snapshot{
				PeriodStart: period, PeriodEnd: period.AddDate(0, 1, 0), AsOf: now,
				UsedGB: 900, TotalGB: 1024, Source: "audit fixture, not a cloud measurement",
			}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, runners)
	for i := 0; i < runners; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- svc.RunOnce(ctx)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("fixture usage run failed: %v", err)
		}
	}
	notes, err := svc.Notifications(context.Background(), 20)
	if err != nil || len(notes) != 1 || notes[0].Attempts < 1 || notes[0].Attempts > 5 {
		t.Fatalf("concurrent checks bypassed the durable notification retry limit: notes=%+v err=%v", notes, err)
	}
}

func TestAuditLiveBarkRetryCannotLoseNotificationClaim(t *testing.T) {
	e := newEnv(t, startJudge(t), nil)
	now := time.Now().UTC()
	period := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	svc, err := usage.New(e.store.DB(), usage.Options{
		MasterKey:      "audit-live-notification-claim-key-0123456789abcdef",
		MonthlyTotalGB: 1024, ThresholdPercent: 80, CheckTime: "09:00", Timezone: "UTC",
		BarkURL: "https://127.0.0.1:1/audit-device", BarkEnabled: true,
		BarkTimeout: 50 * time.Millisecond, BarkRetries: 2, CloudTimeout: 5 * time.Millisecond,
		Reader: auditUsageReaderFunc(func(context.Context) (usage.Snapshot, error) {
			return usage.Snapshot{
				PeriodStart: period, PeriodEnd: period.AddDate(0, 1, 0), AsOf: now,
				UsedGB: 900, TotalGB: 1024, Source: "audit fixture, not a cloud measurement",
			}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstDone := make(chan struct{})
	go func() {
		_ = svc.RunOnce(firstCtx)
		close(firstDone)
	}()
	t.Cleanup(func() {
		cancelFirst()
		select {
		case <-firstDone:
		case <-time.After(3 * time.Second):
			t.Error("canceled notification fixture did not stop")
		}
	})
	deadline := time.Now().Add(time.Second)
	for {
		notes, err := svc.Notifications(context.Background(), 20)
		if err != nil {
			t.Fatal(err)
		}
		if len(notes) == 1 && notes[0].State == "SENDING" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("fixture did not acquire its initial notification claim")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// The private dial guard refuses before TCP, but bounded Bark retry
	// delays keep the first send alive much longer than the cloud timeout.
	time.Sleep(100 * time.Millisecond)
	secondCtx, cancelSecond := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelSecond()
	_ = svc.RunOnce(secondCtx)
	notes, err := svc.Notifications(context.Background(), 20)
	if err != nil || len(notes) != 1 || notes[0].Attempts != 1 {
		t.Fatalf("an unrelated cloud timeout expired a live Bark claim: notes=%+v err=%v", notes, err)
	}
}
