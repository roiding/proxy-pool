package usage

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func testUsageDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "usage.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestNewInitializesSchemaWithoutFixtureSetup(t *testing.T) {
	db := testUsageDB(t)
	if _, err := New(db, Options{
		MasterKey: "0123456789abcdef0123456789abcdef",
		CheckTime: "09:00", Timezone: "Asia/Shanghai",
	}); err != nil {
		t.Fatalf("New must create its own schema: %v", err)
	}
	for _, table := range []string{
		"usage_settings", "usage_snapshots", "usage_status", "usage_notifications",
	} {
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master
			WHERE type='table' AND name=?`, table).Scan(&n); err != nil || n != 1 {
			t.Fatalf("usage table %s missing: n=%d err=%v", table, n, err)
		}
	}
}

func TestClearSentinelSurvivesBootstrapAndIsRemovedOnReplace(t *testing.T) {
	db := testUsageDB(t)
	opts := Options{
		MasterKey:        "0123456789abcdef0123456789abcdef",
		HuaweiAK:         "bootstrap-ak",
		HuaweiSK:         "bootstrap-sk",
		HuaweiRegion:     "bootstrap-region",
		ThresholdPercent: 80,
		CheckTime:        "09:00", Timezone: "Asia/Shanghai",
	}
	svc, err := New(db, opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	view, err := svc.UpdateSettings(ctx, SettingsUpdate{
		HuaweiAK:     OptionalString{Present: true, Null: true},
		HuaweiSK:     OptionalString{Present: true, Null: true},
		HuaweiRegion: OptionalString{Present: true, Null: true},
	})
	if err != nil || view.Huawei.Configured || view.Huawei.Region != "" {
		t.Fatalf("explicit clears did not apply: view=%+v err=%v", view, err)
	}
	restarted, err := New(db, opts)
	if err != nil {
		t.Fatal(err)
	}
	view, err = restarted.Settings(ctx)
	if err != nil || view.Huawei.Configured || view.Huawei.Region != "" {
		t.Fatalf("restart revived bootstrap values: view=%+v err=%v", view, err)
	}
	view, err = restarted.UpdateSettings(ctx, SettingsUpdate{
		HuaweiAK:     OptionalString{Present: true, Value: "replacement-ak"},
		HuaweiSK:     OptionalString{Present: true, Value: "replacement-sk"},
		HuaweiRegion: OptionalString{Present: true, Value: "cn-north-4"},
	})
	if err != nil || !view.Huawei.Configured || view.Huawei.Region != "cn-north-4" {
		t.Fatalf("replacement did not clear sentinel: view=%+v err=%v", view, err)
	}
	var sentinels int
	if err := db.QueryRow(`SELECT COUNT(*) FROM usage_settings WHERE key LIKE '%:cleared'`).Scan(&sentinels); err != nil {
		t.Fatal(err)
	}
	if sentinels != 0 {
		t.Fatalf("replacement left %d clear sentinels", sentinels)
	}
}

func TestSkippedNotificationDoesNotConsumeAttemptBudget(t *testing.T) {
	db := testUsageDB(t)
	now := time.Now().UTC()
	period := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	svc, err := New(db, Options{
		MasterKey:      "0123456789abcdef0123456789abcdef",
		MonthlyTotalGB: 1024, ThresholdPercent: 80,
		CheckTime: "09:00", Timezone: "Asia/Shanghai",
		Reader: snapshotReader{Snapshot{
			PeriodStart: period, PeriodEnd: period.AddDate(0, 1, 0), AsOf: now,
			UsedGB: 900, TotalGB: 1024, Source: "unit fixture",
		}},
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < maxNotificationAttempts+2; i++ {
		if err := svc.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	var state string
	var attempts int
	if err := db.QueryRow(`SELECT state, attempts FROM usage_notifications`).Scan(&state, &attempts); err != nil {
		t.Fatal(err)
	}
	if state != "SKIPPED" || attempts != 0 {
		t.Fatalf("unsent notifications consumed retry budget: state=%s attempts=%d", state, attempts)
	}
}

func TestFailedReaderBackoffDoesNotHotLoop(t *testing.T) {
	db := testUsageDB(t)
	now := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	var calls atomic.Int32
	reader := readerFunc(func(context.Context) (Snapshot, error) {
		calls.Add(1)
		return Snapshot{}, ErrUsageReaderUnavailable
	})
	svc, err := New(db, Options{
		MasterKey: "0123456789abcdef0123456789abcdef",
		CheckTime: "09:00", Timezone: "UTC",
		Reader: reader,
		Now:    func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	svc.Run(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)))
	// The first overdue check runs immediately; the persisted 30s backoff
	// suppresses every subsequent wakeup during this short test.
	if got := calls.Load(); got != 1 {
		t.Fatalf("failed reader hot-looped: calls=%d in 150ms", got)
	}
	// A restart in the same window must also observe the persistent backoff.
	restarted, err := New(db, Options{
		MasterKey: "0123456789abcdef0123456789abcdef",
		CheckTime: "09:00", Timezone: "UTC",
		Reader: reader,
		Now:    func() time.Time { return now.Add(time.Second) },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel2()
	restarted.Run(ctx2, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if got := calls.Load(); got != 1 {
		t.Fatalf("restart bypassed persisted failure backoff: calls=%d", got)
	}
}

func TestBarkSendBudgetCoversEveryAttemptAndRetryWait(t *testing.T) {
	// timeout=50ms and retries=2 means three HTTP attempts plus 500ms and
	// 1s waits. The claim must not be tied to a much smaller cloud timeout.
	got := BarkSendBudget(50*time.Millisecond, 2)
	want := 150*time.Millisecond + 1500*time.Millisecond
	if got != want {
		t.Fatalf("BarkSendBudget=%s want=%s", got, want)
	}
	if huge := BarkSendBudget(time.Hour, 1000); huge > 10*time.Minute {
		t.Fatalf("BarkSendBudget is unbounded: %s", huge)
	}
}

func TestNotificationOutboxAtomicRetryLimitStaysDeduped(t *testing.T) {
	db := testUsageDB(t)
	now := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	period := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	svc, err := New(db, Options{
		MasterKey:      "0123456789abcdef0123456789abcdef",
		MonthlyTotalGB: 1024, ThresholdPercent: 80,
		CheckTime: "09:00", Timezone: "UTC",
		Reader: snapshotReader{Snapshot{
			PeriodStart: period, PeriodEnd: period.AddDate(0, 1, 0), AsOf: now,
			UsedGB: 900, TotalGB: 1024, Source: "unit fixture",
		}},
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := svc.RunOnce(ctx); err != nil { // initial SKIPPED row, no attempts
		t.Fatal(err)
	}
	if _, err := svc.UpdateSettings(ctx, SettingsUpdate{
		BarkURL:     OptionalString{Present: true, Value: "https://127.0.0.1:1/unit-device"},
		BarkEnabled: OptionalBool{Present: true, Value: true},
	}); err != nil {
		t.Fatal(err)
	}
	// The skip is not a retry; enabling Bark makes the same row eligible.
	_ = svc.RunOnce(ctx)
	var state string
	var attempts int
	var idleClaim string
	if err := db.QueryRow(`SELECT state, attempts, claim_token FROM usage_notifications`).
		Scan(&state, &attempts, &idleClaim); err != nil {
		t.Fatal(err)
	}
	if state != "FAILED" || attempts != 1 || idleClaim != "" {
		t.Fatalf("real send failure accounting: state=%s attempts=%d claim=%q", state, attempts, idleClaim)
	}
	// Repeated scans consume the bounded real-attempt budget one claim at a
	// time and never duplicate the deduplicated outbox row.
	for i := 0; i < maxNotificationAttempts+2; i++ {
		_ = svc.RunOnce(ctx)
	}
	var rows int
	if err := db.QueryRow(`SELECT COUNT(*) FROM usage_notifications`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("outbox dedup broke: rows=%d", rows)
	}
	if err := db.QueryRow(`SELECT state, attempts, claim_token FROM usage_notifications`).
		Scan(&state, &attempts, &idleClaim); err != nil {
		t.Fatal(err)
	}
	if state != "FAILED" || attempts != maxNotificationAttempts {
		t.Fatalf("outbox retry limit not enforced: state=%s attempts=%d", state, attempts)
	}
}

type snapshotReader struct{ snap Snapshot }

func (r snapshotReader) ReadMonthlyUsage(context.Context) (Snapshot, error) { return r.snap, nil }

type readerFunc func(context.Context) (Snapshot, error)

func (f readerFunc) ReadMonthlyUsage(ctx context.Context) (Snapshot, error) { return f(ctx) }
