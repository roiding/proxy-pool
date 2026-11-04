package usage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"proxypool/internal/domain"
)

// Options carries the deployment-config bootstrap values. Secret seeds are
// encrypted into the settings table on first boot; the master key itself is
// never persisted anywhere.
type Options struct {
	MasterKey        string
	BarkURL          string
	BarkEnabled      bool
	HuaweiAK         string
	HuaweiSK         string
	HuaweiRegion     string
	HuaweiResourceID string
	HuaweiPackageID  string
	MonthlyTotalGB   int64
	ThresholdPercent int64
	CheckTime        string
	Timezone         string
	BarkTimeout      time.Duration
	BarkRetries      int
	// CloudTimeout bounds one usage reader call; failures keep the last
	// valid snapshot and mark it stale.
	CloudTimeout time.Duration
	// Reader overrides the production reader (tests only).
	Reader UsageReader
	Now    func() time.Time
}

type Service struct {
	db           *sql.DB
	box          *sealedBox
	reader       UsageReader
	defaults     Options
	masterKeySet bool
	Now          func() time.Time
}

func New(db *sql.DB, opts Options) (*Service, error) {
	if err := EnsureSchema(db); err != nil {
		return nil, err
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	s := &Service{db: db, reader: unavailableReader{}, defaults: opts, Now: opts.Now}
	if opts.MasterKey != "" {
		box, err := newSealedBox(opts.MasterKey)
		if err != nil {
			return nil, err
		}
		s.box = box
		s.masterKeySet = true
	}
	if opts.Reader != nil {
		s.reader = opts.Reader
	}
	if err := s.seed(context.Background()); err != nil {
		return nil, err
	}
	return s, nil
}

// seed writes deployment defaults once. Existing rows and persistent
// "cleared" sentinels always win: an admin clear is never silently reversed
// by the deployment config on restart.
func (s *Service) seed(c context.Context) error {
	now := s.Now()
	seeds := []struct {
		key    string
		value  string
		secret bool
		set    bool
	}{
		{keyBarkEnabled, boolStr(s.defaults.BarkEnabled), false, true},
		{keyMonthlyTotalGB, fmtInt(s.defaults.MonthlyTotalGB), false, s.defaults.MonthlyTotalGB > 0},
		{keyThresholdPct, fmtInt(s.defaults.ThresholdPercent), false, s.defaults.ThresholdPercent > 0},
		{keyCheckTime, s.defaults.CheckTime, false, s.defaults.CheckTime != ""},
		{keyTimezone, s.defaults.Timezone, false, s.defaults.Timezone != ""},
		{keyHuaweiRegion, s.defaults.HuaweiRegion, false, s.defaults.HuaweiRegion != ""},
		{keyHuaweiResource, s.defaults.HuaweiResourceID, false, s.defaults.HuaweiResourceID != ""},
		{keyHuaweiPackage, s.defaults.HuaweiPackageID, false, s.defaults.HuaweiPackageID != ""},
		{keyBarkURL, s.defaults.BarkURL, true, s.defaults.BarkURL != ""},
		{keyHuaweiAK, s.defaults.HuaweiAK, true, s.defaults.HuaweiAK != ""},
		{keyHuaweiSK, s.defaults.HuaweiSK, true, s.defaults.HuaweiSK != ""},
	}
	for _, sd := range seeds {
		if !sd.set {
			continue
		}
		cleared, err := s.settingCleared(c, sd.key)
		if err != nil {
			return err
		}
		if cleared {
			continue
		}
		stored := sd.value
		if sd.secret {
			if s.box == nil {
				// Without a master key the plaintext secret is never
				// written; the operator must set it via the admin API.
				continue
			}
			sealed, err := s.box.seal(sd.key, sd.value)
			if err != nil {
				return err
			}
			stored = sealed
		}
		if _, err := s.db.ExecContext(c, `
			INSERT INTO usage_settings(key, value, secret, updated_at) VALUES(?,?,?,?)
			ON CONFLICT(key) DO NOTHING`,
			sd.key, stored, boolInt(sd.secret), fmtTime(now)); err != nil {
			return fmt.Errorf("usage: seed %s: %w", sd.key, err)
		}
	}
	return nil
}

// SnapshotView is the admin read model of one usage sample.
type SnapshotView struct {
	PeriodStart string   `json:"period_start"`
	PeriodEnd   string   `json:"period_end"`
	AsOf        string   `json:"as_of"`
	QueriedAt   string   `json:"queried_at"`
	UsedGB      float64  `json:"used_gb"`
	TotalGB     float64  `json:"total_gb"`
	RemainingGB *float64 `json:"remaining_gb,omitempty"`
	Source      string   `json:"source"`
	Note        string   `json:"note,omitempty"`
}

type NotificationView struct {
	ID          int64   `json:"id"`
	Kind        string  `json:"kind"`
	Title       string  `json:"title"`
	Body        string  `json:"body"`
	State       string  `json:"state"`
	Attempts    int     `json:"attempts"`
	LastError   string  `json:"last_error,omitempty"`
	CreatedAt   string  `json:"created_at"`
	DeliveredAt *string `json:"delivered_at,omitempty"`
}

type StatusView struct {
	Snapshot      *SnapshotView      `json:"snapshot"`
	Stale         bool               `json:"stale"`
	LastAttemptAt *string            `json:"last_attempt_at,omitempty"`
	LastSuccessAt *string            `json:"last_success_at,omitempty"`
	LastError     string             `json:"last_error,omitempty"`
	ReaderStatus  string             `json:"reader_status"`
	ReaderNote    string             `json:"reader_note"`
	Notifications []NotificationView `json:"notifications"`
}

const staleAfter = 48 * time.Hour

// Status reports the latest snapshot, its freshness and recent notifications.
func (s *Service) Status(c context.Context) (StatusView, error) {
	var out StatusView
	out.ReaderStatus = "disabled"
	out.ReaderNote = readerUnavailableNote
	row := s.db.QueryRowContext(c, `
		SELECT period_start, period_end, as_of, queried_at, used_gb, total_gb,
		  remaining_gb, source, note
		FROM usage_snapshots ORDER BY id DESC LIMIT 1`)
	var snap SnapshotView
	var remaining sql.NullFloat64
	err := row.Scan(&snap.PeriodStart, &snap.PeriodEnd, &snap.AsOf, &snap.QueriedAt,
		&snap.UsedGB, &snap.TotalGB, &remaining, &snap.Source, &snap.Note)
	switch {
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		return StatusView{}, err
	default:
		if remaining.Valid {
			v := remaining.Float64
			snap.RemainingGB = &v
		}
		out.Snapshot = &snap
	}
	var lastAttempt, lastSuccess sql.NullString
	var lastError string
	err = s.db.QueryRowContext(c,
		`SELECT last_attempt_at, last_success_at, last_error FROM usage_status WHERE id = 1`).
		Scan(&lastAttempt, &lastSuccess, &lastError)
	switch {
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		return StatusView{}, err
	default:
		out.LastAttemptAt = nullStrPtr(lastAttempt)
		out.LastSuccessAt = nullStrPtr(lastSuccess)
		out.LastError = lastError
	}
	if out.Snapshot != nil {
		if asOfTime, perr := time.Parse(time.RFC3339Nano, out.Snapshot.AsOf); perr == nil {
			out.Stale = lastError != "" || s.Now().Sub(asOfTime) > staleAfter
		} else {
			out.Stale = true
		}
	} else {
		out.Stale = true
	}
	notes, err := s.Notifications(c, 20)
	if err != nil {
		return StatusView{}, err
	}
	out.Notifications = notes
	return out, nil
}

// Notifications lists recent notification records.
func (s *Service) Notifications(c context.Context, limit int) ([]NotificationView, error) {
	if limit < 1 || limit > 200 {
		limit = 20
	}
	rows, err := s.db.QueryContext(c, `
		SELECT id, kind, title, body, state, attempts, last_error, created_at, delivered_at
		FROM usage_notifications ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []NotificationView{}
	for rows.Next() {
		var n NotificationView
		var delivered sql.NullString
		if err := rows.Scan(&n.ID, &n.Kind, &n.Title, &n.Body, &n.State, &n.Attempts,
			&n.LastError, &n.CreatedAt, &delivered); err != nil {
			return nil, err
		}
		n.DeliveredAt = nullStrPtr(delivered)
		out = append(out, n)
	}
	return out, rows.Err()
}

// TestBark sends one real test push through the configured Bark endpoint.
func (s *Service) TestBark(c context.Context, title, body string) error {
	st, err := s.loadSettings(c)
	if err != nil {
		return err
	}
	if st.BarkURL == "" {
		return errors.New("usage: bark_url is not configured")
	}
	client, err := NewBarkClient(st.BarkURL, s.defaults.BarkTimeout, s.defaults.BarkRetries, false)
	if err != nil {
		return err
	}
	if title == "" {
		title = "proxy-pool 测试通知"
	}
	if body == "" {
		body = "这是一条 proxy-pool 管理端测试推送。"
	}
	return client.Push(c, title, body)
}

// RunOnce performs one usage cycle: read, snapshot, evaluate, notify. A
// reader failure keeps the previous snapshot untouched and records the
// failure — usage data never degrades to zero.
func (s *Service) RunOnce(c context.Context) error {
	now := s.Now()
	if err := s.recordStatusAttempt(c, now); err != nil {
		return err
	}
	readCtx, cancel := context.WithTimeout(c, s.cloudTimeout())
	defer cancel()
	snap, err := s.reader.ReadMonthlyUsage(readCtx)
	if err != nil {
		return s.recordStatusError(c, now, err)
	}
	if err := validateSample(snap, now); err != nil {
		return s.recordStatusError(c, now, err)
	}
	if _, err := s.db.ExecContext(c, `
		INSERT INTO usage_snapshots(period_start, period_end, as_of, queried_at,
		  used_gb, total_gb, remaining_gb, source, note)
		VALUES(?,?,?,?,?,?,?,?,?)`,
		fmtTime(snap.PeriodStart), fmtTime(snap.PeriodEnd), fmtTime(snap.AsOf),
		fmtTime(now), snap.UsedGB, snap.TotalGB, snap.RemainingGB, snap.Source,
		snap.Note); err != nil {
		return err
	}
	if err := s.recordStatusSuccess(c, now); err != nil {
		return err
	}
	return s.evaluateThreshold(c, now, snap)
}

func (s *Service) evaluateThreshold(c context.Context, now time.Time, snap Snapshot) error {
	st, err := s.loadSettings(c)
	if err != nil {
		return err
	}
	// The configured monthly total wins when set: the config value is the
	// operator's billing basis, the reader total is a fallback.
	total := snap.TotalGB
	if st.MonthlyTotalGB > 0 {
		total = float64(st.MonthlyTotalGB)
	}
	if total <= 0 {
		return nil
	}
	pct := int64(snap.UsedGB * 100 / total)
	if pct < st.ThresholdPct {
		return nil
	}
	dedup := fmt.Sprintf("usage_threshold|%s|%d",
		snap.PeriodStart.UTC().Format("2006-01"), st.ThresholdPct)
	body := fmt.Sprintf("已用 %.1f GB / %.1f GB（%d%%），超过告警阈值 %d%%。数据口径：%s。",
		snap.UsedGB, total, pct, st.ThresholdPct, snap.Source)
	title := "代理池云流量告警"
	res, err := s.db.ExecContext(c, `
		INSERT INTO usage_notifications(dedup_key, kind, title, body, state, attempts,
		  next_attempt_at, created_at)
		VALUES(?,?,?,?,?,0,?,?)
		ON CONFLICT(dedup_key) DO NOTHING`,
		dedup, "usage_threshold", title, body, "PENDING", fmtTime(now), fmtTime(now))
	if err != nil {
		return err
	}
	inserted, err := res.RowsAffected()
	if err != nil {
		return err
	}
	_ = inserted
	return s.deliverNotification(c, dedup, title, body, now)
}

// maxNotificationAttempts bounds real Bark send retries per notification;
// SKIPPED rows (never actually sent) are exempt so a later enable still
// delivers the pending alert.
const maxNotificationAttempts = 5

// deliverNotification claims one notification row atomically, then performs
// I/O outside SQLite. A claim consumes one real-attempt slot before send, so
// concurrent RunOnce callers can never exceed maxNotificationAttempts. Only
// the claim owner may write the final state.
func (s *Service) deliverNotification(c context.Context, dedup, title, body string, now time.Time) error {
	st, err := s.loadSettings(c)
	if err != nil {
		return err
	}
	if st.BarkURL == "" || !st.BarkEnabled {
		// SKIPPED consumes no actual-send budget and does not need an
		// exclusive claim: it cannot send anything.
		_, err := s.db.ExecContext(c, `
			UPDATE usage_notifications SET state = 'SKIPPED',
			  next_attempt_at = NULL, last_error = ?
			WHERE dedup_key = ? AND state IN ('PENDING', 'SKIPPED')`,
			"bark_url is not configured or notifications are disabled", dedup)
		return err
	}
	sendBudget := BarkSendBudget(s.defaults.BarkTimeout, s.defaults.BarkRetries)
	token, err := s.claimNotification(c, dedup, now, sendBudget)
	if err != nil || token == "" {
		return err
	}
	finalize := func(state, msg string, delivered bool) error {
		var next any
		var deliveredAt any
		if delivered {
			deliveredAt = fmtTime(now)
		}
		_, err = s.db.ExecContext(c, `
			UPDATE usage_notifications SET state = ?, next_attempt_at = ?,
			  delivered_at = ?, last_error = ?, claim_token = '', claim_until = NULL
			WHERE dedup_key = ? AND claim_token = ?`,
			state, next, deliveredAt, msg, dedup, token)
		if c.Err() != nil && err != nil {
			// The caller's canceled request would otherwise strand the claim
			// until expiry. Release with a bounded cleanup context while
			// keeping the already-consumed attempt slot.
			cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, _ = s.db.ExecContext(cleanup, `
				UPDATE usage_notifications SET state = ?, last_error = ?,
				  claim_token = '', claim_until = NULL
				WHERE dedup_key = ? AND claim_token = ?`,
				state, msg, dedup, token)
		}
		return err
	}
	client, err := NewBarkClient(st.BarkURL, s.defaults.BarkTimeout, s.defaults.BarkRetries, false)
	if err != nil {
		return finalize("FAILED", err.Error(), false)
	}
	pushCtx, cancelPush := context.WithTimeout(c, sendBudget)
	defer cancelPush()
	if err := client.Push(pushCtx, title, body); err != nil {
		return finalize("FAILED", err.Error(), false)
	}
	return finalize("DELIVERED", "", true)
}

// claimNotification atomically reserves one real-send slot. An expired claim
// is recoverable after restart, while a live claim blocks every other caller.
func (s *Service) claimNotification(c context.Context, dedup string, now time.Time,
	sendBudget time.Duration) (string, error) {
	token := domain.NewID("note_")
	// The claim must cover the entire sender budget and leave a small margin
	// for the owner to persist its terminal state. It is independent of the
	// cloud reader timeout.
	deadline := now.Add(sendBudget + 5*time.Second)
	res, err := s.db.ExecContext(c, `
		UPDATE usage_notifications
		SET claim_token = ?, claim_until = ?, attempts = attempts + 1,
		    state = 'SENDING', next_attempt_at = NULL
		WHERE dedup_key = ?
		  AND state <> 'DELIVERED'
		  AND attempts < ?
		  AND (claim_token = '' OR claim_until IS NULL OR claim_until <= ?)`,
		token, fmtTime(deadline), dedup, maxNotificationAttempts,
		fmtTime(now))
	if err != nil {
		return "", err
	}
	n, err := res.RowsAffected()
	if err != nil || n == 0 {
		return "", err
	}
	return token, nil
}

func (s *Service) recordStatusAttempt(c context.Context, now time.Time) error {
	_, err := s.db.ExecContext(c, `
		INSERT INTO usage_status(id, last_attempt_at, last_success_at, last_error)
		VALUES(1, ?, NULL, '')
		ON CONFLICT(id) DO UPDATE SET last_attempt_at = excluded.last_attempt_at`,
		fmtTime(now))
	return err
}

func (s *Service) recordStatusError(c context.Context, now time.Time, rerr error) error {
	if _, err := s.db.ExecContext(c,
		`UPDATE usage_status SET last_error = ? WHERE id = 1`, rerr.Error()); err != nil {
		return err
	}
	return rerr
}

func (s *Service) recordStatusSuccess(c context.Context, now time.Time) error {
	_, err := s.db.ExecContext(c,
		`UPDATE usage_status SET last_success_at = ?, last_error = '' WHERE id = 1`,
		fmtTime(now))
	return err
}

// Run schedules the daily check. A reader that is unavailable still marks a
// failed attempt so the console shows the truth instead of silence. Failed
// attempts back off exponentially and the next attempt is persisted, so a
// restart cannot turn an already-passed daily slot into a hot loop.
func (s *Service) Run(c context.Context, log *slog.Logger) {
	for {
		next, wait, due, ok := s.nextCheck(c)
		if !ok {
			return
		}
		if wait < 0 {
			wait = 0
		}
		// Re-read settings at most once a minute. A firing timer only makes
		// the loop re-evaluate the schedule; it never directly invokes the
		// reader while a failed attempt's persisted backoff is still active.
		timer := time.NewTimer(min(wait, settingsReloadInterval))
		select {
		case <-c.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		timer.Stop()
		if !due {
			continue
		}
		if err := s.RunOnce(c); err != nil {
			delay := s.recordSchedulerFailure(c, err)
			if delay <= 0 {
				delay = schedulerRetryFallback
			}
			log.Warn("usage check failed", "next", next.Format(time.RFC3339),
				"retry_in", delay, "error", err)
			if !sleepContext(c, delay) {
				return
			}
		} else {
			s.clearSchedulerFailure(c)
			log.Info("usage check completed")
		}
	}
}

// settingsReloadInterval bounds how late a changed daily check time can be
// observed by the long-running scheduler.
const settingsReloadInterval = time.Minute

const (
	usageRetryBase = 30 * time.Second
	usageRetryMax  = 30 * time.Minute
)

// schedulerRetryFallback is used when persisting backoff fails. It keeps the
// loop bounded even when the scheduler table itself is temporarily unwritable.
var schedulerRetryFallback = usageRetryMax

// nextCheck computes the next slot and whether its check is due. A persisted
// scheduler backoff takes precedence over an overdue daily slot.
func (s *Service) nextCheck(c context.Context) (time.Time, time.Duration, bool, bool) {
	st, err := s.loadSettings(c)
	if err != nil {
		return time.Time{}, 0, false, false
	}
	clock, err := checkTimeSpec(st.CheckTime, st.Timezone)
	if err != nil {
		return time.Time{}, 0, false, false
	}
	loc := time.Local
	if st.Timezone != "" {
		if loaded, lerr := time.LoadLocation(st.Timezone); lerr == nil {
			loc = loaded
		}
	}
	next := s.nextScheduledAfterLastSuccess(c, loc, clock)
	wait := next.Sub(s.Now())
	if wait <= 0 {
		wait = 0
	}
	var nextAttempt sql.NullString
	if err := s.db.QueryRowContext(c,
		`SELECT next_attempt_at FROM usage_scheduler WHERE id = 1`).
		Scan(&nextAttempt); err == nil && nextAttempt.Valid {
		at, perr := time.Parse(time.RFC3339Nano, nextAttempt.String)
		if perr == nil && at.After(s.Now()) {
			return next, at.Sub(s.Now()), false, true
		}
	}
	return next, wait, wait == 0, true
}

// nextScheduledAfterLastSuccess recovers a missed daily run: if today's slot
// has passed and no successful cycle happened after that slot, it returns
// today's slot and RunOnce starts immediately.
func (s *Service) nextScheduledAfterLastSuccess(c context.Context, loc *time.Location,
	clock time.Time) time.Time {
	now := s.Now().In(loc)
	next := time.Date(now.Year(), now.Month(), now.Day(),
		clock.Hour(), clock.Minute(), 0, 0, loc)
	var lastSuccess sql.NullString
	if err := s.db.QueryRowContext(c,
		`SELECT last_success_at FROM usage_status WHERE id = 1`).Scan(&lastSuccess); err == nil && lastSuccess.Valid {
		if success, perr := time.Parse(time.RFC3339Nano, lastSuccess.String); perr == nil &&
			success.In(loc).After(next) {
			return next.Add(24 * time.Hour)
		}
	}
	return next
}

func (s *Service) recordSchedulerFailure(c context.Context, cause error) time.Duration {
	var failures int
	err := s.db.QueryRowContext(c,
		`SELECT failures FROM usage_scheduler WHERE id = 1`).Scan(&failures)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0
	}
	failures++
	delay := usageRetryBase
	for i := 1; i < failures && delay < usageRetryMax; i++ {
		delay *= 2
		if delay > usageRetryMax {
			delay = usageRetryMax
		}
	}
	next := s.Now().Add(delay)
	if _, uerr := s.db.ExecContext(c, `
		INSERT INTO usage_scheduler(id, next_attempt_at, failures) VALUES(1, ?, ?)
		ON CONFLICT(id) DO UPDATE SET next_attempt_at = excluded.next_attempt_at,
		  failures = excluded.failures`,
		fmtTime(next), failures); uerr != nil {
		return 0
	}
	// Keep the error visible in status with a bounded category, never a
	// retry storm in logs or SQLite.
	_ = cause
	return delay
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (s *Service) clearSchedulerFailure(c context.Context) {
	_, _ = s.db.ExecContext(c, `DELETE FROM usage_scheduler WHERE id = 1`)
}

func (s *Service) cloudTimeout() time.Duration {
	if s.defaults.CloudTimeout <= 0 {
		return 30 * time.Second
	}
	return s.defaults.CloudTimeout
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func nullStrPtr(v sql.NullString) *string {
	if !v.Valid {
		return nil
	}
	out := v.String
	return &out
}

func unmarshalString(data []byte, dst *string) error {
	return json.Unmarshal(data, dst)
}

func unmarshalBool(data []byte, dst *bool) error {
	return json.Unmarshal(data, dst)
}

func unmarshalInt64(data []byte, dst *int64) error {
	return json.Unmarshal(data, dst)
}
