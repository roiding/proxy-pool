// Package usage owns traffic accounting, its encrypted configuration and
// notification delivery. It has its own schema so it never fights the
// central allocator migration, and it never touches the acquire path:
// thresholds only produce notifications.
package usage

import (
	"database/sql"
	"fmt"
	"time"
)

// EnsureSchema creates the usage tables. It is deliberately independent of
// the central sqlite.Store migrations so both can run at startup without
// coordinating versions.
func EnsureSchema(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("usage schema: %w", err)
	}
	defer tx.Rollback()
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS usage_settings (
			key        TEXT PRIMARY KEY,
			value      TEXT NOT NULL,
			secret     INTEGER NOT NULL DEFAULT 0,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS usage_snapshots (
			id          INTEGER PRIMARY KEY AUTOINCREMENT,
			period_start TEXT NOT NULL,
			period_end   TEXT NOT NULL,
			as_of        TEXT NOT NULL,
			queried_at   TEXT NOT NULL,
			used_gb      REAL NOT NULL,
			total_gb     REAL NOT NULL,
			remaining_gb REAL,
			source       TEXT NOT NULL,
			note         TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE INDEX IF NOT EXISTS idx_usage_snapshots_queried
			ON usage_snapshots(queried_at DESC)`,
		`CREATE TABLE IF NOT EXISTS usage_status (
			id              INTEGER PRIMARY KEY CHECK (id = 1),
			last_attempt_at TEXT NOT NULL,
			last_success_at TEXT,
			last_error      TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE TABLE IF NOT EXISTS usage_scheduler (
			id              INTEGER PRIMARY KEY CHECK (id = 1),
			next_attempt_at TEXT NOT NULL,
			failures        INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS usage_notifications (
			id           INTEGER PRIMARY KEY AUTOINCREMENT,
			dedup_key    TEXT NOT NULL UNIQUE,
			kind         TEXT NOT NULL,
			title        TEXT NOT NULL,
			body         TEXT NOT NULL,
			state        TEXT NOT NULL,
			attempts     INTEGER NOT NULL DEFAULT 0,
			next_attempt_at TEXT,
			claim_token  TEXT NOT NULL DEFAULT '',
			claim_until  TEXT,
			last_error   TEXT NOT NULL DEFAULT '',
			created_at   TEXT NOT NULL,
			delivered_at TEXT
		)`,
		`CREATE INDEX IF NOT EXISTS idx_usage_notifications_created
			ON usage_notifications(created_at DESC)`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return fmt.Errorf("usage schema: %w", err)
		}
	}
	// Additive migration for usage tables created by the first refactor
	// build. A duplicate-column error means the column already exists.
	if err := addColumnIfMissing(tx,
		"usage_notifications", "next_attempt_at",
		`ALTER TABLE usage_notifications ADD COLUMN next_attempt_at TEXT`); err != nil {
		return fmt.Errorf("usage schema: %w", err)
	}
	if err := addColumnIfMissing(tx,
		"usage_notifications", "claim_token",
		`ALTER TABLE usage_notifications ADD COLUMN claim_token TEXT NOT NULL DEFAULT ''`); err != nil {
		return fmt.Errorf("usage schema: %w", err)
	}
	if err := addColumnIfMissing(tx,
		"usage_notifications", "claim_until",
		`ALTER TABLE usage_notifications ADD COLUMN claim_until TEXT`); err != nil {
		return fmt.Errorf("usage schema: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("usage schema commit: %w", err)
	}
	return nil
}

func addColumnIfMissing(tx *sql.Tx, table, column, ddl string) error {
	var n int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = ?`,
		table, column).Scan(&n); err != nil {
		return err
	}
	if n == 0 {
		_, err := tx.Exec(ddl)
		return err
	}
	return nil
}

func fmtTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func scanTimePtr(value any) (*time.Time, error) {
	if value == nil {
		return nil, nil
	}
	s, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("usage: unexpected timestamp type %T", value)
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return nil, err
	}
	return &t, nil
}
