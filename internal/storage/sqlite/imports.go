package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
	"proxypool/internal/domain"
)

// Durable import-batch bounds. The application layer enforces the request
// shape; these constants govern what is retained per batch and for how long.
const (
	// MaxImportStoredErrors bounds the per-batch error array; the rejected
	// count stays complete even when individual errors are dropped.
	MaxImportStoredErrors = 100
	// ImportRecordRetention bounds how long batch results and idempotency
	// records are kept for replay and result queries.
	ImportRecordRetention = 7 * 24 * time.Hour
	// maxImportCleanupRows bounds one cleanup pass so an import cannot turn
	// into an unbounded delete transaction.
	maxImportCleanupRows = 500
)

type ImportBatchApply struct {
	SourceID       string
	UploaderID     string
	UploadKeyID    string
	SourceVersion  int64
	IdempotencyKey string
	ContentHash    string
	Candidates     []domain.NormalizedCandidate
	Rejected       int
	Errors         []ImportBatchError
	Now            time.Time
}

// queryable accepts either the pool or an open transaction for read helpers.
type queryable interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type ImportBatchError struct {
	Index int    `json:"index"`
	Input string `json:"input,omitempty"`
	Error string `json:"error"`
}

type ApplyImportBatchResult struct {
	Replayed  bool
	ImportID  string
	Added     int
	Updated   int
	Duplicate int
	Rejected  int
	Errors    []ImportBatchError
	CreatedAt time.Time
}

// ApplyImportBatch appends one durable import inside a single transaction:
// idempotency resolution, inventory changes, and the batch result record
// commit or roll back together, so a network retry can never double-apply.
// A same-key replay returns the stored result with Replayed=true; a same-key
// different-content request is a 409, never a second application.
// Authorization and the source snapshot are rechecked before idempotency
// lookup, so a key revoke or source edit rejects both new applies and replays.
func (s *Store) ApplyImportBatch(ctx context.Context, req ImportBatchApply) (ApplyImportBatchResult, error) {
	var out ApplyImportBatchResult
	err := s.tx(ctx, func(t *sql.Tx) error {
		out = ApplyImportBatchResult{}
		if err := ensureImportSchemaTx(ctx, t); err != nil {
			return err
		}
		if err := importFinalAuthorizationGuardTx(ctx, t, req); err != nil {
			return err
		}
		if req.IdempotencyKey != "" {
			var storedHash, storedImportID, storedCreatedAt string
			err := t.QueryRowContext(ctx, `
				SELECT content_hash, import_id, created_at FROM import_idempotency
				WHERE uploader_id = ? AND source_id = ? AND idempotency_key = ?`,
				req.UploaderID, req.SourceID, req.IdempotencyKey).
				Scan(&storedHash, &storedImportID, &storedCreatedAt)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			if err == nil {
				keyCreatedAt, terr := time.Parse(time.RFC3339Nano, storedCreatedAt)
				if terr != nil {
					return fmt.Errorf("decode idempotency time: %w", terr)
				}
				keyExpired := !keyCreatedAt.Add(ImportRecordRetention).After(req.Now)
				if !keyExpired {
					if storedHash != req.ContentHash {
						return domain.E("IDEMPOTENCY_KEY_CONFLICT", 409,
							"idempotency key was already used with different content for this source")
					}
					batch, berr := importBatchByIDTx(ctx, t, req.SourceID, storedImportID)
					if berr != nil {
						return berr
					}
					if batch.CreatedAt.Add(ImportRecordRetention).After(req.Now) {
						// Bounded cleanup also runs on replays, so a workload
						// of identical retries still expires old records.
						if err := cleanupImportRecordsTx(ctx, t, req.Now); err != nil {
							return err
						}
						out = ApplyImportBatchResult{
							Replayed:  true,
							ImportID:  batch.ImportID,
							Added:     batch.Added,
							Updated:   batch.Updated,
							Duplicate: batch.Duplicate,
							Rejected:  batch.Rejected,
							Errors:    batch.Errors,
							CreatedAt: batch.CreatedAt,
						}
						return nil
					}
					// The stored batch aged out of the retention window (or
					// was bounded-purged): the key is beyond its published
					// window, so the same content applies as a new batch.
				}
				if _, err := t.ExecContext(ctx, `
					DELETE FROM import_idempotency
					WHERE uploader_id = ? AND source_id = ? AND idempotency_key = ?`,
					req.UploaderID, req.SourceID, req.IdempotencyKey); err != nil {
					return err
				}
			}
		}
		counts, err := importBatchCandidatesTx(ctx, t, req.Candidates, req.Now)
		if err != nil {
			return err
		}
		importID := domain.NewID("imp_")
		errs := req.Errors
		if len(errs) > MaxImportStoredErrors {
			errs = errs[:MaxImportStoredErrors]
		}
		errorsJSON, err := json.Marshal(errs)
		if err != nil {
			return fmt.Errorf("encode import errors: %w", err)
		}
		if _, err := t.ExecContext(ctx, `
			INSERT INTO import_batches(import_id, source_id, uploader_id, status,
			  added, updated, duplicate, rejected, errors_json, content_hash, created_at)
			VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
			importID, req.SourceID, req.UploaderID, "completed",
			counts.Added, counts.Updated, counts.Duplicate, req.Rejected,
			string(errorsJSON), req.ContentHash, fmtTime(req.Now)); err != nil {
			return err
		}
		if req.IdempotencyKey != "" {
			if _, err := t.ExecContext(ctx, `
				INSERT INTO import_idempotency(uploader_id, source_id, idempotency_key,
				  content_hash, import_id, created_at)
				VALUES(?,?,?,?,?,?)`,
				req.UploaderID, req.SourceID, req.IdempotencyKey,
				req.ContentHash, importID, fmtTime(req.Now)); err != nil {
				return err
			}
		}
		out = ApplyImportBatchResult{
			ImportID:  importID,
			Added:     counts.Added,
			Updated:   counts.Updated,
			Duplicate: counts.Duplicate,
			Rejected:  req.Rejected,
			Errors:    errs,
			CreatedAt: req.Now,
		}
		if err := cleanupImportRecordsTx(ctx, t, req.Now); err != nil {
			return err
		}
		return nil
	})
	if err != nil && isSQLiteBusy(err) {
		return ApplyImportBatchResult{}, domain.E("IMPORT_STORE_BUSY", 503,
			"import store is busy; retry the same batch")
	}
	return out, mapErr(err)
}

// isSQLiteBusy reports a transient lock timeout so callers can answer 503
// and retry with the same Idempotency-Key instead of surfacing a bare 500.
func isSQLiteBusy(err error) bool {
	var sqliteErr sqlite3.Error
	return errors.As(err, &sqliteErr) && sqliteErr.Code == sqlite3.ErrBusy
}

// GetImportBatch returns one stored batch result. When uploaderID is empty
// (admin identity) any uploader's batch for the source matches; a non-empty
// uploaderID scopes the read to that identity's own batches.
func (s *Store) GetImportBatch(ctx context.Context, uploaderID, sourceID, importID string) (ImportBatchRecord, error) {
	var out ImportBatchRecord
	// Read-only and outside the write transaction: result queries must not
	// contend with import commits or run schema DDL. A missing table means
	// no batch was ever recorded on this store.
	var tableCount int
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM sqlite_master
		WHERE type = 'table' AND name IN ('import_batches', 'import_idempotency')`).
		Scan(&tableCount); err != nil {
		return ImportBatchRecord{}, mapErr(err)
	}
	if tableCount < 2 {
		return ImportBatchRecord{}, domain.NotFound("import batch")
	}
	// Records past the published retention window are expired even before
	// the bounded cleanup pass reaches them.
	cutoff := time.Now().Add(-ImportRecordRetention)
	batch, err := importBatchByQuery(ctx, s.db, sourceID, importID, cutoff)
	if err != nil {
		return ImportBatchRecord{}, mapErr(err)
	}
	if uploaderID != "" && batch.UploaderID != uploaderID {
		return ImportBatchRecord{}, domain.NotFound("import batch")
	}
	out = batch
	return out, nil
}

// importFinalAuthorizationGuardTx rechecks the credentials and source
// snapshot that authorized parsing. The source version is never reused, so
// this also detects delete-and-recreate under the same source ID.
func importFinalAuthorizationGuardTx(ctx context.Context, t *sql.Tx, req ImportBatchApply) error {
	keyID := req.UploadKeyID
	if keyID != "" {
		if err := ensureUploadKeySchemaTx(ctx, t); err != nil {
			return err
		}
		var active int
		err := t.QueryRowContext(ctx, `
			SELECT EXISTS(
			  SELECT 1 FROM upload_keys k
			  JOIN upload_key_sources s ON s.key_id = k.key_id
			  WHERE k.key_id = ? AND k.enabled = 1 AND s.source_id = ?)`,
			keyID, req.SourceID).Scan(&active)
		if err != nil {
			return err
		}
		if active != 1 {
			return domain.E("UPLOAD_KEY_REVOKED", 401,
				"upload key is disabled, deleted, or no longer bound to the source")
		}
	}
	var enabled int
	var adapterType string
	var currentVersion int64
	err := t.QueryRowContext(ctx, `
		SELECT enabled, adapter_type, config_version FROM sources WHERE source_id = ?`,
		req.SourceID).Scan(&enabled, &adapterType, &currentVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.NotFound("source")
	}
	if err != nil {
		return err
	}
	if adapterType != "custom" || enabled != 1 {
		return domain.E("SOURCE_STATE_CHANGED", 409,
			"source %q is no longer an enabled upload channel; import rejected at commit",
			req.SourceID)
	}
	if req.SourceVersion > 0 && currentVersion != req.SourceVersion {
		return domain.E("SOURCE_STATE_CHANGED", 409,
			"source %q changed while the import was being parsed; reload and retry",
			req.SourceID)
	}
	return nil
}

func importBatchByQuery(ctx context.Context, q queryable, sourceID, importID string,
	cutoff time.Time) (ImportBatchRecord, error) {
	var batch ImportBatchRecord
	var errorsJSON, createdAt string
	err := q.QueryRowContext(ctx, `
		SELECT import_id, source_id, uploader_id, status, added, updated, duplicate,
		       rejected, errors_json, created_at
		FROM import_batches
		WHERE source_id = ? AND import_id = ? AND created_at > ?`,
		sourceID, importID, fmtTime(cutoff)).
		Scan(&batch.ImportID, &batch.SourceID, &batch.UploaderID, &batch.Status,
			&batch.Added, &batch.Updated, &batch.Duplicate, &batch.Rejected,
			&errorsJSON, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return ImportBatchRecord{}, domain.NotFound("import batch")
	}
	if err != nil {
		return ImportBatchRecord{}, err
	}
	if batch.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt); err != nil {
		return ImportBatchRecord{}, fmt.Errorf("decode import batch time: %w", err)
	}
	if len(errorsJSON) > 0 {
		if err := json.Unmarshal([]byte(errorsJSON), &batch.Errors); err != nil {
			return ImportBatchRecord{}, fmt.Errorf("decode import errors: %w", err)
		}
	}
	return batch, nil
}

type ImportBatchRecord struct {
	ImportID   string
	SourceID   string
	UploaderID string
	Status     string
	Added      int
	Updated    int
	Duplicate  int
	Rejected   int
	Errors     []ImportBatchError
	CreatedAt  time.Time
}

func importBatchByIDTx(ctx context.Context, t *sql.Tx, sourceID, importID string) (ImportBatchRecord, error) {
	var batch ImportBatchRecord
	var errorsJSON, createdAt string
	err := t.QueryRowContext(ctx, `
		SELECT import_id, source_id, uploader_id, status, added, updated, duplicate,
		       rejected, errors_json, created_at
		FROM import_batches
		WHERE source_id = ? AND import_id = ?`,
		sourceID, importID).
		Scan(&batch.ImportID, &batch.SourceID, &batch.UploaderID, &batch.Status,
			&batch.Added, &batch.Updated, &batch.Duplicate, &batch.Rejected,
			&errorsJSON, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return ImportBatchRecord{}, domain.NotFound("import batch")
	}
	if err != nil {
		return ImportBatchRecord{}, err
	}
	if batch.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt); err != nil {
		return ImportBatchRecord{}, fmt.Errorf("decode import batch time: %w", err)
	}
	if len(errorsJSON) > 0 {
		if err := json.Unmarshal([]byte(errorsJSON), &batch.Errors); err != nil {
			return ImportBatchRecord{}, fmt.Errorf("decode import errors: %w", err)
		}
	}
	return batch, nil
}

// cleanupImportRecordsTx drops expired batch results and idempotency records
// with a bounded per-table delete so one import cannot stall behind a huge
// retention sweep.
func cleanupImportRecordsTx(ctx context.Context, t *sql.Tx, now time.Time) error {
	cutoff := fmtTime(now.Add(-ImportRecordRetention))
	for _, table := range []string{"import_batches", "import_idempotency"} {
		if _, err := t.ExecContext(ctx, fmt.Sprintf(
			`DELETE FROM %s WHERE rowid IN (
			   SELECT rowid FROM %s WHERE created_at < ? LIMIT %d)`,
			table, table, maxImportCleanupRows), cutoff); err != nil {
			return err
		}
	}
	return nil
}

func ensureImportSchemaTx(ctx context.Context, t *sql.Tx) error {
	_, err := t.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS import_batches (
		  import_id TEXT PRIMARY KEY,
		  source_id TEXT NOT NULL,
		  uploader_id TEXT NOT NULL,
		  status TEXT NOT NULL,
		  added INTEGER NOT NULL,
		  updated INTEGER NOT NULL,
		  duplicate INTEGER NOT NULL,
		  rejected INTEGER NOT NULL,
		  errors_json TEXT NOT NULL DEFAULT '[]',
		  content_hash TEXT NOT NULL,
		  created_at TEXT NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_import_batches_source_time
		  ON import_batches(source_id, created_at);
		CREATE INDEX IF NOT EXISTS idx_import_batches_uploader
		  ON import_batches(uploader_id, created_at);
		CREATE TABLE IF NOT EXISTS import_idempotency (
		  uploader_id TEXT NOT NULL,
		  source_id TEXT NOT NULL,
		  idempotency_key TEXT NOT NULL,
		  content_hash TEXT NOT NULL,
		  import_id TEXT NOT NULL,
		  created_at TEXT NOT NULL,
		  PRIMARY KEY (uploader_id, source_id, idempotency_key)
		);`)
	return err
}
