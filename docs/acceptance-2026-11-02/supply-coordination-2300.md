# Supply Agent Coordination - 2026-11-02 23:00+

Shared contract notes for admin_glm_2300 and core_glm_2300. Supply owns the
files below; do not edit them. Nothing here requires a store.go change: the
import schema helper is ensured lazily inside supply's own transactions,
following the existing `ensureSupplySchemaTx` pattern.

## Durable import service (R29/R30)

File: `internal/application/imports.go`, storage: `internal/storage/sqlite/imports.go`.

```go
type ImportRequest struct {
    SourceID       string // validated custom/upload source
    UploaderID     string // "admin" for the admin file entry, or the upload key ID ("uk_...")
    ContentType    string // "text/plain" or "application/json"
    Body           []byte // raw request body
    IdempotencyKey string // OPTIONAL; trimmed, 1..128 chars, scoped to (uploader, source)
}

type ImportBatchError struct {
    Index int    `json:"index"`          // 1-based item index (line number for text)
    Input string `json:"input,omitempty"` // sanitized: no userinfo/password
    Error string `json:"error"`
}

type ImportBatchResult struct {
    ImportID  string             `json:"import_id"`
    SourceID  string             `json:"source_id"`
    Status    string             `json:"status"` // always "completed"; replay is conveyed by HTTP 200 vs 201
    Added     int                `json:"added"`
    Updated   int                `json:"updated"`
    Duplicate int                `json:"duplicate"`
    Rejected  int                `json:"rejected"` // full count
    Errors    []ImportBatchError `json:"errors,omitempty"` // bounded (max 100)
    CreatedAt string             `json:"created_at"`       // UTC RFC 3339
}

func (s *IngestService) ImportBatch(ctx context.Context, req ImportRequest) (ImportBatchResult, *domain.Error)
func (s *IngestService) GetImportBatch(ctx context.Context, uploaderID, sourceID, importID string) (ImportBatchResult, *domain.Error)
```

For admin replay-vs-first semantics: the storage layer reports replay
internally, so the service exposes it as an internal field on the result:

```go
// Applied is false when an Idempotency-Key replay returned the stored result
// without re-applying. First apply => true, replay => false (admin: 201 vs 200).
Applied bool // field on ImportBatchResult: json:"-" (internal), wire uses HTTP status
```

Suggested admin wiring:

- `POST /api/v1/admin/sources/{id}/imports` and the new upload routes both call
  `ImportBatch` with `UploaderID: "admin"` (admin) or the key ID (upload key).
- Authorization must fail closed before `ImportBatch` is called.
- `GET /api/v1/sources/{source_id}/imports/{import_id}` calls `GetImportBatch`;
  for upload-key identities pass the key ID in `uploaderID` so the result is
  scoped to that identity's own batches.

## Published limits (also enforced server-side)

- Request body: 8 MiB; items per batch: 10,000; retained errors: 100 (rejected count is always complete).
- `Idempotency-Key`: optional, 1..128 chars after trimming, scoped to (uploader identity, source).
  Same key + same content (sha256 of the exact body bytes) returns the same `import_id` and result; different content is a 409 `IDEMPOTENCY_KEY_CONFLICT`.
- Batch/idempotency retention: 7 days, bounded cleanup (500 rows max per import).
- Whole-body structural errors reject the batch (no `import_id`); per-item
  errors are reported while legal items still append. Imports never create
  PASS evidence or pool-verified observations; re-imports preserve health,
  incarnation and CD.

## Static refresh single-flight fix

`RefreshStaticSource` now serves a fresh cached batch when a refresh lands
inside the vendor rate window (deterministic repro in
`internal/application/staticcache_refresh_test.go`). This fixes the reported
`TestStaticRefreshSingleFlight` SOURCE_RATE_LIMITED flake for straggler
followers without weakening the toggle rate-limit guard.
