package application

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"strings"
	"time"

	"proxypool/internal/domain"
	"proxypool/internal/integrations/custom"
	"proxypool/internal/storage/sqlite"
)

// Published durable-import limits (R29/R30). The API layer additionally
// caps the request body with http.MaxBytesReader; these bounds are enforced
// again here so every entry point shares one contract.
const (
	MaxImportBodyBytes   = 8 << 20
	MaxImportItems       = 10000
	MaxImportErrors      = sqlite.MaxImportStoredErrors
	MaxIdempotencyKeyLen = 128
	MaxImporterIDLen     = 128
	// maxImportCredentialLen bounds the JSON username/password fields; raw
	// credential values never appear in error text regardless of length.
	maxImportCredentialLen = custom.MaxCredentialBytes
)

type ImportRequest struct {
	SourceID       string
	UploaderID     string
	UploadKeyID    string
	ContentType    string
	Body           []byte
	IdempotencyKey string
}

// ImportBatchResult is the durable batch report. Applied is internal: the
// wire conveys first-apply vs replay through the HTTP status (201 vs 200).
type ImportBatchResult struct {
	ImportID  string                    `json:"import_id"`
	SourceID  string                    `json:"source_id"`
	Status    string                    `json:"status"`
	Added     int                       `json:"added"`
	Updated   int                       `json:"updated"`
	Duplicate int                       `json:"duplicate"`
	Rejected  int                       `json:"rejected"`
	Errors    []sqlite.ImportBatchError `json:"errors,omitempty"`
	CreatedAt string                    `json:"created_at"`
	Applied   bool                      `json:"-"`
}

// ImportBatch is the one durable append/merge importer behind the admin file
// entry and the external push API. It accepts text/plain URI lines and
// application/json connection batches, keeps health/incarnation/CD on
// duplicates, never creates PASS evidence or pool-verified observations, and
// commits inventory and the batch result in one transaction. An optional
// Idempotency-Key is scoped to (UploaderID, SourceID): same content replays
// the stored result, different content is a 409 conflict.
func (s *IngestService) ImportBatch(ctx context.Context, req ImportRequest) (ImportBatchResult, *domain.Error) {
	if req.SourceID == "" {
		return ImportBatchResult{}, domain.InvalidRequest("source_id is required")
	}
	if req.UploaderID == "" {
		return ImportBatchResult{}, domain.InvalidRequest("uploader identity is required")
	}
	if req.UploadKeyID != "" && req.UploadKeyID != req.UploaderID {
		return ImportBatchResult{}, domain.InvalidRequest(
			"upload key identity does not match the durable uploader identity")
	}
	if len(req.UploaderID) > MaxImporterIDLen {
		return ImportBatchResult{}, domain.InvalidRequest("uploader identity is too long")
	}
	if len(req.Body) > MaxImportBodyBytes {
		return ImportBatchResult{}, domain.E("IMPORT_BODY_TOO_LARGE", 413,
			"import body exceeds %d bytes", MaxImportBodyBytes)
	}
	idemKey := strings.TrimSpace(req.IdempotencyKey)
	if len(idemKey) > MaxIdempotencyKeyLen {
		return ImportBatchResult{}, domain.InvalidRequest(
			"Idempotency-Key must be at most %d characters", MaxIdempotencyKeyLen)
	}
	src, err := s.store.GetSource(ctx, req.SourceID)
	if err != nil {
		return ImportBatchResult{}, domain.AsError(err)
	}
	if src.AdapterType != AdapterCustom {
		return ImportBatchResult{}, domain.InvalidRequest(
			"source %q is a %q channel; only custom upload sources accept imports",
			req.SourceID, src.AdapterType)
	}
	if !src.Enabled {
		return ImportBatchResult{}, domain.InvalidRequest("source %q is disabled", req.SourceID)
	}
	var cfg SourceConfig
	if err := json.Unmarshal([]byte(src.ConfigJSON), &cfg); err != nil {
		return ImportBatchResult{}, domain.AsError(fmt.Errorf("decode source config: %w", err))
	}
	if cfg.MaxPoolLeases <= 0 {
		cfg.MaxPoolLeases = 1
	}
	candidates, itemErrs, derr := s.parseImportBody(src, cfg, req.ContentType, req.Body)
	if derr != nil {
		return ImportBatchResult{}, derr
	}
	now := s.Now()
	sum := sha256.Sum256(req.Body)
	applied, serr := s.store.ApplyImportBatch(ctx, sqlite.ImportBatchApply{
		SourceID:       req.SourceID,
		UploaderID:     req.UploaderID,
		UploadKeyID:    req.UploadKeyID,
		SourceVersion:  src.ConfigVersion,
		IdempotencyKey: idemKey,
		ContentHash:    hex.EncodeToString(sum[:]),
		Candidates:     candidates,
		Rejected:       len(itemErrs),
		Errors:         itemErrs,
		Now:            now,
	})
	if serr != nil {
		return ImportBatchResult{}, domain.AsError(serr)
	}
	return ImportBatchResult{
		ImportID:  applied.ImportID,
		SourceID:  req.SourceID,
		Status:    "completed",
		Added:     applied.Added,
		Updated:   applied.Updated,
		Duplicate: applied.Duplicate,
		Rejected:  applied.Rejected,
		Errors:    applied.Errors,
		CreatedAt: applied.CreatedAt.UTC().Format(time.RFC3339),
		Applied:   !applied.Replayed,
	}, nil
}

// GetImportBatch returns one stored batch result. For upload-key identities
// the caller must pass the key ID in uploaderID so the read is scoped to that
// identity's own batches; the admin identity passes "".
func (s *IngestService) GetImportBatch(ctx context.Context, uploaderID, sourceID, importID string) (ImportBatchResult, *domain.Error) {
	if sourceID == "" || importID == "" {
		return ImportBatchResult{}, domain.InvalidRequest("source_id and import_id are required")
	}
	rec, err := s.store.GetImportBatch(ctx, uploaderID, sourceID, importID)
	if err != nil {
		return ImportBatchResult{}, domain.AsError(err)
	}
	return ImportBatchResult{
		ImportID:  rec.ImportID,
		SourceID:  rec.SourceID,
		Status:    rec.Status,
		Added:     rec.Added,
		Updated:   rec.Updated,
		Duplicate: rec.Duplicate,
		Rejected:  rec.Rejected,
		Errors:    rec.Errors,
		CreatedAt: rec.CreatedAt.UTC().Format(time.RFC3339),
	}, nil
}

// parseImportBody turns one request body into normalized candidates plus
// per-item rejections. Structural problems return a domain error and reject
// the whole batch before any transaction opens; item-level problems are
// reported while the legal items still append.
func (s *IngestService) parseImportBody(src domain.Source, cfg SourceConfig, contentType string, body []byte) (
	[]domain.NormalizedCandidate, []sqlite.ImportBatchError, *domain.Error) {
	mediaType := ""
	if strings.TrimSpace(contentType) != "" {
		parsed, _, err := mime.ParseMediaType(contentType)
		if err != nil {
			return nil, nil, domain.InvalidRequest(
				"invalid Content-Type; use text/plain or application/json")
		}
		mediaType = strings.ToLower(parsed)
	}
	now := s.Now()
	switch mediaType {
	case "", "text/plain":
		return s.parseImportText(src, cfg, now, body)
	case "application/json":
		return s.parseImportJSON(src, cfg, now, body)
	default:
		return nil, nil, domain.InvalidRequest(
			"unsupported import content type; use text/plain or application/json")
	}
}

func (s *IngestService) parseImportText(src domain.Source, cfg SourceConfig, now time.Time, body []byte) (
	[]domain.NormalizedCandidate, []sqlite.ImportBatchError, *domain.Error) {
	if len(strings.TrimSpace(string(body))) == 0 {
		return nil, nil, domain.InvalidRequest("import body contains no proxy lines")
	}
	// The per-batch item cap applies to the text format too: comment and
	// blank lines do not count as items.
	itemCount := 0
	for _, raw := range strings.Split(string(body), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		itemCount++
	}
	if itemCount > MaxImportItems {
		return nil, nil, domain.E("IMPORT_TOO_MANY_ITEMS", 413,
			"text import exceeds %d items", MaxImportItems)
	}
	parsed, rejected := custom.ParseLines(custom.LinesOptions{
		DefaultTransport: cfg.DefaultTransport,
		AllowPrivate:     s.allowPrivate,
	}, string(body))
	candidates := make([]domain.NormalizedCandidate, 0, len(parsed))
	for _, c := range parsed {
		candidates = append(candidates, s.buildCandidate(src, cfg, c.Conn, now))
	}
	errs := make([]sqlite.ImportBatchError, 0, len(rejected))
	for _, r := range rejected {
		errs = append(errs, sqlite.ImportBatchError{
			Index: r.Line, Input: sanitizeImportErrorDetail(r.Input), Error: r.Error})
	}
	return candidates, errs, nil
}

type jsonImportEnvelope struct {
	Items []json.RawMessage `json:"items"`
}

type jsonImportItem struct {
	Connection        jsonImportConnection `json:"connection"`
	ProviderExpiresAt *string              `json:"provider_expires_at"`
}

type jsonImportConnection struct {
	Transport string `json:"transport"`
	Host      string `json:"host"`
	Port      *int   `json:"port"`
	Username  string `json:"username"`
	Password  string `json:"password"`
}

func (s *IngestService) parseImportJSON(src domain.Source, cfg SourceConfig, now time.Time, body []byte) (
	[]domain.NormalizedCandidate, []sqlite.ImportBatchError, *domain.Error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	var envelope jsonImportEnvelope
	if err := dec.Decode(&envelope); err != nil {
		return nil, nil, domain.InvalidRequest("invalid JSON import body")
	}
	// The body must be exactly one JSON value: a second value or trailing
	// garbage rejects the whole batch instead of being silently ignored.
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		return nil, nil, domain.InvalidRequest(
			"invalid JSON import body: unexpected trailing data after the JSON object")
	}
	if envelope.Items == nil {
		return nil, nil, domain.InvalidRequest(`JSON import requires an "items" array`)
	}
	if len(envelope.Items) > MaxImportItems {
		return nil, nil, domain.E("IMPORT_TOO_MANY_ITEMS", 413,
			"JSON import exceeds %d items", MaxImportItems)
	}
	candidates := make([]domain.NormalizedCandidate, 0, len(envelope.Items))
	errs := make([]sqlite.ImportBatchError, 0)
	for i, raw := range envelope.Items {
		conn, expiresAt, itemErr := parseImportJSONItem(s.allowPrivate, raw)
		if itemErr != nil {
			errs = append(errs, sqlite.ImportBatchError{
				Index: i + 1, Error: sanitizeImportErrorDetail(itemErr.Error())})
			continue
		}
		cand := s.buildCandidate(src, cfg, conn, now)
		cand.ProviderExpiresAt = expiresAt
		candidates = append(candidates, cand)
	}
	return candidates, errs, nil
}

func parseImportJSONItem(allowPrivate bool, raw json.RawMessage) (domain.Connection, *time.Time, error) {
	var item jsonImportItem
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&item); err != nil {
		return domain.Connection{}, nil, fmt.Errorf("invalid item structure")
	}
	switch domain.Transport(item.Connection.Transport) {
	case domain.TransportHTTP, domain.TransportSOCKS5:
	default:
		return domain.Connection{}, nil, fmt.Errorf(
			"unsupported transport; use http or socks5")
	}
	host, err := custom.NormalizeHost(item.Connection.Host, allowPrivate)
	if err != nil {
		if strings.Contains(err.Error(), "address policy") {
			return domain.Connection{}, nil, fmt.Errorf("host rejected by address policy")
		}
		return domain.Connection{}, nil, err
	}
	if len(item.Connection.Username) > maxImportCredentialLen {
		return domain.Connection{}, nil, fmt.Errorf(
			"username exceeds %d bytes", maxImportCredentialLen)
	}
	if len(item.Connection.Password) > maxImportCredentialLen {
		return domain.Connection{}, nil, fmt.Errorf(
			"password exceeds %d bytes", maxImportCredentialLen)
	}
	if item.Connection.Port == nil {
		return domain.Connection{}, nil, fmt.Errorf("missing port")
	}
	port := *item.Connection.Port
	if port < 1 || port > 65535 {
		return domain.Connection{}, nil, fmt.Errorf("invalid port %d", port)
	}
	var expiresAt *time.Time
	if item.ProviderExpiresAt != nil && *item.ProviderExpiresAt != "" {
		parsed, err := time.Parse(time.RFC3339, *item.ProviderExpiresAt)
		if err != nil {
			return domain.Connection{}, nil, fmt.Errorf(
				"invalid provider_expires_at: must be UTC RFC 3339")
		}
		expiresAt = &parsed
	}
	conn := domain.Connection{
		Transport: domain.Transport(item.Connection.Transport),
		Host:      host,
		Port:      port,
		Username:  item.Connection.Username,
		Password:  item.Connection.Password,
	}
	return conn, expiresAt, nil
}

// sanitizeImportErrorDetail strips control characters and caps the length so
// malformed input can never smuggle raw bytes or unbounded payloads into
// stored and returned error text.
func sanitizeImportErrorDetail(detail string) string {
	detail = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, detail)
	const maxDetailRunes = 200
	if runes := []rune(detail); len(runes) > maxDetailRunes {
		return string(runes[:maxDetailRunes]) + "..."
	}
	return detail
}
