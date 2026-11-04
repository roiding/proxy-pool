package api

import (
	"io"
	"net/http"
	"slices"

	"proxypool/internal/application"
	"proxypool/internal/domain"
)

// handleCreateUploadKey mints an upload credential. The plaintext uk_ token
// is returned exactly once here; storage keeps only the HMAC hash.
func (s *Server) handleCreateUploadKey(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name      string   `json:"name"`
		SourceIDs []string `json:"source_ids"`
	}
	if derr := decodeStrictJSON(w, r, 8192, &body, false); derr != nil {
		writeError(w, derr)
		return
	}
	key, token, err := s.uploadKeys.Create(r.Context(), body.Name, body.SourceIDs)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"key": key, "token": token})
}

func (s *Server) handleListUploadKeys(w http.ResponseWriter, r *http.Request) {
	keys, err := s.uploadKeys.List(r.Context())
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if keys == nil {
		keys = []domain.UploadKey{}
	}
	// List output is masked by construction: UploadKey carries the display
	// prefix only, never the hash or any recoverable credential material.
	writeJSON(w, http.StatusOK, keys)
}

// handleUpdateUploadKey renames or disables one upload key. Updates touch
// only the targeted key; sibling keys and their source scopes are unaffected.
func (s *Server) handleUpdateUploadKey(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name    *string `json:"name"`
		Enabled *bool   `json:"enabled"`
	}
	if derr := decodeStrictJSON(w, r, 8192, &body, false); derr != nil {
		writeError(w, derr)
		return
	}
	if body.Name == nil && body.Enabled == nil {
		writeError(w, domain.InvalidRequest("nothing to update; provide name or enabled"))
		return
	}
	if err := s.uploadKeys.Update(r.Context(), r.PathValue("id"), body.Name, body.Enabled); err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "updated"})
}

func (s *Server) handleDeleteUploadKey(w http.ResponseWriter, r *http.Request) {
	if err := s.uploadKeys.Delete(r.Context(), r.PathValue("id")); err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

// uploadKeyAuthorized resolves import permission before any batch work
// starts. Admins may import everywhere; an upload credential must carry the
// exact source in its allowlist. Everything else fails closed.
func (s *Server) uploadKeyAuthorized(ident authIdentity, sourceID string) bool {
	if ident.Admin {
		return true
	}
	if ident.UploadKeyID == "" || s.uploadKeys == nil {
		return false
	}
	return slices.Contains(ident.UploadSources, sourceID)
}

// handleImportV1 is the external push entry: POST
// /api/v1/sources/{source_id}/imports with text/plain lines or an
// application/json batch. An optional Idempotency-Key replays the stored
// result instead of reapplying the same content.
func (s *Server) handleImportV1(w http.ResponseWriter, r *http.Request) {
	ident := identityFrom(r.Context())
	sourceID := r.PathValue("source_id")
	if !s.uploadKeyAuthorized(ident, sourceID) {
		writeError(w, domain.E("SOURCE_NOT_ALLOWED", 403,
			"this credential cannot import into source %q", sourceID))
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, application.MaxImportBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, domain.InvalidRequest("read import body: %v", err))
		return
	}
	if derr := s.recheckUploadKeyActive(r, ident, sourceID); derr != nil {
		writeError(w, derr)
		return
	}
	result, derr := s.ingest.ImportBatch(r.Context(), application.ImportRequest{
		SourceID:       sourceID,
		UploaderID:     s.importUploaderID(ident),
		UploadKeyID:    ident.UploadKeyID,
		ContentType:    r.Header.Get("Content-Type"),
		Body:           body,
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
	})
	if derr != nil {
		writeError(w, derr)
		return
	}
	status := http.StatusOK
	if result.Applied {
		status = http.StatusCreated
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, status, result)
}

// recheckUploadKeyActive revalidates the credential after the body has been
// consumed and immediately before the batch is applied. A key disabled or
// deleted while its request was in flight must not apply, even though the
// request-start authentication snapshot was valid.
func (s *Server) recheckUploadKeyActive(r *http.Request, ident authIdentity, sourceID string) *domain.Error {
	if ident.Admin || ident.UploadKeyID == "" {
		return nil
	}
	active, err := s.store.UploadKeyActiveForSource(r.Context(), ident.UploadKeyID, sourceID)
	if err != nil {
		return domain.StoreUnavailable(err)
	}
	if !active {
		return domain.E("UPLOAD_KEY_REVOKED", 401,
			"this upload key is disabled, deleted, or no longer bound to source %q", sourceID)
	}
	return nil
}

// importUploaderID scopes idempotency and ownership to the pushing
// credential: an upload key is its own identity, while an admin import
// records the admin consumer.
func (s *Server) importUploaderID(ident authIdentity) string {
	if ident.UploadKeyID != "" {
		return ident.UploadKeyID
	}
	return ident.ownershipID()
}

// handleGetImportV1 returns one batch result. Upload credentials can read
// only their own batches on their own sources; the store enforces the owner
// scope, admins may read every batch.
func (s *Server) handleGetImportV1(w http.ResponseWriter, r *http.Request) {
	ident := identityFrom(r.Context())
	sourceID := r.PathValue("source_id")
	if !s.uploadKeyAuthorized(ident, sourceID) {
		writeError(w, domain.E("SOURCE_NOT_ALLOWED", 403,
			"this credential cannot read imports of source %q", sourceID))
		return
	}
	uploaderID := ""
	if !ident.Admin {
		uploaderID = ident.UploadKeyID
	}
	if derr := s.recheckUploadKeyActive(r, ident, sourceID); derr != nil {
		writeError(w, derr)
		return
	}
	result, derr := s.ingest.GetImportBatch(r.Context(), uploaderID, sourceID, r.PathValue("import_id"))
	if derr != nil {
		writeError(w, derr)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, result)
}
