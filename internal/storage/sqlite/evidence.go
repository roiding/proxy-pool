package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"proxypool/internal/domain"
)

var ErrIncompleteEvidence = errors.New("evidence has no completion timestamp")

// EvidenceKey binds a cached result to everything that must match for reuse:
// the exact validation scope (via scope hash), the upstream source and its
// config version, and the object incarnation. A deleted-then-reimported
// resource gets a new incarnation, so an old PASS can never be inherited.
type EvidenceKey struct {
	ResourceID          string
	Fingerprint         string
	SourceID            string
	SourceConfigVersion int64
	Incarnation         string
}

// PutEvidence stores the newest result for one key. Freshness follows
// ev.CompletedAt — never the caller's clock — and the upsert guard rejects
// out-of-order writers, so a stale PASS cannot overwrite a newer FAIL (the
// newest negative result must stay visible until a genuinely newer result
// arrives).
func (s *Store) PutEvidence(ctx context.Context, k EvidenceKey,
	scopeHash string, ev domain.Evidence) error {
	return s.putEvidence(ctx, k, scopeHash, ev, false)
}

// PutEvidenceProduction is the pool/acquire path: it requires a live
// resource incarnation and current source config version even when a caller
// passes a ResourceID. The metadata form is retained for explicitly
// resource-less fixtures and non-delivery evidence.
func (s *Store) PutEvidenceProduction(ctx context.Context, k EvidenceKey,
	scopeHash string, ev domain.Evidence) error {
	return s.putEvidence(ctx, k, scopeHash, ev, true)
}

func (s *Store) putEvidence(ctx context.Context, k EvidenceKey,
	scopeHash string, ev domain.Evidence, production bool) error {
	if scopeHash == "" || k.Fingerprint == "" || k.SourceID == "" ||
		k.SourceConfigVersion <= 0 || k.Incarnation == "" {
		return fmt.Errorf("incomplete evidence identity: %w", ErrNotEligible)
	}
	if ev.Outcome != "PASS" && ev.Outcome != "FAIL" {
		return fmt.Errorf("invalid evidence outcome %q", ev.Outcome)
	}
	if ev.CompletedAt.IsZero() {
		return ErrIncompleteEvidence
	}
	if !ev.StartedAt.IsZero() && ev.CompletedAt.Before(ev.StartedAt) {
		return ErrIncompleteEvidence
	}
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	// Every write validates the current source version. Production writes
	// additionally require a live resource incarnation when ResourceID is
	// present; resource-less API/static callers can store request-local proof
	// under the fingerprint/incarnation contract without inventing inventory.
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO validation_evidence(connection_fingerprint, scope_hash, source_id,
		  source_config_version, incarnation, evidence_json, outcome, completed_at)
		SELECT ?,?,?,?,?,?,?,?
		WHERE (
		  ? = '' OR (
		    ? <> '' AND EXISTS (
		      SELECT 1 FROM proxy_resources r
		      WHERE r.resource_id = ? AND r.incarnation = ?
		    )
		  )
		)
		AND (
		  ? = 0 OR EXISTS (
		    SELECT 1 FROM sources s
		    WHERE s.source_id = ? AND s.enabled = 1
		      AND s.config_version = ?
		  )
		)
		ON CONFLICT(connection_fingerprint, scope_hash) DO UPDATE SET
		  source_id = excluded.source_id,
		  source_config_version = excluded.source_config_version,
		  incarnation = excluded.incarnation,
		  evidence_json = excluded.evidence_json,
		  outcome = excluded.outcome,
		  completed_at = excluded.completed_at
		WHERE (
		  excluded.completed_at > validation_evidence.completed_at
		  OR (
		    excluded.completed_at = validation_evidence.completed_at
		    AND excluded.outcome = 'FAIL'
		    AND validation_evidence.outcome = 'PASS'
		  )
		)
		  AND (
		    ? = 0 OR ? = '' OR EXISTS (
		      SELECT 1 FROM proxy_resources r
		      WHERE r.resource_id = ? AND r.incarnation = excluded.incarnation
		    )
		  )
		  AND (
		    ? = 0 OR EXISTS (
		      SELECT 1 FROM sources s
		      WHERE s.source_id = excluded.source_id AND s.enabled = 1
		        AND s.config_version = excluded.source_config_version
		    )
		  )
		  AND (
		    ? = 0 OR excluded.incarnation = validation_evidence.incarnation
		  )
		  `,
		k.Fingerprint, scopeHash, k.SourceID, k.SourceConfigVersion, k.Incarnation,
		string(data), ev.Outcome, fmtTime(ev.CompletedAt),
		k.ResourceID, boolInt(production), k.ResourceID, k.Incarnation,
		boolInt(production), k.SourceID, k.SourceConfigVersion,
		boolInt(production), k.ResourceID, k.ResourceID, boolInt(production),
		boolInt(production))
	if err != nil {
		return mapErr(err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return mapErr(err)
	} else if n == 0 {
		// The ordering guard rejected an out-of-order result; callers may
		// ignore exactly this typed error.
		return ErrStale
	}
	return nil
}

// RecentEvidence returns still-fresh PASS evidence for the keys that match
// their recorded source version and incarnation inside the exact scope.
func (s *Store) RecentEvidence(ctx context.Context, scopeHash string,
	keys []EvidenceKey, notBefore time.Time) (map[string]domain.Evidence, error) {
	out := map[string]domain.Evidence{}
	if scopeHash == "" || len(keys) == 0 {
		return out, nil
	}
	for _, k := range keys {
		if k.Fingerprint == "" {
			continue
		}
		var data string
		err := s.db.QueryRowContext(ctx, `
			SELECT e.evidence_json FROM validation_evidence e
			WHERE e.connection_fingerprint = ? AND e.scope_hash = ?
			  AND e.source_id = ? AND e.source_config_version = ? AND e.incarnation = ?
			  AND e.outcome = 'PASS' AND e.completed_at >= ?
			  AND (
			    ? = '' OR EXISTS (
			      SELECT 1 FROM proxy_resources r
			      WHERE r.resource_id = ? AND r.incarnation = e.incarnation
			    )
			  )`,
			k.Fingerprint, scopeHash, k.SourceID, k.SourceConfigVersion,
			k.Incarnation, fmtTime(notBefore), k.ResourceID, k.ResourceID).Scan(&data)
		if err == nil {
			var ev domain.Evidence
			if jerr := json.Unmarshal([]byte(data), &ev); jerr != nil {
				return nil, jerr
			}
			if ev.Outcome != "PASS" || ev.CompletedAt.IsZero() || ev.CompletedAt.After(time.Now()) {
				continue
			}
			ev.CacheHit = true
			out[k.Fingerprint] = ev
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, mapErr(err)
		}
	}
	return out, nil
}

// InvalidateEvidence drops cached evidence for fingerprints, e.g. when the
// newest observation is negative or the connection identity changed.
func (s *Store) InvalidateEvidence(ctx context.Context, fingerprints []string) error {
	if len(fingerprints) == 0 {
		return nil
	}
	args := toAny(fingerprints)
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM validation_evidence WHERE connection_fingerprint IN (`+
			placeholders(len(args))+`)`, args...)
	return mapErr(err)
}
