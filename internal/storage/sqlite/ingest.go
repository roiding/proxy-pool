package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"proxypool/internal/domain"
)

type ImportCounts struct {
	Added     int `json:"added"`
	Updated   int `json:"updated"`
	Skipped   int `json:"skipped"`
	Duplicate int `json:"duplicate,omitempty"`
}

// ImportCandidates appends candidates to a self-uploaded source's pool.
// Uploads are additive: every parsed line becomes a stable offer keyed by the
// proxy's content fingerprint, and dead supply is cleaned up by validation,
// not by list replacement. Identical re-uploads are idempotent.
//
// There is deliberately no tombstone gate: a connection whose supply was
// deleted as definitively dead may legitimately reappear in a future upload
// and is then stored again as an unvalidated candidate. The fresh row gets a
// new incarnation, so stale evidence and stale health writes from before the
// delete cannot reattach to it.
func (s *Store) ImportCandidates(ctx context.Context, candidates []domain.NormalizedCandidate, now time.Time) (ImportCounts, error) {
	return s.importCandidates(ctx, candidates, now, false)
}

// ImportBatchCandidates is the durable-import entry: unchanged re-imported
// offers count as duplicates instead of updates, so a batch report can
// distinguish "no change" from "metadata merged".
func (s *Store) ImportBatchCandidates(ctx context.Context, candidates []domain.NormalizedCandidate, now time.Time) (ImportCounts, error) {
	return s.importCandidates(ctx, candidates, now, true)
}

func (s *Store) importCandidates(ctx context.Context, candidates []domain.NormalizedCandidate,
	now time.Time, splitDuplicates bool) (ImportCounts, error) {
	var counts ImportCounts
	err := s.tx(ctx, func(t *sql.Tx) error {
		counts = ImportCounts{}
		return importCandidatesTx(ctx, t, candidates, now, splitDuplicates, &counts)
	})
	return counts, err
}

// importBatchCandidatesTx runs the duplicate-splitting inventory apply inside
// an existing transaction, so the durable import can commit its inventory
// changes and its result record together.
func importBatchCandidatesTx(ctx context.Context, t *sql.Tx,
	candidates []domain.NormalizedCandidate, now time.Time) (ImportCounts, error) {
	var counts ImportCounts
	if err := importCandidatesTx(ctx, t, candidates, now, true, &counts); err != nil {
		return ImportCounts{}, err
	}
	return counts, nil
}

func importCandidatesTx(ctx context.Context, t *sql.Tx, candidates []domain.NormalizedCandidate,
	now time.Time, splitDuplicates bool, counts *ImportCounts) error {
	seen := map[string]bool{}
	for _, c := range candidates {
		if c.ResourceID == "" || c.OfferID == "" {
			counts.Skipped++
			continue
		}
		batchKey := c.SourceID + "|" + c.RecordID + "|" + c.Generation
		if seen[batchKey] {
			counts.Duplicate++
			continue
		}
		seen[batchKey] = true
		var exists int
		if err := t.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM source_offers
			WHERE source_id = ? AND source_record_id = ? AND source_generation = ?`,
			c.SourceID, c.RecordID, c.Generation).Scan(&exists); err != nil {
			return err
		}
		if exists > 0 && splitDuplicates && offerMetadataEqualTx(ctx, t, c) {
			counts.Duplicate++
			continue
		}
		if c.CredentialVersion != "" {
			if _, err := t.ExecContext(ctx, `
				INSERT INTO proxy_credentials(credential_version, username, password, created_at)
				VALUES(?,?,?,?)
				ON CONFLICT(credential_version) DO NOTHING`,
				c.CredentialVersion, c.Conn.Username, c.Conn.Password, fmtTime(now)); err != nil {
				return err
			}
		}
		if _, err := t.ExecContext(ctx, `
			INSERT INTO proxy_resources(resource_id, tenant, transport, host, port, credential_version,
			  session_policy, resource_kind, disabled, incarnation, created_at, updated_at)
			VALUES(?,?,?,?,?,?,?,?,0,?,?,?)
			ON CONFLICT(resource_id) DO UPDATE SET updated_at = excluded.updated_at`,
			c.ResourceID, "default", string(c.Conn.Transport), c.Conn.Host, c.Conn.Port,
			c.CredentialVersion, string(c.Session), string(c.Kind), domain.NewID("res_"),
			fmtTime(now), fmtTime(now)); err != nil {
			return err
		}
		// Offers of the same resource left under older record-ID
		// schemes would otherwise coexist with the stable offer; keep
		// exactly one enabled offer per (source, resource).
		if _, err := t.ExecContext(ctx, `
			UPDATE source_offers SET state = 'REVOKED', updated_at = ?
			WHERE source_id = ? AND resource_id = ? AND state = 'ENABLED'
			  AND source_record_id != ?`,
			fmtTime(now), c.SourceID, c.ResourceID, c.RecordID); err != nil {
			return err
		}
		claimsJSON, err := json.Marshal(c.Claims)
		if err != nil {
			return fmt.Errorf("encode claims: %w", err)
		}
		if _, err := t.ExecContext(ctx, `
			INSERT INTO source_offers(offer_id, source_id, resource_id, source_record_id, source_generation,
			  provider_expires_at, local_retention_deadline, expiry_basis, claims_json, max_pool_leases,
			  capacity_group, state, created_at, updated_at)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(source_id, source_record_id, source_generation) DO UPDATE SET
			  provider_expires_at = excluded.provider_expires_at,
			  local_retention_deadline = excluded.local_retention_deadline,
			  expiry_basis = excluded.expiry_basis,
			  claims_json = excluded.claims_json,
			  max_pool_leases = excluded.max_pool_leases,
			  updated_at = excluded.updated_at`,
			c.OfferID, c.SourceID, c.ResourceID, c.RecordID, c.Generation,
			nullTime(c.ProviderExpiresAt), nullTime(c.LocalRetentionDeadline), c.ExpiryBasis,
			string(claimsJSON), c.MaxPoolLeases, c.CapacityGroup, domain.OfferEnabled,
			fmtTime(now), fmtTime(now)); err != nil {
			return err
		}
		if exists > 0 {
			counts.Updated++
		} else {
			counts.Added++
		}
	}
	return nil
}

// offerMetadataEqualTx reports whether the stored offer already carries the
// candidate's full offer metadata. Only a real metadata change counts as an
// update; an identical re-import is a duplicate and must not touch health,
// incarnation, CD or the offer row at all.
func offerMetadataEqualTx(ctx context.Context, t *sql.Tx, c domain.NormalizedCandidate) bool {
	var providerExp, retention any
	var expiryBasis, claimsJSON string
	var maxPoolLeases int
	err := t.QueryRowContext(ctx, `
		SELECT provider_expires_at, local_retention_deadline, expiry_basis,
		       claims_json, max_pool_leases
		FROM source_offers
		WHERE source_id = ? AND source_record_id = ? AND source_generation = ?`,
		c.SourceID, c.RecordID, c.Generation).
		Scan(&providerExp, &retention, &expiryBasis, &claimsJSON, &maxPoolLeases)
	if err != nil {
		return false
	}
	if expiryBasis != c.ExpiryBasis || maxPoolLeases != c.MaxPoolLeases {
		return false
	}
	if !nullTimeEqual(providerExp, nullTime(c.ProviderExpiresAt)) ||
		!nullTimeEqual(retention, nullTime(c.LocalRetentionDeadline)) {
		return false
	}
	claims, err := json.Marshal(c.Claims)
	if err != nil {
		return false
	}
	return claimsJSON == string(claims)
}

func nullTimeEqual(stored, candidate any) bool {
	if stored == nil || candidate == nil {
		return stored == nil && candidate == nil
	}
	storedStr, ok1 := stored.(string)
	candStr, ok2 := candidate.(string)
	if !ok1 || !ok2 {
		return false
	}
	storedTime, err1 := time.Parse(time.RFC3339Nano, storedStr)
	candTime, err2 := time.Parse(time.RFC3339Nano, candStr)
	if err1 != nil || err2 != nil {
		return storedStr == candStr
	}
	return storedTime.Equal(candTime)
}

// CreateSource inserts a new channel definition. Public create never
// overwrites: an existing ID is a conflict, not a silent update. The version
// comes from a durable per-ID watermark so delete + recreate cannot reuse a
// version held by a prepared candidate or a session snapshot.
func (s *Store) CreateSource(ctx context.Context, src domain.Source, now time.Time) error {
	return s.tx(ctx, func(t *sql.Tx) error {
		if err := ensureSupplySchemaTx(ctx, t); err != nil {
			return err
		}
		var exists int
		if err := t.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM sources WHERE source_id = ?`, src.ID).Scan(&exists); err != nil {
			return err
		}
		if exists > 0 {
			return domain.E("SOURCE_EXISTS", 409,
				"source %q already exists; update it instead of re-creating", src.ID)
		}
		version, err := nextSourceVersionTx(ctx, t, src.ID)
		if err != nil {
			return err
		}
		_, err = t.ExecContext(ctx, `
			INSERT INTO sources(source_id, tenant, adapter_type, parser_type, enabled,
			  config_version, config_json, created_at, updated_at)
			VALUES(?,?,?,?,?,?,?,?,?)`,
			src.ID, src.Tenant, src.AdapterType, src.ParserType, boolInt(src.Enabled),
			version, src.ConfigJSON, fmtTime(now), fmtTime(now))
		return err
	})
}

// UpdateSource applies a definition change guarded by the caller's expected
// config_version and advances the version by one. It only persists fields:
// fetching is always a separate explicit operation.
func (s *Store) UpdateSource(ctx context.Context, src domain.Source,
	expectedVersion int64, now time.Time) error {
	return s.tx(ctx, func(t *sql.Tx) error {
		res, err := t.ExecContext(ctx, `
			UPDATE sources
			SET adapter_type = ?, parser_type = ?, enabled = ?,
			    config_version = config_version + 1, config_json = ?, updated_at = ?
			WHERE source_id = ? AND config_version = ?`,
			src.AdapterType, src.ParserType, boolInt(src.Enabled), src.ConfigJSON,
			fmtTime(now), src.ID, expectedVersion)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			var exists int
			if err := t.QueryRowContext(ctx,
				`SELECT COUNT(*) FROM sources WHERE source_id = ?`, src.ID).Scan(&exists); err != nil {
				return err
			}
			if exists == 0 {
				return domain.NotFound("source")
			}
			return domain.E("SOURCE_VERSION_CONFLICT", 409,
				"source %q changed since version %d; reload and retry", src.ID, expectedVersion)
		}
		return nil
	})
}

// ListSources returns every channel definition ordered by ID.
func (s *Store) ListSources(ctx context.Context) ([]domain.Source, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT source_id, tenant, adapter_type, parser_type, enabled, config_version, config_json
		FROM sources ORDER BY source_id`)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []domain.Source
	for rows.Next() {
		var src domain.Source
		var enabled int
		if err := rows.Scan(&src.ID, &src.Tenant, &src.AdapterType, &src.ParserType,
			&enabled, &src.ConfigVersion, &src.ConfigJSON); err != nil {
			return nil, mapErr(err)
		}
		src.Enabled = enabled == 1
		out = append(out, src)
	}
	return out, mapErr(rows.Err())
}

// DeleteSource removes a channel entirely: its offers, source row, and any
// resources it was the last supplier of (with their leases, reservations, and
// health rows) are deleted so a future re-upload rebuilds the pool from
// scratch and inventory counts reflect live supply only.
// Resources with an active lease are kept until the lease ends.
func (s *Store) DeleteSource(ctx context.Context, sourceID string, now time.Time) (int, error) {
	revoked := 0
	err := s.tx(ctx, func(t *sql.Tx) error {
		if err := ensureSupplySchemaTx(ctx, t); err != nil {
			return err
		}
		var currentVersion int64
		err := t.QueryRowContext(ctx,
			`SELECT config_version FROM sources WHERE source_id = ?`, sourceID).
			Scan(&currentVersion)
		if errors.Is(err, sql.ErrNoRows) {
			return domain.NotFound("source")
		}
		if err != nil {
			return err
		}
		if _, err := t.ExecContext(ctx, `
			INSERT INTO source_version_watermarks(source_id, last_version)
			VALUES(?,?)
			ON CONFLICT(source_id) DO UPDATE SET
			  last_version = MAX(source_version_watermarks.last_version, excluded.last_version)`,
			sourceID, currentVersion); err != nil {
			return err
		}
		// A source snapshot must not become reusable if the same ID is later
		// recreated. Clearing the connection snapshot is enough to prevent an
		// old proxy being returned; project CD remains independent.
		if _, err := t.ExecContext(ctx, `
			UPDATE sticky_bindings SET
			  state = 'INVALID', connection_ciphertext = '', validation_json = '{}',
			  updated_at = ?
			WHERE source_id = ? AND state IN ('ACTIVE','EXPIRED','CLAIMING')`,
			fmtTime(now), sourceID); err != nil {
			return err
		}
		res, err := t.ExecContext(ctx,
			`DELETE FROM source_offers WHERE source_id = ?`, sourceID)
		if err != nil {
			return err
		}
		if n, err := res.RowsAffected(); err == nil {
			revoked = int(n)
		}
		if _, err := t.ExecContext(ctx,
			`DELETE FROM sources WHERE source_id = ?`, sourceID); err != nil {
			return err
		}
		// Orphaned resources: no offer from any source left, and no active
		// lease. Delete their history rows first to satisfy foreign keys,
		// then the resource itself, then now-unreferenced credentials.
		if _, err := t.ExecContext(ctx, `
			DELETE FROM leases WHERE resource_id IN (
			  SELECT r.resource_id FROM proxy_resources r
			  WHERE NOT EXISTS (SELECT 1 FROM source_offers o WHERE o.resource_id = r.resource_id)
			    AND NOT EXISTS (SELECT 1 FROM leases l WHERE l.resource_id = r.resource_id AND l.state = 'ACTIVE')
			)`); err != nil {
			return err
		}
		if _, err := t.ExecContext(ctx, `
			DELETE FROM reservations WHERE resource_id IN (
			  SELECT r.resource_id FROM proxy_resources r
			  WHERE NOT EXISTS (SELECT 1 FROM source_offers o WHERE o.resource_id = r.resource_id)
			    AND NOT EXISTS (SELECT 1 FROM leases l WHERE l.resource_id = r.resource_id AND l.state = 'ACTIVE')
			)`); err != nil {
			return err
		}
		if _, err := t.ExecContext(ctx, `
			DELETE FROM scoped_health WHERE resource_id IN (
			  SELECT r.resource_id FROM proxy_resources r
			  WHERE NOT EXISTS (SELECT 1 FROM source_offers o WHERE o.resource_id = r.resource_id)
			    AND NOT EXISTS (SELECT 1 FROM leases l WHERE l.resource_id = r.resource_id AND l.state = 'ACTIVE')
			)`); err != nil {
			return err
		}
		if _, err := t.ExecContext(ctx, `
			DELETE FROM proxy_resources
			WHERE NOT EXISTS (SELECT 1 FROM source_offers o WHERE o.resource_id = proxy_resources.resource_id)
			  AND NOT EXISTS (
			    SELECT 1 FROM leases l
			    WHERE l.resource_id = proxy_resources.resource_id AND l.state = 'ACTIVE')`); err != nil {
			return err
		}
		if _, err := t.ExecContext(ctx, `
			DELETE FROM proxy_credentials WHERE NOT EXISTS (
			  SELECT 1 FROM proxy_resources r
			  WHERE r.credential_version = proxy_credentials.credential_version)`); err != nil {
			return err
		}
		return nil
	})
	return revoked, mapErr(err)
}

type PeriodicFilters struct {
	Profile      string
	Now          time.Time
	Limit        int
	SourceID     string
	ExcludeByIDs []string
}

// SelectPeriodicCandidates returns the least-recently validated candidates of
// self-uploaded (custom) sources for bounded background revalidation. This
// only refreshes health/ranking evidence and never authorizes delivery.
func (s *Store) SelectPeriodicCandidates(ctx context.Context, f PeriodicFilters) ([]domain.Candidate, error) {
	if f.Limit <= 0 {
		f.Limit = 60
	}
	query := `
		SELECT o.offer_id, o.source_id, o.resource_id, o.source_record_id, o.source_generation,
		       o.provider_expires_at, o.local_retention_deadline, o.expiry_basis, o.claims_json,
		       o.max_pool_leases, o.capacity_group, o.state,
		       r.transport, r.host, r.port, r.credential_version, r.session_policy, r.resource_kind,
		       COALESCE(cr.username, ''), COALESCE(cr.password, ''),
		       COALESCE(h.last_outcome, 'UNKNOWN'), h.last_pass_at, COALESCE(h.consecutive_failures, 0),
		       r.incarnation, src.config_version
		FROM source_offers o
		JOIN proxy_resources r ON r.resource_id = o.resource_id AND r.disabled = 0
		JOIN sources src ON src.source_id = o.source_id AND src.enabled = 1 AND src.adapter_type = 'custom'
		LEFT JOIN source_circuits sc ON sc.source_id = o.source_id
		LEFT JOIN proxy_credentials cr ON cr.credential_version = r.credential_version
		LEFT JOIN scoped_health h
		  ON h.resource_id = r.resource_id
		 AND h.context_key = r.credential_version || '|' || r.session_policy
		 AND h.profile = ?
		WHERE o.state = 'ENABLED'
		  AND (o.provider_expires_at IS NULL OR o.provider_expires_at > ?)
		  AND (o.local_retention_deadline IS NULL OR o.local_retention_deadline > ?)
		  AND (h.cooldown_until IS NULL OR h.cooldown_until <= ?)
		  AND (sc.cooldown_until IS NULL OR sc.cooldown_until <= ?)
		  AND NOT EXISTS (
		        SELECT 1 FROM leases l
		        WHERE l.resource_id = r.resource_id AND l.state = 'ACTIVE')`
	args := []any{f.Profile, fmtTime(f.Now), fmtTime(f.Now), fmtTime(f.Now), fmtTime(f.Now)}
	if f.SourceID != "" {
		query += " AND o.source_id = ?"
		args = append(args, f.SourceID)
	}
	if len(f.ExcludeByIDs) > 0 {
		query += " AND r.resource_id NOT IN (" + placeholders(len(f.ExcludeByIDs)) + ")"
		for _, v := range f.ExcludeByIDs {
			args = append(args, v)
		}
	}
	query += `
		ORDER BY (h.last_pass_at IS NULL) DESC, h.last_pass_at ASC, random()
		LIMIT ?`
	args = append(args, f.Limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []domain.Candidate
	for rows.Next() {
		var c domain.Candidate
		var providerExp, retention, lastPass any
		var claimsJSON string
		var port int
		if err := rows.Scan(
			&c.Offer.ID, &c.Offer.SourceID, &c.Offer.ResourceID, &c.Offer.SourceRecordID,
			&c.Offer.SourceGeneration, &providerExp, &retention, &c.Offer.ExpiryBasis,
			&claimsJSON, &c.Offer.MaxPoolLeases, &c.Offer.CapacityGroup, &c.Offer.State,
			&c.Resource.Conn.Transport, &c.Resource.Conn.Host, &port,
			&c.Resource.CredentialVersion, &c.Resource.Session, &c.Resource.Kind,
			&c.Resource.Conn.Username, &c.Resource.Conn.Password,
			&c.LastOutcome, &lastPass, &c.ConsecutiveFailures,
			&c.Incarnation, &c.SourceConfigVersion,
		); err != nil {
			return nil, mapErr(err)
		}
		c.Resource.Conn.Port = port
		c.Resource.ID = c.Offer.ResourceID
		if c.Offer.ProviderExpiresAt, err = scanTime(providerExp); err != nil {
			return nil, err
		}
		if c.Offer.LocalRetentionDeadline, err = scanTime(retention); err != nil {
			return nil, err
		}
		if c.LastPassAt, err = scanTime(lastPass); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(claimsJSON), &c.Offer.Claims); err != nil {
			return nil, fmt.Errorf("decode claims: %w", err)
		}
		out = append(out, c)
	}
	return out, mapErr(rows.Err())
}

// RetireResource removes a resource whose supply is definitively dead, e.g.
// the proxy rejected valid credentials: its offers, reservations, leases,
// health rows, and the resource itself are deleted so inventory counts
// reflect live supply only. There is deliberately no tombstone or blacklist:
// the same connection may return in a future upload and is then stored as an
// unvalidated candidate under a fresh incarnation. An active lease (rare
// race) keeps the resource row so the lease stays resolvable.
func (s *Store) RetireResource(ctx context.Context, resourceID, reason string, now time.Time) (int, error) {
	return s.RetireResourceScoped(ctx, resourceID, "", reason, now)
}

// RetireResourceScoped is the incarnation-guarded retire used by every
// background or validation path that began from a selected candidate. A
// delete + re-import can reuse the same resource ID under a new incarnation;
// a late AUTH failure from the old probe must never delete that new object.
func (s *Store) RetireResourceScoped(ctx context.Context, resourceID, incarnation,
	reason string, now time.Time) (int, error) {
	return s.RetireResourceOwned(ctx, resourceID, incarnation, "", 0, reason, now)
}

// RetireResourceOwned additionally requires the source offer and its config
// version to still match the probe that selected it. A source reconfiguration
// can therefore not turn an old AUTH result into deletion of a new offer on
// the same resource incarnation.
func (s *Store) RetireResourceOwned(ctx context.Context, resourceID, incarnation,
	sourceID string, sourceConfigVersion int64, reason string, now time.Time) (int, error) {
	revoked := 0
	err := s.tx(ctx, func(t *sql.Tx) error {
		objectGuard := `EXISTS (SELECT 1 FROM proxy_resources r
		  WHERE r.resource_id = source_offers.resource_id`
		objectArgs := make([]any, 0, 2)
		if incarnation != "" {
			objectGuard += ` AND r.incarnation = ?`
			objectArgs = append(objectArgs, incarnation)
		}
		objectGuard += ` AND source_offers.resource_id = ?)`
		objectArgs = append(objectArgs, resourceID)
		offerGuard := objectGuard
		offerArgs := append([]any(nil), objectArgs...)
		if sourceID != "" {
			offerGuard += ` AND source_id = ?`
			offerArgs = append(offerArgs, sourceID)
		}
		if sourceConfigVersion > 0 {
			offerGuard += ` AND EXISTS (SELECT 1 FROM sources src
			  WHERE src.source_id = source_offers.source_id
			    AND src.config_version = ?)`
			offerArgs = append(offerArgs, sourceConfigVersion)
		}
		res, err := t.ExecContext(ctx,
			`DELETE FROM source_offers WHERE `+offerGuard, offerArgs...)
		if err != nil {
			return err
		}
		if n, err := res.RowsAffected(); err == nil {
			revoked = int(n)
		}
		// If the owned offer is gone (or changed), the probe's delete guard
		// already failed. Do not fall through and delete the resource itself.
		if revoked == 0 && sourceID != "" {
			return nil
		}
		guard := `resource_id IN (
		  SELECT resource_id FROM proxy_resources WHERE resource_id = ?`
		guardArgs := []any{resourceID}
		if incarnation != "" {
			guard += ` AND incarnation = ?`
			guardArgs = append(guardArgs, incarnation)
		}
		guard += `)`
		var active int
		if err := t.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM leases WHERE `+guard+` AND state = 'ACTIVE'`,
			guardArgs...).Scan(&active); err != nil {
			return err
		}
		if active == 0 {
			if _, err := t.ExecContext(ctx,
				`DELETE FROM leases WHERE `+guard, guardArgs...); err != nil {
				return err
			}
			if _, err := t.ExecContext(ctx,
				`DELETE FROM reservations WHERE `+guard, guardArgs...); err != nil {
				return err
			}
			if _, err := t.ExecContext(ctx,
				`DELETE FROM scoped_health WHERE `+guard, guardArgs...); err != nil {
				return err
			}
			if _, err := t.ExecContext(ctx,
				`DELETE FROM proxy_resources WHERE `+guard, guardArgs...); err != nil {
				return err
			}
		}
		if _, err := t.ExecContext(ctx, `
			DELETE FROM proxy_credentials WHERE NOT EXISTS (
			  SELECT 1 FROM proxy_resources r
			  WHERE r.credential_version = proxy_credentials.credential_version)`); err != nil {
			return err
		}
		return nil
	})
	return revoked, err
}

// SweepOrphanResources deletes resources without any enabled offer and
// without an active lease or reservation. This covers legacy provider
// inventory revoked by the v6 migration (on-demand supply imports nothing)
// and any leftover delivery-scoped rows.
func (s *Store) SweepOrphanResources(ctx context.Context) (int64, error) {
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM proxy_resources
		WHERE NOT EXISTS (SELECT 1 FROM source_offers o
		                  WHERE o.resource_id = proxy_resources.resource_id AND o.state = 'ENABLED')
		  AND NOT EXISTS (SELECT 1 FROM leases l
		                  WHERE l.resource_id = proxy_resources.resource_id AND l.state = 'ACTIVE')
		  AND NOT EXISTS (SELECT 1 FROM reservations rv
		                  WHERE rv.resource_id = proxy_resources.resource_id
		                    AND rv.state IN ('RESERVED','VALIDATING'))`)
	if err != nil {
		return 0, mapErr(err)
	}
	n, _ := res.RowsAffected()
	_, _ = s.db.ExecContext(ctx, `
		DELETE FROM proxy_credentials WHERE NOT EXISTS (
		  SELECT 1 FROM proxy_resources r
		  WHERE r.credential_version = proxy_credentials.credential_version)`)
	_, _ = s.db.ExecContext(ctx, `
		DELETE FROM scoped_health WHERE NOT EXISTS (
		  SELECT 1 FROM proxy_resources r WHERE r.resource_id = scoped_health.resource_id)`)
	return n, nil
}

type DailyStat struct {
	SourceID string  `json:"source_id"`
	Proxies  int64   `json:"proxies"`
	Pass     int64   `json:"pass"`
	Fail     int64   `json:"fail"`
	PassRate float64 `json:"pass_rate"`
}

// EnsureSupplySchema installs additive supply-side tables idempotently. It
// does not bump the user_version because it is safe on legacy and current
// databases alike. Migration callers should invoke this after their schema
// transaction; RecordAttemptAttributed returns a clear store error if an
// older deployment skipped it.
func ensureSupplySchemaTx(ctx context.Context, t *sql.Tx) error {
	_, err := t.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS validation_attempt_sources (
		  attempt_id TEXT PRIMARY KEY,
		  source_id TEXT NOT NULL,
		  source_config_version INTEGER NOT NULL DEFAULT 0,
		  completed_at TEXT NOT NULL,
		  outcome TEXT NOT NULL DEFAULT ''
		);
		CREATE INDEX IF NOT EXISTS idx_attempt_sources_source_day
		  ON validation_attempt_sources(source_id, completed_at);
		CREATE INDEX IF NOT EXISTS idx_attempt_sources_completed
		  ON validation_attempt_sources(completed_at);
		CREATE TABLE IF NOT EXISTS source_stat_counters (
		  source_id TEXT NOT NULL,
		  day TEXT NOT NULL,
		  pass_count INTEGER NOT NULL DEFAULT 0,
		  fail_count INTEGER NOT NULL DEFAULT 0,
		  updated_at TEXT NOT NULL,
		  PRIMARY KEY(source_id, day)
		);
		CREATE INDEX IF NOT EXISTS idx_source_stats_day
		  ON source_stat_counters(day, source_id);
		CREATE TABLE IF NOT EXISTS source_version_watermarks (
		  source_id TEXT PRIMARY KEY,
		  last_version INTEGER NOT NULL
		);
		INSERT INTO source_version_watermarks(source_id, last_version)
		SELECT source_id, config_version FROM sources
		WHERE true
		ON CONFLICT(source_id) DO UPDATE SET
		  last_version = MAX(source_version_watermarks.last_version, excluded.last_version);`)
	if err != nil {
		return err
	}
	var hasOutcome int
	if err := t.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM pragma_table_info('validation_attempt_sources')
		WHERE name = 'outcome'`).Scan(&hasOutcome); err != nil {
		return err
	}
	if hasOutcome == 0 {
		_, err = t.ExecContext(ctx,
			`ALTER TABLE validation_attempt_sources ADD COLUMN outcome TEXT NOT NULL DEFAULT ''`)
	}
	return err
}

// nextSourceVersionTx returns a version never previously assigned to this
// source ID in this database. The watermark row is held by the write
// transaction, so concurrent recreate attempts cannot allocate the same one.
func nextSourceVersionTx(ctx context.Context, t *sql.Tx, sourceID string) (int64, error) {
	var last int64
	err := t.QueryRowContext(ctx, `
		SELECT last_version FROM source_version_watermarks WHERE source_id = ?`,
		sourceID).Scan(&last)
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	if err != nil {
		return 0, err
	}
	next := last + 1
	if next < 1 {
		next = 1
	}
	if _, err := t.ExecContext(ctx, `
		INSERT INTO source_version_watermarks(source_id, last_version) VALUES(?,?)
		ON CONFLICT(source_id) DO UPDATE SET last_version = excluded.last_version`,
		sourceID, next); err != nil {
		return 0, err
	}
	return next, nil
}

func (s *Store) EnsureSupplySchema(ctx context.Context) error {
	return s.tx(ctx, func(t *sql.Tx) error {
		return ensureSupplySchemaTx(ctx, t)
	})
}

// RecordSourceStats increments durable per-source attempt counters. These
// counters intentionally survive offer/resource deletion so source quality
// history is not erased when an AUTH failure removes the candidate.
func (s *Store) RecordSourceStats(ctx context.Context, sourceID string,
	pass, fail int, now time.Time) error {
	if sourceID == "" || pass < 0 || fail < 0 {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO source_stat_counters(source_id, day, pass_count, fail_count, updated_at)
		VALUES(?,?,?,?,?)
		ON CONFLICT(source_id, day) DO UPDATE SET
		  pass_count = source_stat_counters.pass_count + excluded.pass_count,
		  fail_count = source_stat_counters.fail_count + excluded.fail_count,
		  updated_at = excluded.updated_at`,
		sourceID, now.UTC().Format("2006-01-02"), pass, fail, fmtTime(now))
	return mapErr(err)
}

// SourceStats returns the durable, deletion-safe source counters. It is
// intentionally separate from DailySourceStats, whose window is exact. The
// counters are bucketed by UTC date, so their callers should request whole
// UTC days; a partial [from,to) window is not representable here.
func (s *Store) SourceStats(ctx context.Context, from, to time.Time) ([]DailyStat, error) {
	// The counters are day buckets, while this API accepts a precise window.
	// Merge only buckets whose UTC day intersects [from,to), then use the
	// precise validation_attempt_sources rows below to keep exact boundaries.
	fromDay := from.UTC().Format("2006-01-02")
	toDay := to.UTC().Format("2006-01-02")
	if to.After(to.UTC().Truncate(24 * time.Hour)) {
		toDay = to.UTC().AddDate(0, 0, 1).Format("2006-01-02")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT source_id,
		       SUM(pass_count) + SUM(fail_count) AS proxies,
		       SUM(pass_count) AS pass,
		       SUM(fail_count) AS fail
		FROM source_stat_counters
		WHERE day >= ? AND day < ?
		GROUP BY source_id
		ORDER BY source_id`,
		fromDay, toDay)
	if err != nil {
		return nil, nil
	}
	defer rows.Close()
	var out []DailyStat
	for rows.Next() {
		var d DailyStat
		if err := rows.Scan(&d.SourceID, &d.Proxies, &d.Pass, &d.Fail); err != nil {
			return nil, mapErr(err)
		}
		if d.Proxies > 0 {
			d.PassRate = float64(d.Pass) / float64(d.Proxies)
		}
		out = append(out, d)
	}
	return out, mapErr(rows.Err())
}

func (s *Store) DailySourceStats(ctx context.Context, from, to time.Time) ([]DailyStat, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT source_id, COUNT(*),
		       SUM(outcome = 'PASS'), SUM(outcome = 'FAIL')
		FROM validation_attempt_sources
		WHERE completed_at >= ? AND completed_at < ?
		  AND outcome IN ('PASS','FAIL')
		GROUP BY source_id
		ORDER BY source_id`, fmtTime(from), fmtTime(to))
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []DailyStat
	for rows.Next() {
		var d DailyStat
		if err := rows.Scan(&d.SourceID, &d.Proxies, &d.Pass, &d.Fail); err != nil {
			return nil, mapErr(err)
		}
		if d.Proxies > 0 {
			d.PassRate = float64(d.Pass) / float64(d.Proxies)
		}
		out = append(out, d)
	}
	return out, mapErr(rows.Err())
}

type SourceOverview struct {
	SourceID      string  `json:"source_id"`
	AdapterType   string  `json:"adapter_type"`
	ParserType    string  `json:"parser_type"`
	Enabled       bool    `json:"enabled"`
	Offers        int64   `json:"offers"`
	EnabledOffers int64   `json:"enabled_offers"`
	HealthPass    int64   `json:"health_pass"`
	HealthFail    int64   `json:"health_fail"`
	LastValidated *string `json:"last_validated_at"`
	CircuitState  string  `json:"circuit_state"`
	CooldownUntil *string `json:"source_cooldown_until"`
}

func (s *Store) SourcesOverview(ctx context.Context) ([]SourceOverview, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT s.source_id, s.adapter_type, s.parser_type, s.enabled,
		       COALESCE(o.offers, 0), COALESCE(o.enabled_offers, 0),
		       COALESCE(h.health_pass, 0), COALESCE(h.health_fail, 0),
		       va.last_validated, COALESCE(sc.state, 'CLOSED'), sc.cooldown_until
		FROM sources s
		LEFT JOIN source_circuits sc ON sc.source_id = s.source_id
		LEFT JOIN (
		  SELECT source_id, COUNT(*) AS offers, SUM(state = 'ENABLED') AS enabled_offers
		  FROM source_offers GROUP BY source_id
		) o ON o.source_id = s.source_id
		LEFT JOIN (
		  SELECT o2.source_id AS source_id,
		         SUM(h.last_outcome = 'PASS') AS health_pass,
		         SUM(h.last_outcome = 'FAIL') AS health_fail
		  FROM source_offers o2
		  JOIN scoped_health h ON h.resource_id = o2.resource_id
		  GROUP BY o2.source_id
		) h ON h.source_id = s.source_id
		LEFT JOIN (
		  SELECT o3.source_id AS source_id, MAX(va.completed_at) AS last_validated
		  FROM validation_attempts va
		  JOIN proxy_resources r2 ON r2.resource_id = va.resource_id
		  JOIN source_offers o3 ON o3.resource_id = r2.resource_id
		  GROUP BY o3.source_id
		) va ON va.source_id = s.source_id
		ORDER BY s.source_id`)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []SourceOverview
	for rows.Next() {
		var o SourceOverview
		var enabled int
		var lastValidated, cooldown any
		if err := rows.Scan(&o.SourceID, &o.AdapterType, &o.ParserType, &enabled, &o.Offers, &o.EnabledOffers,
			&o.HealthPass, &o.HealthFail, &lastValidated, &o.CircuitState, &cooldown); err != nil {
			return nil, mapErr(err)
		}
		o.Enabled = enabled == 1
		if lastValidated != nil {
			if v, ok := lastValidated.(string); ok {
				o.LastValidated = &v
			}
		}
		if cooldown != nil {
			if v, ok := cooldown.(string); ok {
				o.CooldownUntil = &v
			}
		}
		out = append(out, o)
	}
	return out, mapErr(rows.Err())
}

type SelectionFilters struct {
	Profile      string
	Now          time.Time
	UsefulCutoff time.Time
	Transports   []domain.Transport
	Kinds        []domain.ResourceKind
	SourceIDs    []string
	Countries    []string
	ExcludeByIDs []string
	// ConsumerID is accepted for call-site stability but no longer gates
	// selection: the fixed project CD is filtered before selection and
	// rechecked inside the reservation transaction.
	ConsumerID string
	Limit      int
}

// StaticBatchLive rechecks the exact source configuration that produced a
// request-local static batch. Callers invoke it immediately before dispatch;
// the final delivery transaction also rechecks source enable/version.
func (s *Store) StaticBatchLive(ctx context.Context, sourceID string,
	sourceConfigVersion int64, batchID string, now time.Time) (bool, error) {
	if sourceID == "" || sourceConfigVersion <= 0 || batchID == "" {
		return false, nil
	}
	var enabled int
	var actualVersion int64
	err := s.db.QueryRowContext(ctx, `
		SELECT enabled, config_version FROM sources
		WHERE source_id = ? AND adapter_type IN ('site_fetch','static_url')`,
		sourceID).Scan(&enabled, &actualVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return enabled == 1 && actualVersion == sourceConfigVersion, nil
}

// SelectCandidates returns eligible, unthrottled candidates ordered so that
// recently passing, then untested, then failing resources are tried first.
// Sources with an active circuit cooldown are excluded; sources whose
// cooldown has elapsed are ranked last so they act as half-open trials.
func (s *Store) SelectCandidates(ctx context.Context, f SelectionFilters) ([]domain.Candidate, error) {
	if f.Limit <= 0 {
		f.Limit = 8
	}
	args := []any{f.Profile, fmtTime(f.UsefulCutoff), fmtTime(f.UsefulCutoff),
		fmtTime(f.Now), fmtTime(f.Now), fmtTime(f.Now), fmtTime(f.Now)}
	query := `
		SELECT o.offer_id, o.source_id, o.resource_id, o.source_record_id, o.source_generation,
		       o.provider_expires_at, o.local_retention_deadline, o.expiry_basis, o.claims_json,
		       o.max_pool_leases, o.capacity_group, o.state,
		       r.transport, r.host, r.port, r.credential_version, r.session_policy, r.resource_kind,
		       COALESCE(cr.username, ''), COALESCE(cr.password, ''),
		       COALESCE(h.last_outcome, 'UNKNOWN'), h.last_pass_at, COALESCE(h.consecutive_failures, 0),
		       ro.exit_ip, ro.country_code, ro.country, ro.region, ro.city, ro.isp, ro.observed_at,
		       src.config_version, r.incarnation
		FROM source_offers o
		JOIN proxy_resources r ON r.resource_id = o.resource_id AND r.disabled = 0
		JOIN sources src ON src.source_id = o.source_id AND src.enabled = 1
		  AND src.adapter_type = 'custom'
		LEFT JOIN proxy_credentials cr ON cr.credential_version = r.credential_version
		LEFT JOIN source_circuits sc ON sc.source_id = o.source_id
		LEFT JOIN resource_observations ro ON ro.resource_id = r.resource_id
		LEFT JOIN scoped_health h
		  ON h.resource_id = r.resource_id
		 AND h.context_key = r.credential_version || '|' || r.session_policy
		 AND h.profile = ?
		WHERE o.state = 'ENABLED'
		  AND src.adapter_type = 'custom'
		  AND (o.provider_expires_at IS NULL OR o.provider_expires_at > ?)
		  AND (o.local_retention_deadline IS NULL OR o.local_retention_deadline > ?)
		  AND (h.cooldown_until IS NULL OR h.cooldown_until <= ?)
		  AND (sc.cooldown_until IS NULL OR sc.cooldown_until <= ?)
		  AND (
		        (SELECT COUNT(*) FROM leases l
		         WHERE l.resource_id = r.resource_id AND l.state = 'ACTIVE' AND l.deadline > ?)
		      + (SELECT COUNT(*) FROM reservations rv
		         WHERE rv.resource_id = r.resource_id
		           AND rv.state IN ('RESERVED','VALIDATING') AND rv.deadline > ?)
		      ) < o.max_pool_leases`
	if len(f.Transports) > 0 {
		query += " AND r.transport IN (" + placeholders(len(f.Transports)) + ")"
		for _, v := range f.Transports {
			args = append(args, string(v))
		}
	}
	if len(f.Kinds) > 0 {
		query += " AND r.resource_kind IN (" + placeholders(len(f.Kinds)) + ")"
		for _, v := range f.Kinds {
			args = append(args, string(v))
		}
	}
	if len(f.SourceIDs) > 0 {
		query += " AND o.source_id IN (" + placeholders(len(f.SourceIDs)) + ")"
		for _, v := range f.SourceIDs {
			args = append(args, v)
		}
	}
	if len(f.Countries) > 0 {
		// Unobserved candidates stay eligible so a country filter can be
		// satisfied by probing; the fresh observation is re-checked before
		// delivery.
		query += " AND (ro.country_code IN (" + placeholders(len(f.Countries)) + ") OR ro.resource_id IS NULL)"
		for _, v := range f.Countries {
			args = append(args, strings.ToUpper(v))
		}
	}
	if len(f.ExcludeByIDs) > 0 {
		query += " AND r.resource_id NOT IN (" + placeholders(len(f.ExcludeByIDs)) + ")"
		for _, v := range f.ExcludeByIDs {
			args = append(args, v)
		}
	}
	query += `
		ORDER BY CASE WHEN sc.state = 'OPEN' THEN 1 ELSE 0 END,
		         (h.last_outcome = 'PASS') DESC,
		         (h.last_outcome = 'FAIL') ASC,
		         h.last_pass_at DESC,
		         random()
		LIMIT ?`
	args = append(args, f.Limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []domain.Candidate
	for rows.Next() {
		var c domain.Candidate
		var providerExp, retention, lastPass any
		var claimsJSON string
		var port int
		var obsIP, obsCountryCode, obsCountry, obsRegion, obsCity, obsISP, obsAt sql.NullString
		if err := rows.Scan(
			&c.Offer.ID, &c.Offer.SourceID, &c.Offer.ResourceID, &c.Offer.SourceRecordID,
			&c.Offer.SourceGeneration, &providerExp, &retention, &c.Offer.ExpiryBasis,
			&claimsJSON, &c.Offer.MaxPoolLeases, &c.Offer.CapacityGroup, &c.Offer.State,
			&c.Resource.Conn.Transport, &c.Resource.Conn.Host, &port,
			&c.Resource.CredentialVersion, &c.Resource.Session, &c.Resource.Kind,
			&c.Resource.Conn.Username, &c.Resource.Conn.Password,
			&c.LastOutcome, &lastPass, &c.ConsecutiveFailures,
			&obsIP, &obsCountryCode, &obsCountry, &obsRegion, &obsCity, &obsISP, &obsAt,
			&c.SourceConfigVersion, &c.Incarnation,
		); err != nil {
			return nil, mapErr(err)
		}
		c.Resource.Conn.Port = port
		c.Resource.ID = c.Offer.ResourceID
		if c.Offer.ProviderExpiresAt, err = scanTime(providerExp); err != nil {
			return nil, err
		}
		if c.Offer.LocalRetentionDeadline, err = scanTime(retention); err != nil {
			return nil, err
		}
		if c.LastPassAt, err = scanTime(lastPass); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(claimsJSON), &c.Offer.Claims); err != nil {
			return nil, fmt.Errorf("decode claims: %w", err)
		}
		if obsIP.Valid && obsIP.String != "" {
			obs := &domain.Observation{
				ExitIP:      obsIP.String,
				CountryCode: obsCountryCode.String,
				Country:     obsCountry.String,
				Region:      obsRegion.String,
				City:        obsCity.String,
				ISP:         obsISP.String,
			}
			if obsAt.Valid {
				if t, err := time.Parse(time.RFC3339Nano, obsAt.String); err == nil {
					obs.ObservedAt = t
				}
			}
			c.Observation = obs
		}
		out = append(out, c)
	}
	return out, mapErr(rows.Err())
}

func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

// ExpireBindingsForFingerprints drops the connection snapshots of sticky
// bindings whose proxy was definitively retired. It only touches binding
// rows: the project CD windows stay independent and keep their expiry.
func (s *Store) ExpireBindingsForFingerprints(ctx context.Context,
	fingerprints []string, now time.Time) (int64, error) {
	if len(fingerprints) == 0 {
		return 0, nil
	}
	args := append([]any{fmtTime(now)}, toAny(fingerprints)...)
	res, err := s.db.ExecContext(ctx, `
		UPDATE sticky_bindings
		SET state = 'INVALID', connection_ciphertext = '', updated_at = ?
		WHERE state = 'ACTIVE'
		  AND fingerprint IN (`+placeholders(len(fingerprints))+`)`, args...)
	if err != nil {
		return 0, mapErr(err)
	}
	n, err := res.RowsAffected()
	return n, mapErr(err)
}
