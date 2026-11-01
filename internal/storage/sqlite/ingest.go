package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"proxypool/internal/domain"
)

type ImportCounts struct {
	Added   int `json:"added"`
	Updated int `json:"updated"`
	Skipped int `json:"skipped"`
}

// ImportCandidates appends candidates to a self-uploaded source's pool.
// Uploads are additive: every parsed line becomes a stable offer keyed by the
// proxy's content fingerprint, and dead supply is cleaned up by validation,
// not by list replacement. Identical re-uploads are idempotent.
func (s *Store) ImportCandidates(ctx context.Context, candidates []domain.NormalizedCandidate, now time.Time) (ImportCounts, error) {
	var counts ImportCounts
	err := s.tx(ctx, func(t *sql.Tx) error {
		counts = ImportCounts{}
		for _, c := range candidates {
			if c.ResourceID == "" || c.OfferID == "" {
				counts.Skipped++
				continue
			}
			var tombstoned int
			if err := t.QueryRowContext(ctx, `
				SELECT COUNT(*) FROM eviction_tombstones
				WHERE resource_id = ? AND source_id = ? AND source_record_id = ?
				  AND source_generation = ?`,
				c.ResourceID, c.SourceID, c.RecordID, c.Generation).Scan(&tombstoned); err != nil {
				return err
			}
			if tombstoned > 0 {
				counts.Skipped++
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
				INSERT INTO proxy_resources(resource_id, tenant, transport, host, port, credential_version, session_policy, resource_kind, disabled, created_at, updated_at)
				VALUES(?,?,?,?,?,?,?,?,0,?,?)
				ON CONFLICT(resource_id) DO UPDATE SET updated_at = excluded.updated_at`,
				c.ResourceID, "default", string(c.Conn.Transport), c.Conn.Host, c.Conn.Port,
				c.CredentialVersion, string(c.Session), string(c.Kind), fmtTime(now), fmtTime(now)); err != nil {
				return err
			}
			var exists int
			if err := t.QueryRowContext(ctx, `
				SELECT COUNT(*) FROM source_offers
				WHERE source_id = ? AND source_record_id = ? AND source_generation = ?`,
				c.SourceID, c.RecordID, c.Generation).Scan(&exists); err != nil {
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
	})
	return counts, err
}

// DeleteSource removes a channel entirely: its offers, tombstones, source
// row, and any resources it was the last supplier of (with their leases,
// reservations, and health rows) are deleted so a future re-upload rebuilds
// the pool from scratch and inventory counts reflect live supply only.
// Resources with an active lease are kept until the lease ends.
func (s *Store) DeleteSource(ctx context.Context, sourceID string, now time.Time) (int, error) {
	revoked := 0
	err := s.tx(ctx, func(t *sql.Tx) error {
		var exists int
		if err := t.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM sources WHERE source_id = ?`, sourceID).Scan(&exists); err != nil {
			return err
		}
		if exists == 0 {
			return domain.NotFound("source")
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
			`DELETE FROM eviction_tombstones WHERE source_id = ?`, sourceID); err != nil {
			return err
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
		       COALESCE(h.last_outcome, 'UNKNOWN'), h.last_pass_at, COALESCE(h.consecutive_failures, 0)
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

// RetireResource removes a dead resource entirely: every enabled offer is
// tombstoned (so re-uploading the same list cannot revive it) and the
// resource itself plus its offers, reservations, leases, and health rows are
// deleted so inventory counts reflect live supply only. An active lease
// (rare race) keeps the resource row so the lease stays resolvable.
func (s *Store) RetireResource(ctx context.Context, resourceID, reason string, now time.Time) (int, error) {
	revoked := 0
	err := s.tx(ctx, func(t *sql.Tx) error {
		rows, err := t.QueryContext(ctx, `
			SELECT offer_id, source_id, source_record_id, source_generation
			FROM source_offers WHERE resource_id = ? AND state = 'ENABLED'`, resourceID)
		if err != nil {
			return err
		}
		type offerKey struct{ offerID, sourceID, recordID, generation string }
		var offers []offerKey
		for rows.Next() {
			var k offerKey
			if err := rows.Scan(&k.offerID, &k.sourceID, &k.recordID, &k.generation); err != nil {
				rows.Close()
				return err
			}
			offers = append(offers, k)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, k := range offers {
			if _, err := t.ExecContext(ctx, `
				INSERT INTO eviction_tombstones(tombstone_id, resource_id, source_id, source_record_id,
				  source_generation, reason, created_at)
				VALUES(?,?,?,?,?,?,?)
				ON CONFLICT(resource_id, source_id, source_record_id, source_generation, reason) DO NOTHING`,
				domain.NewID("tmb_"), resourceID, k.sourceID, k.recordID, k.generation,
				reason, fmtTime(now)); err != nil {
				return err
			}
			revoked++
		}
		if _, err := t.ExecContext(ctx,
			`DELETE FROM source_offers WHERE resource_id = ?`, resourceID); err != nil {
			return err
		}
		var active int
		if err := t.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM leases WHERE resource_id = ? AND state = 'ACTIVE'`,
			resourceID).Scan(&active); err != nil {
			return err
		}
		if active == 0 {
			if _, err := t.ExecContext(ctx,
				`DELETE FROM leases WHERE resource_id = ?`, resourceID); err != nil {
				return err
			}
			if _, err := t.ExecContext(ctx,
				`DELETE FROM reservations WHERE resource_id = ?`, resourceID); err != nil {
				return err
			}
			if _, err := t.ExecContext(ctx,
				`DELETE FROM scoped_health WHERE resource_id = ?`, resourceID); err != nil {
				return err
			}
			if _, err := t.ExecContext(ctx,
				`DELETE FROM proxy_resources WHERE resource_id = ?`, resourceID); err != nil {
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

type DailyStat struct {
	SourceID string  `json:"source_id"`
	Proxies  int64   `json:"proxies"`
	Pass     int64   `json:"pass"`
	Fail     int64   `json:"fail"`
	PassRate float64 `json:"pass_rate"`
}

// RetireCandidates lists resources of the given adapter type whose scoped
// consecutive failure count reached the threshold. Custom sources reach it
// via nightly probes; provider sources via request-time acquire attempts.
func (s *Store) RetireCandidates(ctx context.Context, threshold int, adapterType, sourceID string) ([]string, error) {
	query := `
		SELECT DISTINCT so.resource_id
		FROM scoped_health h
		JOIN source_offers so ON so.resource_id = h.resource_id AND so.state = 'ENABLED'
		JOIN sources src ON src.source_id = so.source_id AND src.adapter_type = ?
		WHERE h.consecutive_failures >= ?`
	args := []any{adapterType, threshold}
	if sourceID != "" {
		query += " AND so.source_id = ?"
		args = append(args, sourceID)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, mapErr(err)
		}
		out = append(out, id)
	}
	return out, mapErr(rows.Err())
}

func (s *Store) DailySourceStats(ctx context.Context, from, to time.Time) ([]DailyStat, error) {
	rows, err := s.db.QueryContext(ctx, `
		WITH latest AS (
		  SELECT resource_id, outcome FROM (
		    SELECT va.resource_id, va.outcome,
		           ROW_NUMBER() OVER (PARTITION BY va.resource_id ORDER BY va.completed_at DESC) AS rn
		    FROM validation_attempts va
		    WHERE va.completed_at >= ? AND va.completed_at < ?
		  ) WHERE rn = 1
		),
		provenance AS (
		  SELECT DISTINCT resource_id, source_id FROM source_offers
		  UNION
		  SELECT DISTINCT resource_id, source_id FROM eviction_tombstones
		),
		pairs AS (
		  SELECT DISTINCT p.source_id, l.resource_id, l.outcome
		  FROM latest l
		  JOIN provenance p ON p.resource_id = l.resource_id
		)
		SELECT source_id, COUNT(*), SUM(outcome = 'PASS'), SUM(outcome = 'FAIL')
		FROM pairs
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
	Profile          string
	Now              time.Time
	UsefulCutoff     time.Time
	Transports       []domain.Transport
	Kinds            []domain.ResourceKind
	SourceIDs        []string
	Countries        []string
	ExcludeByIDs     []string
	ConsumerID       string
	ConsumerCooldown time.Duration
	Limit            int
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
		       ro.exit_ip, ro.country_code, ro.country, ro.region, ro.city, ro.isp, ro.observed_at
		FROM source_offers o
		JOIN proxy_resources r ON r.resource_id = o.resource_id AND r.disabled = 0
		JOIN sources src ON src.source_id = o.source_id AND src.enabled = 1
		LEFT JOIN proxy_credentials cr ON cr.credential_version = r.credential_version
		LEFT JOIN source_circuits sc ON sc.source_id = o.source_id
		LEFT JOIN resource_observations ro ON ro.resource_id = r.resource_id
		LEFT JOIN scoped_health h
		  ON h.resource_id = r.resource_id
		 AND h.context_key = r.credential_version || '|' || r.session_policy
		 AND h.profile = ?
		WHERE o.state = 'ENABLED'
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
	if f.ConsumerID != "" && f.ConsumerCooldown > 0 {
		query += ` AND NOT EXISTS (
		        SELECT 1 FROM consumer_proxy_cooldowns cpc
		        WHERE cpc.consumer_id = ? AND cpc.resource_id = r.resource_id
		          AND cpc.last_delivered_at > ?)`
		args = append(args, f.ConsumerID, fmtTime(f.Now.Add(-f.ConsumerCooldown)))
	}
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
