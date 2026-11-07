package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"proxypool/internal/domain"
	"proxypool/internal/policies"
)

const (
	CircuitClosed = "CLOSED"
	CircuitOpen   = "OPEN"
)

type SourceCircuit struct {
	SourceID            string
	State               string
	ConsecutiveFailures int
	OpenCount           int
	CooldownUntil       *time.Time
	LastOutcome         string
	LastSuccessAt       *time.Time
	LastFailureAt       *time.Time
	UpdatedAt           time.Time
}

func (c SourceCircuit) Cooling(now time.Time) bool {
	return c.State == CircuitOpen && c.CooldownUntil != nil && c.CooldownUntil.After(now)
}

type SourceCircuitPolicy struct {
	FailureThreshold int
	Base             time.Duration
	Max              time.Duration
}

// RecordSourceOutcome advances one source's circuit without checking which
// source configuration produced the outcome. It exists for metadata and test
// setup only; real probe callers must use RecordSourceOutcomeOwned so a late
// result cannot mutate a replaced source configuration.
func (s *Store) RecordSourceOutcome(ctx context.Context, sourceID, outcome string,
	now time.Time, p SourceCircuitPolicy) (SourceCircuit, error) {
	var out SourceCircuit
	err := s.tx(ctx, func(t *sql.Tx) error {
		var err error
		out, err = recordSourceOutcomeTx(ctx, t, sourceID, outcome, now, p)
		return err
	})
	return out, mapErr(err)
}

// RecordSourceOutcomeOwned advances a source circuit only while the source
// row still exists, is enabled, and carries the config version observed by
// the probe. The guard and circuit write share one transaction, so a source
// update or delete/recreate cannot let an old outcome mutate the new runtime.
// PASS closes and resets the circuit. FAIL opens it after the configured
// consecutive-failure threshold, or immediately when a post-cooldown trial
// fails; each reopening doubles the backoff up to the cap.
func (s *Store) RecordSourceOutcomeOwned(
	ctx context.Context, sourceID string, sourceConfigVersion int64, outcome string,
	now time.Time, p SourceCircuitPolicy,
) (SourceCircuit, error) {
	if sourceID == "" || sourceConfigVersion <= 0 {
		return SourceCircuit{}, ErrStale
	}
	var out SourceCircuit
	err := s.tx(ctx, func(t *sql.Tx) error {
		var enabled int
		var currentVersion int64
		var adapter string
		err := t.QueryRowContext(ctx, `
			SELECT enabled, config_version, adapter_type FROM sources WHERE source_id = ?`,
			sourceID).Scan(&enabled, &currentVersion, &adapter)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrStale
		}
		if err != nil {
			return err
		}
		if enabled != 1 {
			return ErrNotEligible
		}
		if currentVersion != sourceConfigVersion {
			return ErrStale
		}
		// Independent connections in a downloaded list are not a shared
		// upstream failure domain. Three dead rows must not hide hundreds of
		// untested rows or fresh PASS evidence from the same list. Download
		// errors have their own persistent source_fetch_state backoff.
		if adapter == "site_fetch" || adapter == "static_url" || adapter == "account_list" || adapter == "provider_api" {
			if outcome != "PASS" && outcome != "FAIL" {
				return fmt.Errorf("unsupported source outcome %q", outcome)
			}
			_, err := t.ExecContext(ctx, `DELETE FROM source_circuits WHERE source_id = ?`, sourceID)
			out = SourceCircuit{SourceID: sourceID, State: CircuitClosed, LastOutcome: "UNKNOWN"}
			return err
		}
		out, err = recordSourceOutcomeTx(ctx, t, sourceID, outcome, now, p)
		return err
	})
	return out, mapErr(err)
}

func recordSourceOutcomeTx(ctx context.Context, t *sql.Tx, sourceID, outcome string,
	now time.Time, p SourceCircuitPolicy) (SourceCircuit, error) {
	cur, err := scanSourceCircuit(t.QueryRowContext(ctx, `
		SELECT source_id, state, consecutive_failures, open_count, cooldown_until,
		       last_outcome, last_success_at, last_failure_at, updated_at
		FROM source_circuits WHERE source_id = ?`, sourceID))
	if errors.Is(err, sql.ErrNoRows) {
		cur = SourceCircuit{SourceID: sourceID, State: CircuitClosed, LastOutcome: "UNKNOWN"}
	} else if err != nil {
		return SourceCircuit{}, err
	}
	cur.LastOutcome = outcome
	cur.UpdatedAt = now
	switch outcome {
	case "PASS":
		cur.State = CircuitClosed
		cur.ConsecutiveFailures = 0
		cur.OpenCount = 0
		cur.CooldownUntil = nil
		cur.LastSuccessAt = &now
	case "FAIL":
		cur.ConsecutiveFailures++
		cur.LastFailureAt = &now
		trialExpired := cur.State == CircuitOpen &&
			(cur.CooldownUntil == nil || !cur.CooldownUntil.After(now))
		switch {
		case trialExpired:
			cur.OpenCount++
			until := now.Add(policies.SourceCooldown(cur.OpenCount, p.Base, p.Max))
			cur.CooldownUntil = &until
		case cur.State != CircuitOpen && cur.ConsecutiveFailures >= p.FailureThreshold:
			cur.State = CircuitOpen
			cur.OpenCount = 1
			until := now.Add(policies.SourceCooldown(1, p.Base, p.Max))
			cur.CooldownUntil = &until
		}
	default:
		return SourceCircuit{}, fmt.Errorf("unsupported source outcome %q", outcome)
	}
	_, err = t.ExecContext(ctx, `
		INSERT INTO source_circuits(source_id, state, consecutive_failures, open_count,
		  cooldown_until, last_outcome, last_success_at, last_failure_at, updated_at)
		VALUES(?,?,?,?,?,?,?,?,?)
		ON CONFLICT(source_id) DO UPDATE SET
		  state = excluded.state,
		  consecutive_failures = excluded.consecutive_failures,
		  open_count = excluded.open_count,
		  cooldown_until = excluded.cooldown_until,
		  last_outcome = excluded.last_outcome,
		  last_success_at = excluded.last_success_at,
		  last_failure_at = excluded.last_failure_at,
		  updated_at = excluded.updated_at`,
		cur.SourceID, cur.State, cur.ConsecutiveFailures, cur.OpenCount,
		nullTime(cur.CooldownUntil), cur.LastOutcome, nullTime(cur.LastSuccessAt),
		nullTime(cur.LastFailureAt), fmtTime(cur.UpdatedAt))
	if err != nil {
		return SourceCircuit{}, err
	}
	return cur, nil
}

func scanSourceCircuit(row interface {
	Scan(dest ...any) error
}) (SourceCircuit, error) {
	var c SourceCircuit
	var cooldown, success, failure, updated any
	if err := row.Scan(&c.SourceID, &c.State, &c.ConsecutiveFailures, &c.OpenCount,
		&cooldown, &c.LastOutcome, &success, &failure, &updated); err != nil {
		return SourceCircuit{}, err
	}
	var err error
	if c.CooldownUntil, err = scanTime(cooldown); err != nil {
		return SourceCircuit{}, err
	}
	if c.LastSuccessAt, err = scanTime(success); err != nil {
		return SourceCircuit{}, err
	}
	if c.LastFailureAt, err = scanTime(failure); err != nil {
		return SourceCircuit{}, err
	}
	if c.UpdatedAt, err = scanTimeValue(updated); err != nil {
		return SourceCircuit{}, err
	}
	return c, nil
}

func scanTimeValue(v any) (time.Time, error) {
	t, err := scanTime(v)
	if err != nil || t == nil {
		return time.Time{}, err
	}
	return *t, nil
}

// CountCoolingSources reports enabled sources with an unexpired circuit
// cooldown and at least one enabled offer, so acquire can explain an empty
// candidate page as "all sources cooling" rather than "no inventory".
func (s *Store) CountCoolingSources(ctx context.Context, now time.Time) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM sources src
		JOIN source_circuits sc ON sc.source_id = src.source_id
		WHERE src.enabled = 1
		  AND sc.state = 'OPEN'
		  AND sc.cooldown_until IS NOT NULL
		  AND sc.cooldown_until > ?
		  AND EXISTS (SELECT 1 FROM source_offers o
		              WHERE o.source_id = src.source_id AND o.state = 'ENABLED')`,
		fmtTime(now)).Scan(&n)
	return n, mapErr(err)
}

// ProviderSourcesForRefill lists enabled provider channels whose circuit is
// not cooling, optionally restricted to the request's source filter. Demand
// refill uses this when acquire finds no eligible candidates.
func (s *Store) ProviderSourcesForRefill(ctx context.Context, now time.Time,
	allowed []string, limit int, projects ...string) ([]string, error) {
	if limit <= 0 {
		limit = 1
	}
	query := `
		SELECT src.source_id FROM sources src
		LEFT JOIN source_circuits sc ON sc.source_id = src.source_id
		WHERE src.enabled = 1 AND src.adapter_type = 'provider_api'
		  AND (sc.cooldown_until IS NULL OR sc.cooldown_until <= ?)`
	args := []any{fmtTime(now)}
	if len(projects) > 0 && projects[0] != "" {
		query += sourceProjectAccessClause
		args = append(args, projects[0])
	}
	switch {
	case allowed != nil && len(allowed) == 0:
		// An empty non-nil allowlist authorizes nothing; nil is unrestricted.
		query += " AND 1 = 0"
	case len(allowed) > 0:
		query += " AND src.source_id IN (" + placeholders(len(allowed)) + ")"
		for _, id := range allowed {
			args = append(args, id)
		}
	}
	query += " ORDER BY random() LIMIT ?"
	args = append(args, limit)
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

// StaticSourcesForRefill lists enabled static-list channels whose circuit is
// not cooling, so acquire can supplement the pool from the bounded static
// cache after the on-demand refill. Order is randomized to spread load.
func (s *Store) StaticSourcesForRefill(ctx context.Context, now time.Time,
	allowed []string, limit int, projects ...string) ([]string, error) {
	if limit <= 0 {
		limit = 2
	}
	query := `
		SELECT src.source_id FROM sources src
		LEFT JOIN source_circuits sc ON sc.source_id = src.source_id
		WHERE src.enabled = 1 AND src.adapter_type IN ('site_fetch','static_url','account_list','provider_api')
		  AND (sc.cooldown_until IS NULL OR sc.cooldown_until <= ?)`
	args := []any{fmtTime(now)}
	if len(projects) > 0 && projects[0] != "" {
		query += sourceProjectAccessClause
		args = append(args, projects[0])
	}
	if allowed != nil && len(allowed) == 0 {
		return []string{}, nil
	}
	if len(allowed) > 0 {
		query += " AND src.source_id IN (" + placeholders(len(allowed)) + ")"
		for _, id := range allowed {
			args = append(args, id)
		}
	}
	query += " ORDER BY random() LIMIT ?"
	args = append(args, limit)
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

func (s *Store) UpsertObservation(ctx context.Context, resourceID string, obs domain.Observation) error {
	if obs.ExitIP == "" {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO resource_observations(resource_id, exit_ip, country_code, country,
		  region, city, isp, observed_at)
		VALUES(?,?,?,?,?,?,?,?)
		ON CONFLICT(resource_id) DO UPDATE SET
		  exit_ip = excluded.exit_ip,
		  country_code = CASE
		    WHEN excluded.exit_ip <> resource_observations.exit_ip THEN excluded.country_code
		    WHEN excluded.country_code <> '' THEN excluded.country_code
		    ELSE resource_observations.country_code END,
		  country = CASE
		    WHEN excluded.exit_ip <> resource_observations.exit_ip THEN excluded.country
		    WHEN excluded.country <> '' THEN excluded.country
		    ELSE resource_observations.country END,
		  region = CASE
		    WHEN excluded.exit_ip <> resource_observations.exit_ip THEN excluded.region
		    WHEN excluded.region <> '' THEN excluded.region
		    ELSE resource_observations.region END,
		  city = CASE
		    WHEN excluded.exit_ip <> resource_observations.exit_ip THEN excluded.city
		    WHEN excluded.city <> '' THEN excluded.city
		    ELSE resource_observations.city END,
		  isp = CASE
		    WHEN excluded.exit_ip <> resource_observations.exit_ip THEN excluded.isp
		    WHEN excluded.isp <> '' THEN excluded.isp
		    ELSE resource_observations.isp END,
		  observed_at = excluded.observed_at`,
		resourceID, obs.ExitIP, obs.CountryCode, obs.Country, obs.Region,
		obs.City, obs.ISP, fmtTime(obs.ObservedAt))
	return mapErr(err)
}
