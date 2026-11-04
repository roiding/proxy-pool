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

// DefaultProjectID hosts deliveries authenticated with the deployment
// master token and any key created before explicit project management lands.
const DefaultProjectID = "default"

func legacyProjectID(keyID string) string {
	return "proj_legacy_" + keyID
}

// encodeAllowedSources keeps nil (unrestricted) and empty (nothing allowed)
// distinguishable in storage: SQL NULL versus the JSON array [].
func encodeAllowedSources(ids []string) any {
	if ids == nil {
		return nil
	}
	return marshalJSONDefault(ids)
}

func decodeAllowedSources(value any) ([]string, error) {
	if value == nil {
		return nil, nil
	}
	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("allowed_source_ids is %T, want string", value)
	}
	var ids []string
	if err := json.Unmarshal([]byte(str), &ids); err != nil {
		return nil, err
	}
	return ids, nil
}

// EnsureProject inserts a project when absent and never overwrites existing
// configuration; it is safe to call on every boot.
func (s *Store) EnsureProject(ctx context.Context, p domain.Project, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO projects(project_id, name, enabled, cooldown_ms, timeout_ms,
		  validation_json, allowed_source_ids, config_version, created_at, updated_at)
		VALUES(?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(project_id) DO NOTHING`,
		p.ID, p.Name, boolInt(p.Enabled), p.Defaults.CooldownMS, p.Defaults.TimeoutMS,
		marshalJSONDefault(p.Defaults.Validation), encodeAllowedSources(p.AllowedSourceIDs),
		p.ConfigVersion, fmtTime(now), fmtTime(now))
	return mapErr(err)
}

// UpsertProject replaces the configuration and bumps config_version so
// in-flight reservations can detect the change.
func (s *Store) UpsertProject(ctx context.Context, p domain.Project, now time.Time) (domain.Project, error) {
	var version int64
	err := s.tx(ctx, func(t *sql.Tx) error {
		err := t.QueryRowContext(ctx,
			`SELECT config_version FROM projects WHERE project_id = ?`, p.ID).Scan(&version)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			version = 0
		case err != nil:
			return err
		}
		version++
		_, err = t.ExecContext(ctx, `
			INSERT INTO projects(project_id, name, enabled, cooldown_ms, timeout_ms,
			  validation_json, allowed_source_ids, config_version, created_at, updated_at)
			VALUES(?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(project_id) DO UPDATE SET
			  name = excluded.name,
			  enabled = excluded.enabled,
			  cooldown_ms = excluded.cooldown_ms,
			  timeout_ms = excluded.timeout_ms,
			  validation_json = excluded.validation_json,
			  allowed_source_ids = excluded.allowed_source_ids,
			  config_version = excluded.config_version,
			  updated_at = excluded.updated_at`,
			p.ID, p.Name, boolInt(p.Enabled), p.Defaults.CooldownMS, p.Defaults.TimeoutMS,
			marshalJSONDefault(p.Defaults.Validation), encodeAllowedSources(p.AllowedSourceIDs),
			version, fmtTime(now), fmtTime(now))
		return err
	})
	if err != nil {
		return domain.Project{}, mapErr(err)
	}
	p.ConfigVersion = version
	return p, nil
}

func scanProject(row interface{ Scan(...any) error }) (domain.Project, error) {
	var p domain.Project
	var enabled int
	var allowed any
	var validation string
	var created, updated string
	err := row.Scan(&p.ID, &p.Name, &enabled, &p.Defaults.CooldownMS, &p.Defaults.TimeoutMS,
		&validation, &allowed, &p.ConfigVersion, &created, &updated)
	if err != nil {
		return domain.Project{}, err
	}
	p.Enabled = enabled == 1
	if err := json.Unmarshal([]byte(validation), &p.Defaults.Validation); err != nil {
		return domain.Project{}, err
	}
	if p.AllowedSourceIDs, err = decodeAllowedSources(allowed); err != nil {
		return domain.Project{}, err
	}
	if p.CreatedAt, err = time.Parse(time.RFC3339Nano, created); err != nil {
		return domain.Project{}, err
	}
	if p.UpdatedAt, err = time.Parse(time.RFC3339Nano, updated); err != nil {
		return domain.Project{}, err
	}
	return p, nil
}

func (s *Store) GetProject(ctx context.Context, id string) (domain.Project, error) {
	p, err := scanProject(s.db.QueryRowContext(ctx, `
		SELECT project_id, name, enabled, cooldown_ms, timeout_ms, validation_json,
		       allowed_source_ids, config_version, created_at, updated_at
		FROM projects WHERE project_id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Project{}, domain.NotFound("project")
	}
	return p, mapErr(err)
}

func (s *Store) ListProjects(ctx context.Context) ([]domain.Project, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT project_id, name, enabled, cooldown_ms, timeout_ms, validation_json,
		       allowed_source_ids, config_version, created_at, updated_at
		FROM projects ORDER BY created_at, project_id`)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []domain.Project
	for rows.Next() {
		p, err := scanProject(rows)
		if err != nil {
			return nil, mapErr(err)
		}
		out = append(out, p)
	}
	return out, mapErr(rows.Err())
}
