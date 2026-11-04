package sqlite

import (
	"context"
	"encoding/json"
	"time"

	"proxypool/internal/domain"
)

// ListBindings returns sticky bindings for the admin console. The owner and
// ciphertext columns are deliberately excluded: the console needs identity
// and lifecycle state, never sealed connection material.
func (s *Store) ListBindings(ctx context.Context, projectID string, limit int) ([]domain.StickyBinding, error) {
	if limit <= 0 {
		limit = 50
	}
	query := `
		SELECT project_id, session_id, version, fingerprint, source_id,
		       source_config_version, resource_id, incarnation, validation_json, deadline,
		       state, created_at, updated_at
		FROM sticky_bindings`
	args := []any{}
	if projectID != "" {
		query += ` WHERE project_id = ?`
		args = append(args, projectID)
	}
	query += ` ORDER BY updated_at DESC, project_id, session_id LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	out := []domain.StickyBinding{}
	for rows.Next() {
		var b domain.StickyBinding
		var validation, deadline, created, updated string
		if err := rows.Scan(&b.ProjectID, &b.SessionID, &b.Version, &b.Fingerprint,
			&b.SourceID, &b.SourceConfigVersion, &b.ResourceID, &b.Incarnation,
			&validation, &deadline, &b.State, &created, &updated); err != nil {
			return nil, mapErr(err)
		}
		if err := json.Unmarshal([]byte(validation), &b.Validation); err != nil {
			return nil, err
		}
		var perr error
		if b.Deadline, perr = time.Parse(time.RFC3339Nano, deadline); perr != nil {
			return nil, perr
		}
		if b.CreatedAt, perr = time.Parse(time.RFC3339Nano, created); perr != nil {
			return nil, perr
		}
		if b.UpdatedAt, perr = time.Parse(time.RFC3339Nano, updated); perr != nil {
			return nil, perr
		}
		out = append(out, b)
	}
	return out, mapErr(rows.Err())
}
