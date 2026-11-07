package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"proxypool/internal/domain"
)

// UpdateProject applies a configuration change guarded by the expected
// config_version inside one transaction, bumping the version by one. The
// read-modify-write race in callers cannot overwrite a concurrent change.
func (s *Store) UpdateProject(ctx context.Context, p domain.Project,
	expectedVersion int64, now time.Time) (domain.Project, error) {
	var version int64
	err := s.tx(ctx, func(t *sql.Tx) error {
		res, err := t.ExecContext(ctx, `
			UPDATE projects
			SET name = ?, enabled = ?, cooldown_ms = ?, timeout_ms = ?,
			    validation_json = ?, allowed_source_ids = ?, network_reuse_cooldown_ms = ?, auto_prewarm_target = ?,
			    config_version = config_version + 1, updated_at = ?
			WHERE project_id = ? AND config_version = ?`,
			p.Name, boolInt(p.Enabled), p.Defaults.CooldownMS, p.Defaults.TimeoutMS,
			marshalJSONDefault(p.Defaults.Validation), encodeAllowedSources(p.AllowedSourceIDs), normalizedNetworkCooldown(p.Defaults.NetworkReuseCooldownMS),
			p.Defaults.AutoPrewarmTarget,
			fmtTime(now), p.ID, expectedVersion)
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
				`SELECT COUNT(*) FROM projects WHERE project_id = ?`, p.ID).Scan(&exists); err != nil {
				return err
			}
			if exists == 0 {
				return domain.NotFound("project")
			}
			return domain.E("CONFIG_VERSION_CONFLICT", 409,
				"project %q changed since version %d; reload and retry", p.ID, expectedVersion)
		}
		if err := t.QueryRowContext(ctx,
			`SELECT config_version FROM projects WHERE project_id = ?`, p.ID).Scan(&version); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return domain.Project{}, mapErr(err)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Project{}, domain.NotFound("project")
	}
	p.ConfigVersion = version
	return p, nil
}
