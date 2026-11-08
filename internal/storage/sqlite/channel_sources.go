package sqlite

import (
	"context"
	"database/sql"
	"time"

	"proxypool/internal/domain"
)

// ApplyChannelSources commits a channel switch and any missing protocol
// definitions together. The snapshot guards membership as well as versions,
// so another admin cannot add or move a source halfway through the switch.
func (s *Store) ApplyChannelSources(ctx context.Context, snapshot, changes []domain.Source, now time.Time) error {
	return s.tx(ctx, func(t *sql.Tx) error {
		rows, err := t.QueryContext(ctx, `SELECT source_id, config_version FROM sources ORDER BY source_id`)
		if err != nil {
			return err
		}
		expected := make(map[string]int64, len(snapshot))
		for _, src := range snapshot {
			expected[src.ID] = src.ConfigVersion
		}
		matches := true
		for rows.Next() {
			var id string
			var version int64
			if err := rows.Scan(&id, &version); err != nil {
				rows.Close()
				return err
			}
			if expected[id] != version {
				matches = false
			}
			delete(expected, id)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if !matches || len(expected) != 0 {
			return domain.E("CHANNEL_VERSION_CONFLICT", 409, "channel configuration changed; reload and retry")
		}
		if err := ensureSupplySchemaTx(ctx, t); err != nil {
			return err
		}
		for _, src := range changes {
			if src.ConfigVersion == 0 {
				version, err := nextSourceVersionTx(ctx, t, src.ID)
				if err != nil {
					return err
				}
				_, err = t.ExecContext(ctx, `INSERT INTO sources
				 (source_id, tenant, adapter_type, parser_type, enabled, config_version, config_json, created_at, updated_at)
				 VALUES(?,?,?,?,?,?,?,?,?)`, src.ID, src.Tenant, src.AdapterType, src.ParserType,
					boolInt(src.Enabled), version, src.ConfigJSON, fmtTime(now), fmtTime(now))
				if err != nil {
					return err
				}
			} else {
				_, err := t.ExecContext(ctx, `UPDATE sources SET enabled=?, config_version=config_version+1,
				 updated_at=? WHERE source_id=? AND config_version=?`, boolInt(src.Enabled), fmtTime(now), src.ID, src.ConfigVersion)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}
