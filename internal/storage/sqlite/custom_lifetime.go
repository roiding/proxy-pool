package sqlite

import "context"

// KeepCustomSupply removes historical clock-based retention of uploaded
// offers. This does not alter identities, credentials, leases or project CD.
// New imports follow the same policy in the application normalization path.
func (s *Store) KeepCustomSupply(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `UPDATE source_offers
	  SET claims_json = CASE WHEN provider_expires_at IS NOT NULL
	    THEN json_set(CASE WHEN json_type(claims_json) = 'object' THEN claims_json ELSE '{}' END,
	      '$.uploaded_expires_at', provider_expires_at) ELSE claims_json END,
	    local_retention_deadline = NULL, provider_expires_at = NULL, expiry_basis = 'unknown'
	  WHERE source_id IN (SELECT source_id FROM sources WHERE adapter_type = 'custom')
	    AND (local_retention_deadline IS NOT NULL OR provider_expires_at IS NOT NULL)`)
	return mapErr(err)
}
