package sqlite

import (
	"context"
	"time"

	"proxypool/internal/domain"
)

func (s *Store) EphemeralCandidateAvailable(ctx context.Context, c domain.Candidate, now time.Time) (bool, error) {
	if blocked, err := s.SourceProxyBlocked(ctx, c.Offer.SourceID, c.Resource.ID, now); err != nil {
		return false, err
	} else if blocked {
		return false, nil
	}
	if c.CacheExpiresAt != nil && !c.CacheExpiresAt.After(now) {
		return false, nil
	}
	if c.Offer.ProviderExpiresAt != nil && !c.Offer.ProviderExpiresAt.After(now) {
		return false, nil
	}
	max := c.Offer.MaxPoolLeases
	if max < 1 {
		max = 1
	}
	var occupied int
	err := s.db.QueryRowContext(ctx, `SELECT
 (SELECT COUNT(*) FROM leases WHERE resource_id=? AND state='ACTIVE' AND deadline>?) +
 (SELECT COUNT(*) FROM reservations WHERE resource_id=? AND state IN ('RESERVED','VALIDATING') AND deadline>?)`,
		c.Resource.ID, fmtTime(now), c.Resource.ID, fmtTime(now)).Scan(&occupied)
	return occupied < max, mapErr(err)
}
