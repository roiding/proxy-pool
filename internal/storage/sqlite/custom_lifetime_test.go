package sqlite

import (
	"encoding/json"
	"testing"
	"time"
)

func TestCustomLifetimeNormalizationPreservesRowsAndExpiryMetadata(t *testing.T) {
	s, ctx := newSchedulingStore(t)
	now := time.Now().UTC()
	seedAccessSource(t, s, "upload", "custom", `{}`, 1, now)
	if _, err := s.db.ExecContext(ctx, `UPDATE source_offers SET
	  provider_expires_at = ?, local_retention_deadline = ? WHERE source_id = 'upload'`,
		fmtTime(now.Add(-time.Hour)), fmtTime(now.Add(-time.Minute))); err != nil {
		t.Fatal(err)
	}
	if err := s.KeepCustomSupply(ctx); err != nil {
		t.Fatal(err)
	}
	var exp, retention *string
	var claims string
	if err := s.db.QueryRowContext(ctx, `SELECT provider_expires_at, local_retention_deadline,
	  claims_json FROM source_offers WHERE source_id = 'upload'`).Scan(&exp, &retention, &claims); err != nil {
		t.Fatal(err)
	}
	var parsed map[string]string
	if json.Unmarshal([]byte(claims), &parsed) != nil || exp != nil || retention != nil || parsed["uploaded_expires_at"] == "" {
		t.Fatal("upload lifetime normalized by losing metadata or retaining an expiry")
	}
	if err := s.KeepCustomSupply(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := s.SelectCandidates(ctx, SelectionFilters{Now: now, Profile: "scope", Limit: 10})
	if err != nil || len(rows) != 1 {
		t.Fatal("normalization deleted or disabled an uploaded connection")
	}
}
