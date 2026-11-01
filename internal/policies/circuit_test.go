package policies

import (
	"testing"
	"time"
)

func TestSourceCooldownDoublesAndCaps(t *testing.T) {
	base := 10 * time.Second
	max := 25 * time.Second
	cases := []struct {
		open int
		want time.Duration
	}{
		{0, 10 * time.Second},
		{1, 10 * time.Second},
		{2, 20 * time.Second},
		{3, 25 * time.Second},
		{8, 25 * time.Second},
	}
	for _, tc := range cases {
		if got := SourceCooldown(tc.open, base, max); got != tc.want {
			t.Fatalf("SourceCooldown(%d) = %s, want %s", tc.open, got, tc.want)
		}
	}
}
