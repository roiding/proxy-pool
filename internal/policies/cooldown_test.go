package policies

import (
	"testing"
	"time"
)

func TestNextCooldownBackoffAndCap(t *testing.T) {
	base := 30 * time.Second
	max := 15 * time.Minute
	first := NextCooldown(1, base, max, 0)
	if first < base {
		t.Fatalf("first cooldown %s below base", first)
	}
	fifth := NextCooldown(5, base, max, 0)
	if fifth != 16*base {
		t.Fatalf("fifth cooldown %s, want 16x base", fifth)
	}
	tenth := NextCooldown(10, base, max, 0)
	if tenth != max {
		t.Fatalf("tenth cooldown %s, want capped at %s", tenth, max)
	}
	jittered := NextCooldown(1, base, max, 0.25)
	if jittered < base || jittered > base+base/4 {
		t.Fatalf("jittered cooldown %s outside allowed range", jittered)
	}
}
