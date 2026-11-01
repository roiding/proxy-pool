package policies

import "time"

// SourceCooldown returns the source-circuit backoff after the given number of
// consecutive openings: base doubled per opening, capped at max.
func SourceCooldown(openCount int, base, max time.Duration) time.Duration {
	if openCount < 1 {
		openCount = 1
	}
	d := base
	for i := 1; i < openCount && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	return d
}
