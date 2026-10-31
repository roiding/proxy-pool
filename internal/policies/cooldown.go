package policies

import (
	"crypto/rand"
	"math/big"
	"time"
)

// NextCooldown returns the backoff for the nth consecutive attributed failure:
// base doubled each failure, capped, plus multiplicative jitter.
func NextCooldown(consecutive int, base, max time.Duration, jitterFraction float64) time.Duration {
	if consecutive < 1 {
		consecutive = 1
	}
	d := base
	for i := 1; i < consecutive && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	if jitterFraction <= 0 || d <= 0 {
		return d
	}
	frac, err := rand.Int(rand.Reader, big.NewInt(1000))
	if err != nil {
		return d
	}
	jitter := time.Duration(float64(d) * jitterFraction * (float64(frac.Int64()) / 1000))
	return d + jitter
}

const EphemeralRetireThreshold = 5
