package retry

import (
	"math"
	"math/rand"
	"time"
)

// Delay returns the wait duration before attempt number `attempt` (0-indexed).
// Formula: min(base * multiplier^attempt, maxDelay) ± jitter
func (p Policy) Delay(attempt int) time.Duration {
	raw := float64(p.BaseDelay) * math.Pow(p.Multiplier, float64(attempt))
	if raw > float64(p.MaxDelay) {
		raw = float64(p.MaxDelay)
	}

	// Jitter spreads retries so all consumers don't hammer the same resource
	// at the same instant (thundering herd prevention).
	jitter := raw * p.JitterPct * (2*rand.Float64() - 1)
	d := time.Duration(raw + jitter)
	if d < 0 {
		d = 0
	}
	return d
}

// NextRetryAt returns the absolute time when a retry should next be attempted.
func (p Policy) NextRetryAt(attempt int) time.Time {
	return time.Now().Add(p.Delay(attempt))
}
