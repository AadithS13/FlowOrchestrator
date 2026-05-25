package retry

import "time"

// Policy configures exponential-backoff retry behaviour for a service.
type Policy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Multiplier  float64
	JitterPct   float64 // fraction, e.g. 0.1 = ±10%
}

// DefaultPolicy is used by all services unless overridden.
//
//	Attempt 0 (first):  ~1 s
//	Attempt 1 (retry):  ~2 s
//	Attempt 2 (retry):  ~4 s  → then DLQ
var DefaultPolicy = Policy{
	MaxAttempts: 3,
	BaseDelay:   1 * time.Second,
	MaxDelay:    30 * time.Second,
	Multiplier:  2.0,
	JitterPct:   0.1,
}

func (p Policy) ShouldRetry(attemptCount int) bool {
	return attemptCount < p.MaxAttempts
}
