package queue

import (
	"math/rand/v2"
	"time"
)

// retryDelay returns how long a job waits after its attempt-th failure. The
// wait starts at base and doubles with every failure, up to limit, so a
// service that is down gets room to recover instead of being hammered. The
// result is random within the upper half of that wait, so jobs that failed
// together don't all come back at the same instant.
func retryDelay(base, limit time.Duration, attempt int) time.Duration {
	if base <= 0 {
		base = DefaultBackoffBase
	}
	if limit <= 0 {
		limit = max(DefaultBackoffMax, base)
	}
	wait := min(base, limit)
	for range attempt - 1 {
		if wait > limit/2 {
			wait = limit
			break
		}
		wait *= 2
	}
	return wait/2 + rand.N(wait/2+1)
}
