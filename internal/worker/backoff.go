package worker

import (
	"math/rand/v2"
	"time"
)

const (
	idleMin = 10 * time.Millisecond
	idleMax = 200 * time.Millisecond
)

// idleBackoff spaces out claim attempts while the queue is empty. The wait
// starts short, so a job arriving just after the queue drains is picked up
// quickly, and doubles up to idleMax, so idle workers put little load on Redis.
type idleBackoff struct {
	wait time.Duration
}

// next returns how long to sleep before the next attempt. The result is
// random within the upper half of the current wait, so idle slots spread out
// instead of all hitting Redis at the same instant.
func (b *idleBackoff) next() time.Duration {
	b.wait = min(max(b.wait*2, idleMin), idleMax)
	return b.wait/2 + rand.N(b.wait/2)
}

// reset goes back to the shortest wait, after a job was found.
func (b *idleBackoff) reset() {
	b.wait = 0
}
