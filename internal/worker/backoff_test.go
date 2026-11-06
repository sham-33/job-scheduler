package worker

import (
	"testing"
	"time"
)

func TestIdleBackoffGrowsAndCaps(t *testing.T) {
	ceilings := []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		40 * time.Millisecond,
		80 * time.Millisecond,
		160 * time.Millisecond,
		200 * time.Millisecond,
		200 * time.Millisecond,
	}
	// Repeat so the random jitter gets a chance to step out of bounds.
	for range 1000 {
		var b idleBackoff
		for i, ceiling := range ceilings {
			got := b.next()
			if got < ceiling/2 || got >= ceiling {
				t.Fatalf("wait %d = %s, want in [%s, %s)", i, got, ceiling/2, ceiling)
			}
		}
	}
}

func TestIdleBackoffReset(t *testing.T) {
	var b idleBackoff
	for range 10 {
		b.next()
	}
	b.reset()
	if got := b.next(); got >= idleMin {
		t.Errorf("first wait after reset = %s, want under %s", got, idleMin)
	}
}

func TestIdleBackoffJitters(t *testing.T) {
	seen := map[time.Duration]bool{}
	for range 100 {
		var b idleBackoff
		seen[b.next()] = true
	}
	if len(seen) < 2 {
		t.Errorf("100 waits had %d distinct values, want them spread out", len(seen))
	}
}
