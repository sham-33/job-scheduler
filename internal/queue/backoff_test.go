package queue

import (
	"math"
	"testing"
	"time"
)

func TestRetryDelayDoublesUpToLimit(t *testing.T) {
	const base, limit = 100 * time.Millisecond, time.Second
	// The un-jittered wait for each attempt; the result is in its upper half.
	wants := []time.Duration{100, 200, 400, 800, 1000, 1000}
	for i, want := range wants {
		want *= time.Millisecond
		for range 200 {
			got := retryDelay(base, limit, i+1)
			if got < want/2 || got > want {
				t.Fatalf("retryDelay(attempt %d) = %s, want between %s and %s", i+1, got, want/2, want)
			}
		}
	}
}

func TestRetryDelayJitters(t *testing.T) {
	seen := map[time.Duration]bool{}
	for range 50 {
		seen[retryDelay(time.Second, time.Minute, 3)] = true
	}
	if len(seen) < 2 {
		t.Error("50 delays were all identical, want jitter")
	}
}

func TestRetryDelayDefaultsAndExtremes(t *testing.T) {
	if got := retryDelay(0, 0, 1); got < DefaultBackoffBase/2 || got > DefaultBackoffBase {
		t.Errorf("retryDelay with no settings = %s, want about %s", got, DefaultBackoffBase)
	}
	// A huge attempt count or limit must not overflow into a negative wait.
	for _, limit := range []time.Duration{time.Minute, math.MaxInt64} {
		if got := retryDelay(time.Second, limit, 1000); got < limit/2 || got > limit {
			t.Errorf("retryDelay(attempt 1000, limit %s) = %s, want between %s and %s", limit, got, limit/2, limit)
		}
	}
	if got := retryDelay(time.Second, time.Minute, 0); got < 500*time.Millisecond || got > time.Second {
		t.Errorf("retryDelay(attempt 0) = %s, want about 1s", got)
	}
}
