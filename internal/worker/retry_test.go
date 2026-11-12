package worker_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/sham-33/job-scheduler/internal/job"
	"github.com/sham-33/job-scheduler/internal/queue"
	"github.com/sham-33/job-scheduler/internal/store"
	"github.com/sham-33/job-scheduler/internal/worker"
)

func stat(t *testing.T, rdb *redis.Client, keys store.Keys, name string) int {
	t.Helper()
	n, err := rdb.HGet(context.Background(), keys.Stats(), name).Int()
	if err != nil && !errors.Is(err, redis.Nil) {
		t.Fatalf("HGET stats %s: %v", name, err)
	}
	return n
}

func mustGet(t *testing.T, q *queue.Queue, id string) *job.Job {
	t.Helper()
	j, err := q.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get(%s): %v", id, err)
	}
	return j
}

func TestPoolRetriesFailedJobWithGrowingDelays(t *testing.T) {
	const base, failures = 100 * time.Millisecond, 3
	q, rdb, keys := newQueue(t)
	runs := make(chan run, failures+1)
	startPool(t, q, 1, worker.Handlers{
		"shaky": func(_ context.Context, j *job.Job) error {
			runs <- run{id: j.ID, at: time.Now()}
			if j.Attempts <= failures {
				return errors.New("not yet")
			}
			return nil
		},
	})
	id := mustEnqueue(t, q, queue.Spec{Type: "shaky", Backoff: queue.Backoff{Base: base, Max: time.Second}})

	prev := waitRun(t, runs, 2*time.Second)
	wait := base // the wait before jitter, which picks from its upper half
	for attempt := 2; attempt <= failures+1; attempt++ {
		next := waitRun(t, runs, 2*time.Second)
		gap := next.at.Sub(prev.at)
		t.Logf("attempt %d started %s after attempt %d (backoff %s to %s)", attempt, gap.Round(time.Millisecond), attempt-1, wait/2, wait)
		// The promoter and an idle slot's poll can add up to about 300ms.
		if gap < wait/2 || gap > wait+500*time.Millisecond {
			t.Errorf("gap before attempt %d = %s, want between %s and %s", attempt, gap, wait/2, wait+500*time.Millisecond)
		}
		prev, wait = next, wait*2
	}

	waitStatus(t, q, id, job.StatusSucceeded)
	if j := mustGet(t, q, id); j.Attempts != failures+1 || j.LastError != "not yet" {
		t.Errorf("job = %+v, want %d attempts with the last failure kept", j, failures+1)
	}
	if got := stat(t, rdb, keys, "retried"); got != failures {
		t.Errorf("stats.retried = %d, want %d", got, failures)
	}
}

func TestPoolMovesJobToDeadLetterQueueThenReplaysIt(t *testing.T) {
	q, rdb, keys := newQueue(t)
	var runs atomic.Int32
	var fixed atomic.Bool
	startPool(t, q, 2, worker.Handlers{
		"broken": func(context.Context, *job.Job) error {
			runs.Add(1)
			if fixed.Load() {
				return nil
			}
			return errors.New("still broken")
		},
	})
	// MaxAttempts is left at its default of 5.
	id := mustEnqueue(t, q, queue.Spec{Type: "broken", Backoff: queue.Backoff{Base: 5 * time.Millisecond, Max: 20 * time.Millisecond}})

	start := time.Now()
	waitStatusWithin(t, q, id, job.StatusDead, 5*time.Second)
	t.Logf("job reached the dead-letter queue after %d attempts in %s", runs.Load(), time.Since(start).Round(time.Millisecond))

	// Give a wrongly scheduled extra attempt time to show up.
	time.Sleep(300 * time.Millisecond)
	if got := runs.Load(); got != queue.DefaultMaxAttempts {
		t.Errorf("handler ran %d times, want %d", got, queue.DefaultMaxAttempts)
	}
	j := mustGet(t, q, id)
	if j.Attempts != queue.DefaultMaxAttempts || j.LastError != "still broken" {
		t.Errorf("job = %+v, want %d attempts and last error 'still broken'", j, queue.DefaultMaxAttempts)
	}
	if err := rdb.ZScore(context.Background(), keys.DLQ(), id).Err(); err != nil {
		t.Errorf("job is not in the dead-letter queue: %v", err)
	}
	if retried, dead := stat(t, rdb, keys, "retried"), stat(t, rdb, keys, "dead"); retried != queue.DefaultMaxAttempts-1 || dead != 1 {
		t.Errorf("stats: retried %d, dead %d, want %d and 1", retried, dead, queue.DefaultMaxAttempts-1)
	}

	// Fix the problem, then replay.
	fixed.Store(true)
	if err := q.Replay(context.Background(), id); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	waitStatus(t, q, id, job.StatusSucceeded)
	if j := mustGet(t, q, id); j.Attempts != 1 {
		t.Errorf("Attempts after replay = %d, want 1", j.Attempts)
	}
	if n, _ := rdb.ZCard(context.Background(), keys.DLQ()).Result(); n != 0 {
		t.Errorf("dead-letter queue holds %d jobs after the replay, want 0", n)
	}
}

func TestPoolPermanentFailureSkipsRetries(t *testing.T) {
	q, _, _ := newQueue(t)
	var runs atomic.Int32
	startPool(t, q, 1, worker.Handlers{
		"hopeless": func(context.Context, *job.Job) error {
			runs.Add(1)
			return job.Permanent(errors.New("bad payload"))
		},
	})
	id := mustEnqueue(t, q, queue.Spec{Type: "hopeless", Backoff: queue.Backoff{Base: 5 * time.Millisecond}})

	waitStatus(t, q, id, job.StatusDead)
	time.Sleep(200 * time.Millisecond)
	if got := runs.Load(); got != 1 {
		t.Errorf("handler ran %d times, want 1", got)
	}
	if j := mustGet(t, q, id); j.Attempts != 1 || j.LastError != "bad payload" {
		t.Errorf("job = %+v, want dead after 1 attempt", j)
	}
}

func TestPoolTimesOutSlowAttempt(t *testing.T) {
	const timeout = 60 * time.Millisecond
	q, _, _ := newQueue(t)
	took := make(chan time.Duration, 1)
	startPool(t, q, 1, worker.Handlers{
		"slow-once": func(ctx context.Context, j *job.Job) error {
			if j.Attempts > 1 {
				return nil
			}
			start := time.Now()
			<-ctx.Done() // would hang forever without the timeout
			took <- time.Since(start)
			return ctx.Err()
		},
	})
	id := mustEnqueue(t, q, queue.Spec{Type: "slow-once", Timeout: timeout, Backoff: queue.Backoff{Base: 10 * time.Millisecond}})

	select {
	case d := <-took:
		t.Logf("first attempt was cancelled after %s (timeout %s)", d.Round(time.Millisecond), timeout)
		if d < timeout || d > timeout+300*time.Millisecond {
			t.Errorf("attempt was cancelled after %s, want about %s", d, timeout)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the slow attempt was never cancelled")
	}
	waitStatus(t, q, id, job.StatusSucceeded)
	if j := mustGet(t, q, id); j.Attempts != 2 || j.LastError != "timed out after 60ms" {
		t.Errorf("job = %+v, want 2 attempts and last error 'timed out after 60ms'", j)
	}
}

func TestPoolStopsCancelledJob(t *testing.T) {
	// The worker learns of the cancel at its next heartbeat, 50ms away at
	// most. The 2s lease is long enough that nothing else could stop the
	// handler within the second this test allows.
	q, rdb, keys := newQueue(t)
	started := make(chan struct{}, 1)
	stopped := make(chan struct{}, 1)
	opts := worker.Options{Concurrency: 1, ShutdownGrace: time.Second, HeartbeatEvery: 50 * time.Millisecond}
	startPoolWith(t, q, opts, worker.Handlers{
		"long": func(ctx context.Context, _ *job.Job) error {
			started <- struct{}{}
			<-ctx.Done()
			stopped <- struct{}{}
			return ctx.Err()
		},
	})
	id := mustEnqueue(t, q, queue.Spec{Type: "long"})
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("job never started")
	}

	begin := time.Now()
	if err := q.Cancel(context.Background(), id); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	select {
	case <-stopped:
		t.Logf("handler stopped %s after the cancel", time.Since(begin).Round(time.Millisecond))
	case <-time.After(time.Second):
		t.Fatal("handler kept running after its job was cancelled")
	}

	// The handler's error must not turn the cancelled job into a retry.
	time.Sleep(300 * time.Millisecond)
	if j := mustGet(t, q, id); j.Status != job.StatusCancelled || j.Attempts != 1 || strings.Contains(j.LastError, "canceled") {
		t.Errorf("job = %+v, want cancelled after 1 attempt with no failure recorded", j)
	}
	if retried, fenced := stat(t, rdb, keys, "retried"), stat(t, rdb, keys, "fenced"); retried != 0 || fenced != 0 {
		t.Errorf("stats: retried %d, fenced %d, want 0 and 0", retried, fenced)
	}
}
