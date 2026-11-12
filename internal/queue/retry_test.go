package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sham-33/job-scheduler/internal/job"
)

func mustClaim(t *testing.T, q *Queue, owner string) *job.Job {
	t.Helper()
	j, err := q.Claim(context.Background(), owner)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if j == nil {
		t.Fatal("Claim returned no job, want one")
	}
	return j
}

// claimAgain waits out a retry delay, promotes the job and claims it.
func claimAgain(t *testing.T, q *Queue, wait time.Duration) *job.Job {
	t.Helper()
	time.Sleep(wait + 5*time.Millisecond)
	if _, err := q.Promote(context.Background(), 10); err != nil {
		t.Fatalf("Promote: %v", err)
	}
	return mustClaim(t, q, "w1")
}

func TestEnqueueStoresRetrySettings(t *testing.T) {
	q, _, _ := newTestQueue(t)

	def := mustGet(t, q, mustEnqueue(t, q, Spec{Type: "echo"}))
	if def.MaxAttempts != DefaultMaxAttempts || def.Timeout != 0 ||
		def.BackoffBase != DefaultBackoffBase || def.BackoffMax != DefaultBackoffMax {
		t.Errorf("defaults = %d attempts, timeout %s, backoff %s..%s", def.MaxAttempts, def.Timeout, def.BackoffBase, def.BackoffMax)
	}

	custom := mustGet(t, q, mustEnqueue(t, q, Spec{
		Type: "echo", MaxAttempts: 3, Timeout: 2 * time.Second,
		Backoff: Backoff{Base: 250 * time.Millisecond, Max: 4 * time.Second},
	}))
	if custom.MaxAttempts != 3 || custom.Timeout != 2*time.Second ||
		custom.BackoffBase != 250*time.Millisecond || custom.BackoffMax != 4*time.Second {
		t.Errorf("custom = %d attempts, timeout %s, backoff %s..%s", custom.MaxAttempts, custom.Timeout, custom.BackoffBase, custom.BackoffMax)
	}

	// A base above the default cap raises the cap with it.
	big := mustGet(t, q, mustEnqueue(t, q, Spec{Type: "echo", Backoff: Backoff{Base: 5 * time.Minute}}))
	if big.BackoffMax != 5*time.Minute {
		t.Errorf("BackoffMax = %s, want 5m to match the base", big.BackoffMax)
	}
}

func TestEnqueueRejectsBadRetrySettings(t *testing.T) {
	q, _, _ := newTestQueue(t)
	for name, s := range map[string]Spec{
		"negative attempts": {Type: "echo", MaxAttempts: -1},
		"negative timeout":  {Type: "echo", Timeout: -time.Second},
		"negative backoff":  {Type: "echo", Backoff: Backoff{Base: -time.Second}},
		"max below base":    {Type: "echo", Backoff: Backoff{Base: time.Minute, Max: time.Second}},
	} {
		if _, _, err := q.Enqueue(context.Background(), s); err == nil {
			t.Errorf("%s: Enqueue succeeded, want an error", name)
		}
	}
}

func TestFailSchedulesRetry(t *testing.T) {
	q, rdb, keys := newTestQueue(t)
	ctx := context.Background()
	id := mustEnqueue(t, q, Spec{Type: "echo", Backoff: Backoff{Base: 200 * time.Millisecond}})
	claimed := mustClaim(t, q, "w1")

	retryIn, dead, err := q.Fail(ctx, claimed, errors.New("boom"))
	if err != nil || dead {
		t.Fatalf("Fail = (%s, dead %v, %v), want a retry", retryIn, dead, err)
	}
	if retryIn < 100*time.Millisecond || retryIn > 200*time.Millisecond {
		t.Errorf("retryIn = %s, want between 100ms and 200ms for a first failure", retryIn)
	}

	j := mustGet(t, q, id)
	if j.Status != job.StatusScheduled || j.LastError != "boom" || j.Attempts != 1 {
		t.Errorf("job = %+v, want scheduled after 1 attempt with last error boom", j)
	}
	if got := j.RunAt.Sub(j.UpdatedAt); got != retryIn {
		t.Errorf("RunAt is %s after the failure, want %s", got, retryIn)
	}
	if score, ok := inZSet(t, rdb, keys.Scheduled(), id); !ok || int64(score) != j.RunAt.UnixMilli() {
		t.Errorf("scheduled set: present %v, score %v, want RunAt %d", ok, score, j.RunAt.UnixMilli())
	}
	if _, ok := inZSet(t, rdb, keys.Leases(), id); ok {
		t.Error("failed job still has a lease")
	}
	if got := stat(t, rdb, keys, "retried"); got != 1 {
		t.Errorf("stats.retried = %d, want 1", got)
	}

	// Not claimable until the wait is over.
	if n, _ := q.Promote(ctx, 10); n != 0 {
		t.Errorf("Promote moved %d jobs before the retry was due", n)
	}
	if again := claimAgain(t, q, retryIn); again.ID != id || again.Attempts != 2 {
		t.Errorf("second claim = %+v, want attempt 2 of the same job", again)
	}
}

func TestFailBuriesJobOutOfAttempts(t *testing.T) {
	q, rdb, keys := newTestQueue(t)
	ctx := context.Background()
	id := mustEnqueue(t, q, Spec{Type: "echo", MaxAttempts: 2, Backoff: Backoff{Base: 10 * time.Millisecond}})

	retryIn, dead, err := q.Fail(ctx, mustClaim(t, q, "w1"), errors.New("first"))
	if err != nil || dead {
		t.Fatalf("first Fail = (dead %v, %v), want a retry", dead, err)
	}
	_, dead, err = q.Fail(ctx, claimAgain(t, q, retryIn), errors.New("second"))
	if err != nil || !dead {
		t.Fatalf("second Fail = (dead %v, %v), want dead", dead, err)
	}

	j := mustGet(t, q, id)
	if j.Status != job.StatusDead || j.LastError != "second" || j.Attempts != 2 || j.FinishedAt.IsZero() {
		t.Errorf("job = %+v, want dead after 2 attempts with a finish time", j)
	}
	if _, ok := inZSet(t, rdb, keys.DLQ(), id); !ok {
		t.Error("dead job is not in the dead-letter queue")
	}
	for name, key := range map[string]string{"scheduled": keys.Scheduled(), "ready": keys.Ready(), "leases": keys.Leases()} {
		if _, ok := inZSet(t, rdb, key, id); ok {
			t.Errorf("dead job is still in the %s set", name)
		}
	}
	if got := stat(t, rdb, keys, "dead"); got != 1 {
		t.Errorf("stats.dead = %d, want 1", got)
	}
}

func TestFailPermanentSkipsRetries(t *testing.T) {
	q, rdb, keys := newTestQueue(t)
	ctx := context.Background()
	for _, cause := range []error{
		job.Permanent(errors.New("bad payload")),
		fmt.Errorf("handler: %w", job.Permanent(errors.New("bad payload"))),
	} {
		id := mustEnqueue(t, q, Spec{Type: "echo"})
		_, dead, err := q.Fail(ctx, mustClaim(t, q, "w1"), cause)
		if err != nil || !dead {
			t.Fatalf("Fail(%v) = (dead %v, %v), want dead on the first attempt", cause, dead, err)
		}
		if j := mustGet(t, q, id); j.Status != job.StatusDead || j.Attempts != 1 || j.LastError != cause.Error() {
			t.Errorf("job = %+v, want dead after 1 attempt", j)
		}
	}
	if got := stat(t, rdb, keys, "retried"); got != 0 {
		t.Errorf("stats.retried = %d, want 0", got)
	}
}

func TestFailRejectsStaleToken(t *testing.T) {
	q, rdb, keys := leaseTestQueue(t, 50*time.Millisecond)
	ctx := context.Background()
	id := mustEnqueue(t, q, Spec{Type: "echo"})
	stale := mustClaim(t, q, "w1")

	time.Sleep(70 * time.Millisecond)
	if n, err := q.Reap(ctx, 10); err != nil || n != 1 {
		t.Fatalf("Reap = %d, %v, want 1", n, err)
	}
	current := mustClaim(t, q, "w2")

	if _, _, err := q.Fail(ctx, stale, errors.New("late failure")); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Fail with a stale token = %v, want ErrLeaseLost", err)
	}
	j := mustGet(t, q, id)
	if j.Status != job.StatusRunning || j.LeaseToken != current.LeaseToken || j.LastError == "late failure" {
		t.Errorf("job = %+v, want it untouched and still running for w2", j)
	}
	if got := stat(t, rdb, keys, "fenced"); got != 1 {
		t.Errorf("stats.fenced = %d, want 1", got)
	}
}

func TestFailTruncatesLongError(t *testing.T) {
	q, _, _ := newTestQueue(t)
	id := mustEnqueue(t, q, Spec{Type: "echo"})
	if _, _, err := q.Fail(context.Background(), mustClaim(t, q, "w1"), errors.New(strings.Repeat("x", 5000))); err != nil {
		t.Fatalf("Fail: %v", err)
	}
	if got := len(mustGet(t, q, id).LastError); got != maxErrorLen {
		t.Errorf("stored error is %d bytes, want %d", got, maxErrorLen)
	}
}

func TestReapBuriesJobThatKeepsLosingItsWorker(t *testing.T) {
	q, rdb, keys := leaseTestQueue(t, 50*time.Millisecond)
	ctx := context.Background()
	id := mustEnqueue(t, q, Spec{Type: "echo", MaxAttempts: 2})

	// First crash: the job goes straight back to ready.
	mustClaim(t, q, "w1")
	time.Sleep(70 * time.Millisecond)
	if n, err := q.Reap(ctx, 10); err != nil || n != 1 {
		t.Fatalf("first Reap = %d, %v, want 1", n, err)
	}
	if j := mustGet(t, q, id); j.Status != job.StatusReady {
		t.Fatalf("after the first crash status = %s, want ready", j.Status)
	}

	// Second crash uses up the last attempt.
	mustClaim(t, q, "w2")
	time.Sleep(70 * time.Millisecond)
	if n, err := q.Reap(ctx, 10); err != nil || n != 1 {
		t.Fatalf("second Reap = %d, %v, want 1", n, err)
	}
	j := mustGet(t, q, id)
	if j.Status != job.StatusDead || j.LastError != "lease expired" || j.Attempts != 2 {
		t.Errorf("job = %+v, want dead after 2 attempts with last error 'lease expired'", j)
	}
	if _, ok := inZSet(t, rdb, keys.DLQ(), id); !ok {
		t.Error("job is not in the dead-letter queue")
	}
	if _, ok := inZSet(t, rdb, keys.Ready(), id); ok {
		t.Error("dead job is in the ready set")
	}
	if reclaimed, dead := stat(t, rdb, keys, "reclaimed"), stat(t, rdb, keys, "dead"); reclaimed != 1 || dead != 1 {
		t.Errorf("stats: reclaimed %d, dead %d, want 1 and 1", reclaimed, dead)
	}
}

func TestCancelWaitingJob(t *testing.T) {
	for name, delay := range map[string]time.Duration{"ready": 0, "scheduled": 30 * time.Millisecond} {
		t.Run(name, func(t *testing.T) {
			q, rdb, keys := newTestQueue(t)
			ctx := context.Background()
			id := mustEnqueue(t, q, Spec{Type: "echo", Delay: delay})

			if err := q.Cancel(ctx, id); err != nil {
				t.Fatalf("Cancel: %v", err)
			}
			if j := mustGet(t, q, id); j.Status != job.StatusCancelled || j.FinishedAt.IsZero() {
				t.Errorf("job = %+v, want cancelled with a finish time", j)
			}
			for set, key := range map[string]string{"scheduled": keys.Scheduled(), "ready": keys.Ready()} {
				if _, ok := inZSet(t, rdb, key, id); ok {
					t.Errorf("cancelled job is still in the %s set", set)
				}
			}

			// It never becomes claimable, even once it would have been due.
			time.Sleep(delay + 10*time.Millisecond)
			if n, err := q.Promote(ctx, 10); err != nil || n != 0 {
				t.Errorf("Promote = %d, %v, want 0", n, err)
			}
			if j, err := q.Claim(ctx, "w1"); err != nil || j != nil {
				t.Errorf("Claim = %+v, %v, want nothing", j, err)
			}
			if got := stat(t, rdb, keys, "cancelled"); got != 1 {
				t.Errorf("stats.cancelled = %d, want 1", got)
			}
			if err := q.Cancel(ctx, id); !errors.Is(err, ErrFinished) {
				t.Errorf("second Cancel = %v, want ErrFinished", err)
			}
		})
	}
}

func TestCancelRunningJobStopsItsWorker(t *testing.T) {
	q, rdb, keys := newTestQueue(t)
	ctx := context.Background()
	id := mustEnqueue(t, q, Spec{Type: "echo"})
	claimed := mustClaim(t, q, "w1")

	if err := q.Cancel(ctx, id); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if _, ok := inZSet(t, rdb, keys.Leases(), id); ok {
		t.Error("cancelled job still has a lease")
	}

	// Everything the worker tries next tells it the job was cancelled.
	if err := q.Heartbeat(ctx, claimed); !errors.Is(err, ErrCancelled) {
		t.Errorf("Heartbeat = %v, want ErrCancelled", err)
	}
	if err := q.Complete(ctx, claimed); !errors.Is(err, ErrCancelled) {
		t.Errorf("Complete = %v, want ErrCancelled", err)
	}
	if _, _, err := q.Fail(ctx, claimed, errors.New("boom")); !errors.Is(err, ErrCancelled) {
		t.Errorf("Fail = %v, want ErrCancelled", err)
	}
	if j := mustGet(t, q, id); j.Status != job.StatusCancelled {
		t.Errorf("Status = %s, want it to stay cancelled", j.Status)
	}
	// A cancelled job is not a fencing event.
	if got := stat(t, rdb, keys, "fenced"); got != 0 {
		t.Errorf("stats.fenced = %d, want 0", got)
	}
	// The reaper has nothing to take back.
	if n, err := q.Reap(ctx, 10); err != nil || n != 0 {
		t.Errorf("Reap = %d, %v, want 0", n, err)
	}
}

func TestCancelErrors(t *testing.T) {
	q, _, _ := newTestQueue(t)
	ctx := context.Background()

	if err := q.Cancel(ctx, "no-such-job"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Cancel(unknown) = %v, want ErrNotFound", err)
	}

	id := mustEnqueue(t, q, Spec{Type: "echo"})
	if err := q.Complete(ctx, mustClaim(t, q, "w1")); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if err := q.Cancel(ctx, id); !errors.Is(err, ErrFinished) {
		t.Errorf("Cancel(succeeded) = %v, want ErrFinished", err)
	}
	if j := mustGet(t, q, id); j.Status != job.StatusSucceeded {
		t.Errorf("Status = %s, want it to stay succeeded", j.Status)
	}
}

func TestReplay(t *testing.T) {
	q, rdb, keys := newTestQueue(t)
	ctx := context.Background()
	id := mustEnqueue(t, q, Spec{Type: "echo", MaxAttempts: 1})

	if err := q.Replay(ctx, id); !errors.Is(err, ErrNotDead) {
		t.Errorf("Replay(ready job) = %v, want ErrNotDead", err)
	}
	if err := q.Replay(ctx, "no-such-job"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Replay(unknown) = %v, want ErrNotFound", err)
	}

	if _, dead, err := q.Fail(ctx, mustClaim(t, q, "w1"), errors.New("boom")); err != nil || !dead {
		t.Fatalf("Fail = (dead %v, %v), want dead", dead, err)
	}
	if err := q.Replay(ctx, id); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	j := mustGet(t, q, id)
	if j.Status != job.StatusReady || j.Attempts != 0 || !j.FinishedAt.IsZero() || j.LastError != "boom" {
		t.Errorf("job = %+v, want ready with 0 attempts, no finish time, and its last error kept", j)
	}
	if _, ok := inZSet(t, rdb, keys.DLQ(), id); ok {
		t.Error("replayed job is still in the dead-letter queue")
	}
	if got := stat(t, rdb, keys, "replayed"); got != 1 {
		t.Errorf("stats.replayed = %d, want 1", got)
	}

	// It runs like a new job.
	again := mustClaim(t, q, "w1")
	if again.ID != id || again.Attempts != 1 {
		t.Fatalf("claim after replay = %+v, want attempt 1 of the same job", again)
	}
	if err := q.Complete(ctx, again); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if err := q.Replay(ctx, id); !errors.Is(err, ErrNotDead) {
		t.Errorf("Replay(succeeded job) = %v, want ErrNotDead", err)
	}
}
