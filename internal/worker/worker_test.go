package worker_test

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/sham-33/job-scheduler/internal/job"
	"github.com/sham-33/job-scheduler/internal/queue"
	"github.com/sham-33/job-scheduler/internal/scheduler"
	"github.com/sham-33/job-scheduler/internal/store"
	"github.com/sham-33/job-scheduler/internal/store/storetest"
	"github.com/sham-33/job-scheduler/internal/worker"
)

func newQueue(t *testing.T) (*queue.Queue, *redis.Client, store.Keys) {
	t.Helper()
	rdb := storetest.Client(t)
	keys := storetest.Keys(t, rdb)
	return queue.New(rdb, keys, 2*time.Second), rdb, keys
}

// startPool runs a pool and the promoter until stop is called or the test
// ends. stop waits for every slot to finish.
func startPool(t *testing.T, q *queue.Queue, concurrency int, handlers worker.Handlers) (stop func()) {
	t.Helper()
	logger := slog.New(slog.DiscardHandler)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() { worker.NewPool("test", concurrency, q, handlers, logger).Run(ctx) })
	wg.Go(func() { scheduler.RunPromoter(ctx, q, logger) })
	stop = sync.OnceFunc(func() {
		cancel()
		wg.Wait()
	})
	t.Cleanup(stop)
	return stop
}

func mustEnqueue(t *testing.T, q *queue.Queue, s queue.Spec) string {
	t.Helper()
	id, _, err := q.Enqueue(context.Background(), s)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	return id
}

func waitStatus(t *testing.T, q *queue.Queue, id string, want job.Status) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		j, err := q.Get(context.Background(), id)
		if err == nil && j.Status == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("job %s: got %+v (err %v), want status %s", id, j, err, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

type run struct {
	id string
	at time.Time
}

func waitRun(t *testing.T, runs <-chan run, timeout time.Duration) run {
	t.Helper()
	select {
	case r := <-runs:
		return r
	case <-time.After(timeout):
		t.Fatalf("no job ran within %s", timeout)
		return run{}
	}
}

// recorder returns a handler that reports each job's ID and start time.
func recorder(runs chan<- run) worker.HandlerFunc {
	return func(_ context.Context, j *job.Job) error {
		runs <- run{id: j.ID, at: time.Now()}
		return nil
	}
}

func TestPoolRunsImmediateAndDelayedJobs(t *testing.T) {
	q, _, _ := newQueue(t)
	runs := make(chan run, 2)
	startPool(t, q, 2, worker.Handlers{"record": recorder(runs)})
	const delay = time.Second

	submitted := time.Now()
	delayed := mustEnqueue(t, q, queue.Spec{Type: "record", Delay: delay})
	immediate := mustEnqueue(t, q, queue.Spec{Type: "record"})

	first := waitRun(t, runs, 2*time.Second)
	if first.id != immediate {
		t.Fatalf("first job to run = %s, want the immediate job %s", first.id, immediate)
	}
	t.Logf("immediate job ran %s after submit", first.at.Sub(submitted))

	second := waitRun(t, runs, 3*time.Second)
	if second.id != delayed {
		t.Fatalf("second job to run = %s, want the delayed job %s", second.id, delayed)
	}
	// Redis rounds to whole milliseconds, so allow 1ms of slack.
	elapsed := second.at.Sub(submitted)
	t.Logf("delayed job ran %s after submit (delay %s)", elapsed, delay)
	if elapsed < delay-time.Millisecond {
		t.Errorf("delayed job ran after %s, before its %s delay", elapsed, delay)
	}
	if elapsed > delay+500*time.Millisecond {
		t.Errorf("delayed job ran after %s, more than 500ms late", elapsed)
	}

	for _, id := range []string{immediate, delayed} {
		waitStatus(t, q, id, job.StatusSucceeded)
	}
}

// TestPoolRunsEveryJobExactlyOnce is the core guarantee of claim.lua. The
// pool is already running while jobs are submitted, and some jobs are
// delayed, so claims race with enqueues and promotions as well as each other.
func TestPoolRunsEveryJobExactlyOnce(t *testing.T) {
	const (
		concurrency = 25
		jobs        = 10_000
		producers   = 10
	)
	q, rdb, keys := newQueue(t)
	ctx := context.Background()

	var mu sync.Mutex
	runs := make(map[string]int, jobs)
	stop := startPool(t, q, concurrency, worker.Handlers{
		"count": func(_ context.Context, j *job.Job) error {
			mu.Lock()
			defer mu.Unlock()
			runs[j.ID]++
			return nil
		},
	})

	start := time.Now()
	ids := make([]string, jobs)
	var submit sync.WaitGroup
	for p := range producers {
		submit.Go(func() {
			for i := p; i < jobs; i += producers {
				spec := queue.Spec{Type: "count"}
				if i%10 == 0 {
					spec.Delay = 50 * time.Millisecond
				}
				id, _, err := q.Enqueue(ctx, spec)
				if err != nil {
					t.Errorf("Enqueue: %v", err)
					return
				}
				ids[i] = id
			}
		})
	}
	submit.Wait()

	deadline := time.Now().Add(60 * time.Second)
	for {
		done, _ := rdb.HGet(ctx, keys.Stats(), "succeeded").Int()
		if done >= jobs {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d jobs finished within 60s", done, jobs)
		}
		time.Sleep(20 * time.Millisecond)
	}
	elapsed := time.Since(start)

	// Leave time for a duplicate run to show up before counting.
	time.Sleep(300 * time.Millisecond)
	stop()

	missing, duplicated := 0, 0
	for _, id := range ids {
		switch n := runs[id]; {
		case n == 0:
			missing++
		case n > 1:
			duplicated++
		}
	}
	if missing != 0 || duplicated != 0 || len(runs) != jobs {
		t.Errorf("%d jobs never ran, %d ran more than once, %d distinct jobs ran (want %d)", missing, duplicated, len(runs), jobs)
	}

	stats := rdb.HGetAll(ctx, keys.Stats()).Val()
	for _, name := range []string{"submitted", "claimed", "succeeded"} {
		if got, want := stats[name], "10000"; got != want {
			t.Errorf("stats.%s = %s, want %s", name, got, want)
		}
	}
	for _, key := range []string{keys.Ready(), keys.Scheduled(), keys.Leases()} {
		if n := rdb.ZCard(ctx, key).Val(); n != 0 {
			t.Errorf("%s still holds %d entries, want 0", key, n)
		}
	}

	pipe := rdb.Pipeline()
	cmds := make([]*redis.SliceCmd, jobs)
	for i, id := range ids {
		cmds[i] = pipe.HMGet(ctx, keys.Job(id), "status", "attempts", "lease_token", "lease_owner")
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("read jobs back: %v", err)
	}
	tokens := make(map[any]bool, jobs)
	owners := map[any]int{}
	for i, cmd := range cmds {
		f := cmd.Val()
		if f[0] != string(job.StatusSucceeded) || f[1] != "1" {
			t.Fatalf("job %s: status %v, attempts %v, want succeeded on attempt 1", ids[i], f[0], f[1])
		}
		tokens[f[2]] = true
		owners[f[3]]++
	}
	if len(tokens) != jobs {
		t.Errorf("%d distinct fencing tokens across %d jobs, want one each", len(tokens), jobs)
	}
	if len(owners) != concurrency {
		t.Errorf("%d slots ran jobs, want all %d", len(owners), concurrency)
	}
	t.Logf("%d jobs on %d slots in %s (%.0f jobs/s)", jobs, len(owners), elapsed.Round(time.Millisecond), jobs/elapsed.Seconds())
}

// TestPoolRunsJobsConcurrently proves the pool is really N wide: every
// handler blocks until all N are running, which can only happen in parallel.
func TestPoolRunsJobsConcurrently(t *testing.T) {
	const concurrency = 25
	q, _, _ := newQueue(t)
	arrived := make(chan string, concurrency)
	release := make(chan struct{})
	for range concurrency {
		mustEnqueue(t, q, queue.Spec{Type: "block"})
	}

	startPool(t, q, concurrency, worker.Handlers{
		"block": func(ctx context.Context, j *job.Job) error {
			arrived <- j.LeaseOwner
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})

	owners := map[string]bool{}
	for i := range concurrency {
		select {
		case owner := <-arrived:
			owners[owner] = true
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d jobs were running at the same time", i, concurrency)
		}
	}
	close(release)
	if len(owners) != concurrency {
		t.Errorf("jobs ran on %d distinct slots, want %d", len(owners), concurrency)
	}
}

func TestPoolNeverExceedsConcurrency(t *testing.T) {
	const (
		concurrency = 3
		jobs        = 30
	)
	q, _, _ := newQueue(t)
	var running, peak atomic.Int32
	finished := make(chan struct{}, jobs)
	for range jobs {
		mustEnqueue(t, q, queue.Spec{Type: "track"})
	}

	startPool(t, q, concurrency, worker.Handlers{
		"track": func(context.Context, *job.Job) error {
			n := running.Add(1)
			defer running.Add(-1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
			finished <- struct{}{}
			return nil
		},
	})

	for i := range jobs {
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d jobs finished", i, jobs)
		}
	}
	if got := peak.Load(); got > concurrency || got < 2 {
		t.Errorf("peak running jobs = %d, want between 2 and %d", got, concurrency)
	}
}

func TestPoolSurvivesHandlerPanic(t *testing.T) {
	q, _, _ := newQueue(t)
	runs := make(chan run, 1)
	// One slot, so the second job can only run if that slot survived.
	startPool(t, q, 1, worker.Handlers{
		"boom":   func(context.Context, *job.Job) error { panic("kaboom") },
		"record": recorder(runs),
	})

	bad := mustEnqueue(t, q, queue.Spec{Type: "boom"})
	waitStatus(t, q, bad, job.StatusRunning)
	good := mustEnqueue(t, q, queue.Spec{Type: "record"})

	if got := waitRun(t, runs, 2*time.Second); got.id != good {
		t.Fatalf("ran job %s, want %s", got.id, good)
	}
	waitStatus(t, q, good, job.StatusSucceeded)
	waitStatus(t, q, bad, job.StatusRunning)
}

func TestPoolLeavesFailedJobRunning(t *testing.T) {
	q, _, _ := newQueue(t)
	startPool(t, q, 1, nil)
	id := mustEnqueue(t, q, queue.Spec{Type: "no-such-type"})

	// Until retries exist (Phase 4), a failed job keeps its lease and stays
	// running so the reaper (Phase 3) can hand it to another worker.
	waitStatus(t, q, id, job.StatusRunning)
	time.Sleep(200 * time.Millisecond)
	if j, _ := q.Get(context.Background(), id); j.Status != job.StatusRunning {
		t.Errorf("Status = %s, want running", j.Status)
	}
}

func TestPoolStopsWhenCancelled(t *testing.T) {
	q, _, _ := newQueue(t)
	stop := startPool(t, q, 5, nil)
	time.Sleep(50 * time.Millisecond)

	stopped := make(chan struct{})
	go func() {
		stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("pool did not stop within 1s of cancel")
	}
}
