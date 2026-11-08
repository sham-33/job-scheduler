package worker_test

import (
	"context"
	"errors"
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
	return newQueueWithLease(t, 2*time.Second)
}

func newQueueWithLease(t *testing.T, lease time.Duration) (*queue.Queue, *redis.Client, store.Keys) {
	t.Helper()
	rdb := storetest.Client(t)
	keys := storetest.Keys(t, rdb)
	return queue.New(rdb, keys, lease), rdb, keys
}

// startPool runs a pool with default options until stop is called or the
// test ends.
func startPool(t *testing.T, q *queue.Queue, concurrency int, handlers worker.Handlers) (stop func()) {
	t.Helper()
	return startPoolWith(t, q, worker.Options{Concurrency: concurrency, ShutdownGrace: time.Second}, handlers)
}

// startPoolWith runs a pool, the promoter and the reaper until stop is
// called or the test ends. stop waits for the pool to shut down.
func startPoolWith(t *testing.T, q *queue.Queue, opts worker.Options, handlers worker.Handlers) (stop func()) {
	t.Helper()
	opts.ID = "test"
	if opts.HeartbeatEvery == 0 {
		opts.HeartbeatEvery = q.LeaseTTL() / 4
	}
	logger := slog.New(slog.DiscardHandler)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() { worker.NewPool(opts, q, handlers, logger).Run(ctx) })
	wg.Go(func() { scheduler.RunPromoter(ctx, q, logger) })
	wg.Go(func() { scheduler.RunReaper(ctx, q, 20*time.Millisecond, logger) })
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
	waitStatusWithin(t, q, id, want, 2*time.Second)
}

func waitStatusWithin(t *testing.T, q *queue.Queue, id string, want job.Status, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
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
	for _, name := range []string{"reclaimed", "fenced"} {
		if got := stats[name]; got != "" {
			t.Errorf("stats.%s = %s, want none on a healthy run", name, got)
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

// Until retries exist (Phase 4), a failed job's lease simply expires and the
// reaper hands it out again.
func TestPoolRunsFailedJobAgainAfterLeaseExpires(t *testing.T) {
	q, _, _ := newQueueWithLease(t, 100*time.Millisecond)
	startPool(t, q, 1, worker.Handlers{
		"fail": func(context.Context, *job.Job) error { return errors.New("nope") },
	})
	id := mustEnqueue(t, q, queue.Spec{Type: "fail"})

	deadline := time.Now().Add(3 * time.Second)
	for {
		j, err := q.Get(context.Background(), id)
		if err == nil && j.Attempts >= 3 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("job = %+v (err %v), want at least 3 attempts", j, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A job can run far longer than its lease, as long as the heartbeat renews it.
func TestPoolHeartbeatKeepsLongJobAlive(t *testing.T) {
	const lease = 200 * time.Millisecond
	q, rdb, keys := newQueueWithLease(t, lease)
	var runs atomic.Int32
	startPool(t, q, 1, worker.Handlers{
		"long": func(context.Context, *job.Job) error {
			runs.Add(1)
			time.Sleep(4 * lease)
			return nil
		},
	})
	id := mustEnqueue(t, q, queue.Spec{Type: "long"})

	waitStatusWithin(t, q, id, job.StatusSucceeded, 4*lease+2*time.Second)
	if got := runs.Load(); got != 1 {
		t.Errorf("handler ran %d times, want 1", got)
	}
	if j, _ := q.Get(context.Background(), id); j.Attempts != 1 {
		t.Errorf("Attempts = %d, want 1", j.Attempts)
	}
	stats := rdb.HGetAll(context.Background(), keys.Stats()).Val()
	if stats["reclaimed"] != "" || stats["fenced"] != "" {
		t.Errorf("stats = %v, want nothing reclaimed or fenced", stats)
	}
}

// A worker that dies never heartbeats again, which is exactly what claiming
// a job directly and then ignoring it looks like.
func TestPoolPicksUpJobFromCrashedWorker(t *testing.T) {
	const lease = 300 * time.Millisecond
	q, _, _ := newQueueWithLease(t, lease)
	ctx := context.Background()
	id := mustEnqueue(t, q, queue.Spec{Type: "record"})
	crashed, err := q.Claim(ctx, "crashed-worker")
	if err != nil || crashed == nil {
		t.Fatalf("Claim = (%v, %v)", crashed, err)
	}
	claimed := time.Now()

	runs := make(chan run, 1)
	startPool(t, q, 1, worker.Handlers{"record": recorder(runs)})

	got := waitRun(t, runs, lease+2*time.Second)
	recovery := got.at.Sub(claimed)
	t.Logf("job re-ran %s after its worker's last heartbeat (lease %s)", recovery, lease)
	if recovery < lease-5*time.Millisecond {
		t.Errorf("job re-ran after %s, before its %s lease could expire", recovery, lease)
	}
	if recovery > lease+500*time.Millisecond {
		t.Errorf("job re-ran after %s, want within 500ms of the %s lease", recovery, lease)
	}

	waitStatus(t, q, id, job.StatusSucceeded)
	j, _ := q.Get(ctx, id)
	if j.Attempts != 2 || j.LeaseOwner == crashed.LeaseOwner {
		t.Errorf("job = %+v, want attempt 2 under a new owner", j)
	}
	if err := q.Complete(ctx, crashed); !errors.Is(err, queue.ErrLeaseLost) {
		t.Errorf("crashed worker's Complete = %v, want ErrLeaseLost", err)
	}
}

func TestPoolCancelsHandlerWhenLeaseIsLost(t *testing.T) {
	q, rdb, keys := newQueueWithLease(t, 200*time.Millisecond)
	ctx := context.Background()
	started := make(chan *job.Job, 8)
	causes := make(chan error, 8)
	startPoolWith(t, q, worker.Options{Concurrency: 1}, worker.Handlers{
		"block": func(ctx context.Context, j *job.Job) error {
			started <- j
			<-ctx.Done()
			causes <- context.Cause(ctx)
			return ctx.Err()
		},
	})
	id := mustEnqueue(t, q, queue.Spec{Type: "block"})

	var j *job.Job
	select {
	case j = <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("job never started")
	}

	// Another worker taking over shows up as a newer fencing token.
	rdb.HSet(ctx, keys.Job(id), "lease_token", j.LeaseToken+1000)

	select {
	case cause := <-causes:
		if !errors.Is(cause, queue.ErrLeaseLost) {
			t.Errorf("handler cancelled with cause %v, want ErrLeaseLost", cause)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler was not cancelled after losing its lease")
	}
	if got, _ := q.Get(ctx, id); got.Status == job.StatusSucceeded {
		t.Error("job was marked succeeded by a worker that lost its lease")
	}
}

func TestPoolFinishesRunningJobsOnShutdown(t *testing.T) {
	q, _, _ := newQueue(t)
	ctx := context.Background()
	started := make(chan struct{}, 2)
	stop := startPoolWith(t, q, worker.Options{Concurrency: 1, ShutdownGrace: 5 * time.Second}, worker.Handlers{
		"work": func(ctx context.Context, _ *job.Job) error {
			started <- struct{}{}
			select {
			case <-time.After(300 * time.Millisecond):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
	running := mustEnqueue(t, q, queue.Spec{Type: "work"})
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("job never started")
	}
	waiting := mustEnqueue(t, q, queue.Spec{Type: "work"})

	stop()

	if j, _ := q.Get(ctx, running); j.Status != job.StatusSucceeded {
		t.Errorf("running job has status %s after shutdown, want succeeded", j.Status)
	}
	if j, _ := q.Get(ctx, waiting); j.Status != job.StatusReady || j.Attempts != 0 {
		t.Errorf("waiting job = %+v, want it left ready for another worker", j)
	}
}

func TestPoolCancelsJobsWhenGraceRunsOut(t *testing.T) {
	const grace = 100 * time.Millisecond
	q, _, _ := newQueue(t)
	started := make(chan struct{}, 1)
	cancelled := make(chan struct{}, 1)
	stop := startPoolWith(t, q, worker.Options{Concurrency: 1, ShutdownGrace: grace}, worker.Handlers{
		"block": func(ctx context.Context, _ *job.Job) error {
			started <- struct{}{}
			<-ctx.Done()
			cancelled <- struct{}{}
			return ctx.Err()
		},
	})
	id := mustEnqueue(t, q, queue.Spec{Type: "block"})
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("job never started")
	}

	begin := time.Now()
	stop()
	took := time.Since(begin)

	select {
	case <-cancelled:
	default:
		t.Error("handler was not cancelled")
	}
	if took < grace || took > grace+time.Second {
		t.Errorf("shutdown took %s, want about the %s grace period", took, grace)
	}
	// It keeps its lease, so the reaper can hand it to another worker.
	if j, _ := q.Get(context.Background(), id); j.Status != job.StatusRunning {
		t.Errorf("Status = %s, want running", j.Status)
	}
}

func TestPoolShutdownDoesNotWaitForeverOnStuckHandler(t *testing.T) {
	q, _, _ := newQueue(t)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	stop := startPoolWith(t, q, worker.Options{Concurrency: 1, ShutdownGrace: 10 * time.Millisecond}, worker.Handlers{
		"stuck": func(context.Context, *job.Job) error {
			started <- struct{}{}
			<-release // ignores cancellation
			return nil
		},
	})
	mustEnqueue(t, q, queue.Spec{Type: "stuck"})
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("job never started")
	}

	done := make(chan struct{})
	go func() {
		stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown hung on a handler that ignores cancellation")
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
