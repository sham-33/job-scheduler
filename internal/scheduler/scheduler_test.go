package scheduler_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/sham-33/job-scheduler/internal/election"
	"github.com/sham-33/job-scheduler/internal/queue"
	"github.com/sham-33/job-scheduler/internal/scheduler"
	"github.com/sham-33/job-scheduler/internal/store"
	"github.com/sham-33/job-scheduler/internal/store/storetest"
)

// startScheduler runs one scheduler on its own Redis connection, the way the
// scheduler command does, and returns that connection.
func startScheduler(t *testing.T, keys store.Keys, id string, ttl time.Duration) *redis.Client {
	t.Helper()
	rdb := storetest.Client(t)
	q := queue.New(rdb, keys, 2*time.Second)
	logger := slog.New(slog.DiscardHandler)
	e := election.New(rdb, keys, election.Options{ID: id, TTL: ttl, RenewEvery: ttl / 3, PollEvery: 20 * time.Millisecond}, logger)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		e.Run(ctx, func(ctx context.Context, _ int64) {
			scheduler.Lead(ctx, q, 20*time.Millisecond, logger)
		})
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return rdb
}

func TestFailoverKeepsDelayedJobsMoving(t *testing.T) {
	const (
		ttl    = 300 * time.Millisecond
		jobs   = 100
		spread = 1500 * time.Millisecond
	)
	rdb := storetest.Client(t)
	keys := storetest.Keys(t, rdb)
	q := queue.New(rdb, keys, 2*time.Second)
	ctx := context.Background()

	conns := map[string]*redis.Client{
		"a": startScheduler(t, keys, "a", ttl),
		"b": startScheduler(t, keys, "b", ttl),
	}
	var leader string
	for deadline := time.Now().Add(time.Second); leader == ""; {
		if time.Now().After(deadline) {
			t.Fatal("no leader elected")
		}
		leader, _, _ = election.Leader(ctx, rdb, keys)
		time.Sleep(10 * time.Millisecond)
	}

	ids := map[string]bool{}
	for i := range jobs {
		id, _, err := q.Enqueue(ctx, queue.Spec{Type: "echo", Delay: spread * time.Duration(i) / jobs})
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		ids[id] = true
	}

	// The leader is cut off from Redis partway through: it can neither do its
	// work nor hand the seat over, as if it had crashed.
	time.AfterFunc(spread/3, func() { _ = conns[leader].Close() })

	// Stand in for a worker: claim and finish jobs, counting each run.
	runs := map[string]int{}
	var late time.Duration
	for deadline := time.Now().Add(spread + 3*time.Second); len(runs) < jobs; {
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d jobs ran", len(runs), jobs)
		}
		j, err := q.Claim(ctx, "w")
		if err != nil {
			t.Fatalf("Claim: %v", err)
		}
		if j == nil {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		runs[j.ID]++
		late = max(late, j.StartedAt.Sub(j.RunAt))
		if err := q.Complete(ctx, j); err != nil {
			t.Fatalf("Complete: %v", err)
		}
	}
	for id, n := range runs {
		if !ids[id] || n != 1 {
			t.Errorf("job %s ran %d times, want once", id, n)
		}
	}
	now, epoch, _ := election.Leader(ctx, rdb, keys)
	t.Logf("leader %s was cut off; %s took over (epoch %d); the latest job started %s after its run_at",
		leader, now, epoch, late.Round(time.Millisecond))
	if now == leader || now == "" || epoch != 2 {
		t.Errorf("leader after failover = %q (epoch %d), want the other scheduler in epoch 2", now, epoch)
	}
	// Jobs due during the gap wait for the new leader, so up to about a term.
	if late > ttl+500*time.Millisecond {
		t.Errorf("a job started %s late, want under %s", late, ttl+500*time.Millisecond)
	}
}

func TestLeadRunsEveryLoop(t *testing.T) {
	rdb := storetest.Client(t)
	keys := storetest.Keys(t, rdb)
	ctx := context.Background()
	q := queue.New(rdb, keys, 100*time.Millisecond)

	// A job whose worker died: claimed, then never renewed.
	orphan, _, err := q.Enqueue(ctx, queue.Spec{Type: "echo"})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if j, err := q.Claim(ctx, "dead-worker"); err != nil || j == nil {
		t.Fatalf("Claim = %v, %v", j, err)
	}
	// A job that falls due shortly.
	delayed, _, err := q.Enqueue(ctx, queue.Spec{Type: "echo", Delay: 100 * time.Millisecond})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	leadCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		scheduler.Lead(leadCtx, q, 20*time.Millisecond, slog.New(slog.DiscardHandler))
	}()
	defer func() {
		stop()
		<-done
	}()

	for _, id := range []string{orphan, delayed} {
		deadline := time.Now().Add(time.Second)
		for {
			j, err := q.Get(ctx, id)
			if err == nil && j.Status == "ready" {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("job %s is %+v, want ready", id, j)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}
