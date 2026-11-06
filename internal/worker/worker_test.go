package worker_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/sham-33/job-scheduler/internal/job"
	"github.com/sham-33/job-scheduler/internal/queue"
	"github.com/sham-33/job-scheduler/internal/scheduler"
	"github.com/sham-33/job-scheduler/internal/store/storetest"
	"github.com/sham-33/job-scheduler/internal/worker"
)

type run struct {
	id string
	at time.Time
}

// startWorker runs a worker and the promoter against a fresh namespace.
// Every job of type "record" reports its ID and start time on the channel.
func startWorker(t *testing.T) (*queue.Queue, <-chan run) {
	t.Helper()
	rdb := storetest.Client(t)
	q := queue.New(rdb, storetest.Keys(t, rdb), 2*time.Second)
	runs := make(chan run, 10)
	handlers := worker.Handlers{
		"record": func(ctx context.Context, j *job.Job) error {
			runs <- run{id: j.ID, at: time.Now()}
			return nil
		},
	}
	logger := slog.New(slog.DiscardHandler)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{}, 2)
	go func() { worker.New("test-worker", q, handlers, logger).Run(ctx); done <- struct{}{} }()
	go func() { scheduler.RunPromoter(ctx, q, logger); done <- struct{}{} }()
	t.Cleanup(func() {
		cancel()
		<-done
		<-done
	})
	return q, runs
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

func TestWorkerRunsImmediateAndDelayedJobs(t *testing.T) {
	q, runs := startWorker(t)
	ctx := context.Background()
	const delay = time.Second

	submitted := time.Now()
	delayed, _, err := q.Enqueue(ctx, queue.Spec{Type: "record", Delay: delay})
	if err != nil {
		t.Fatal(err)
	}
	immediate, _, err := q.Enqueue(ctx, queue.Spec{Type: "record"})
	if err != nil {
		t.Fatal(err)
	}

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

func TestWorkerLeavesFailedJobRunning(t *testing.T) {
	q, _ := startWorker(t)
	id, _, err := q.Enqueue(context.Background(), queue.Spec{Type: "no-such-type"})
	if err != nil {
		t.Fatal(err)
	}
	// Until retries exist (Phase 4), a failed job keeps its lease and stays
	// running so the reaper (Phase 3) can hand it to another worker.
	waitStatus(t, q, id, job.StatusRunning)
	time.Sleep(200 * time.Millisecond)
	if j, _ := q.Get(context.Background(), id); j.Status != job.StatusRunning {
		t.Errorf("Status = %s, want running", j.Status)
	}
}

func TestWorkerStopsWhenCancelled(t *testing.T) {
	rdb := storetest.Client(t)
	q := queue.New(rdb, storetest.Keys(t, rdb), time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		worker.New("w", q, nil, slog.New(slog.DiscardHandler)).Run(ctx)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return within 1s of cancel")
	}
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
			t.Fatalf("job %s: status = %v (err %v), want %s", id, j, err, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
