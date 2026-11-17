package events_test

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/sham-33/job-scheduler/internal/election"
	"github.com/sham-33/job-scheduler/internal/events"
	"github.com/sham-33/job-scheduler/internal/job"
	"github.com/sham-33/job-scheduler/internal/queue"
	"github.com/sham-33/job-scheduler/internal/scheduler"
	"github.com/sham-33/job-scheduler/internal/worker"
)

// eventKinds are the job changes that are both counted and streamed.
var eventKinds = []string{"submitted", "promoted", "claimed", "succeeded", "retried",
	"dead", "reclaimed", "released", "cancelled", "replayed"}

// TestCountersAndEventsFollowJobs runs a worker pool and an elected
// scheduler, sends jobs through every path (plain, delayed, retried, dead,
// cancelled, and reclaimed from a worker that crashed), and reads snapshots
// the whole time. Every snapshot must agree with itself, and with the one
// before it.
func TestCountersAndEventsFollowJobs(t *testing.T) {
	ctx := context.Background()
	const lease = 300 * time.Millisecond
	q, r, rdb, keys := setup(t, lease)
	logger := slog.New(slog.DiscardHandler)
	runCtx, stop := context.WithCancel(ctx)
	var services sync.WaitGroup
	t.Cleanup(func() { stop(); services.Wait() })

	// One elected scheduler runs the promoter, the reaper and crons.
	elector := election.New(rdb, keys, election.Options{ID: "sched-1", TTL: time.Second, RenewEvery: 250 * time.Millisecond}, logger)
	services.Go(func() {
		elector.Run(runCtx, func(ctx context.Context, _ int64) { scheduler.Lead(ctx, q, 20*time.Millisecond, logger) })
	})

	// A job claimed by a worker that then crashed: nothing will renew its
	// lease, so the reaper must take it back.
	crashedJob := mustEnqueue(t, q, queue.Spec{Type: "ok"})
	if j, err := q.Claim(ctx, "crashed/0"); err != nil || j == nil || j.ID != crashedJob {
		t.Fatalf("Claim = %v, %v", j, err)
	}

	poolCtx, stopPool := context.WithCancel(runCtx)
	var pool sync.WaitGroup
	pool.Go(func() {
		worker.NewPool(worker.Options{ID: "w1", Concurrency: 4, HeartbeatEvery: lease / 4,
			ShutdownGrace: time.Second, BeatEvery: 100 * time.Millisecond}, q, worker.Handlers{
			"ok": func(ctx context.Context, _ *job.Job) error {
				time.Sleep(20 * time.Millisecond)
				return nil
			},
			"flaky": func(_ context.Context, j *job.Job) error {
				if j.Attempts < 3 {
					return errors.New("flaked")
				}
				return nil
			},
			"bad": func(context.Context, *job.Job) error { return job.Permanent(errors.New("bad input")) },
		}, logger).Run(poolCtx)
	})

	start := time.Now()
	var snaps []*events.Snapshot
	snap := func() *events.Snapshot {
		s := mustSnapshot(t, r, 1000)
		snaps = append(snaps, s)
		return s
	}
	waitFor(t, 2*time.Second, "the worker to register and a scheduler to lead", func() bool {
		s := snap()
		return len(s.Workers) == 1 && s.Leader.ID != ""
	})

	fast := queue.Backoff{Base: 10 * time.Millisecond, Max: 10 * time.Millisecond}
	ids := []string{crashedJob}
	for range 20 {
		ids = append(ids, mustEnqueue(t, q, queue.Spec{Type: "ok"}))
	}
	for range 5 {
		ids = append(ids, mustEnqueue(t, q, queue.Spec{Type: "ok", Delay: 200 * time.Millisecond}))
	}
	flaky := mustEnqueue(t, q, queue.Spec{Type: "flaky", Backoff: fast})
	bad := mustEnqueue(t, q, queue.Spec{Type: "bad"})
	cancelled := mustEnqueue(t, q, queue.Spec{Type: "ok", Delay: time.Hour})
	if err := q.Cancel(ctx, cancelled); err != nil {
		t.Fatal(err)
	}
	ids = append(ids, flaky, bad, cancelled)

	want := map[string]int64{
		"submitted": 29,
		"promoted":  7,  // 5 delayed jobs, and the flaky job's 2 retries
		"claimed":   31, // 26 that ran once, the flaky job 3 times, the crashed job twice
		"succeeded": 27,
		"retried":   2,
		"dead":      1,
		"reclaimed": 1,
		"cancelled": 1,
	}
	waitFor(t, 10*time.Second, "every job to finish", func() bool {
		s := snap()
		return s.Stats["succeeded"] == want["succeeded"] && s.Stats["dead"] == want["dead"] &&
			s.Depths == (events.Depths{Dead: 1})
	})
	t.Logf("all %d jobs finished in %s; took %d snapshots", len(ids), time.Since(start).Round(time.Millisecond), len(snaps))

	// Every snapshot agrees with itself: its counters match the events in
	// it exactly, because both are read in one transaction. And nothing
	// ever goes backwards between snapshots.
	for i, s := range snaps {
		counts := map[string]int64{}
		for _, e := range s.Events {
			counts[e.Event]++
		}
		for _, kind := range eventKinds {
			if counts[kind] != s.Stats[kind] {
				t.Errorf("snapshot %d: stats.%s = %d, but it holds %d %s events", i, kind, s.Stats[kind], counts[kind], kind)
			}
		}
		if i == 0 {
			continue
		}
		prev := snaps[i-1]
		for name, n := range prev.Stats {
			if s.Stats[name] < n {
				t.Errorf("snapshot %d: stats.%s fell from %d to %d", i, name, n, s.Stats[name])
			}
		}
		if len(s.Events) < len(prev.Events) || (len(prev.Events) > 0 && s.Events[len(s.Events)-len(prev.Events)] != prev.Events[0]) {
			t.Errorf("snapshot %d: events don't continue on from snapshot %d", i, i-1)
		}
		if s.At.Before(prev.At) {
			t.Errorf("snapshot %d taken at %s, before snapshot %d at %s", i, s.At, i-1, prev.At)
		}
	}
	// Counters rose in steps that the snapshots saw, not all at once.
	var seen []int64
	for _, s := range snaps {
		if n := s.Stats["succeeded"]; len(seen) == 0 || n != seen[len(seen)-1] {
			seen = append(seen, n)
		}
	}
	t.Logf("succeeded counter as the snapshots saw it: %v", seen)
	if len(seen) < 3 {
		t.Errorf("snapshots saw succeeded take values %v, want it seen rising", seen)
	}

	final := snaps[len(snaps)-1]
	for _, name := range slices.Sorted(maps.Keys(want)) {
		if final.Stats[name] != want[name] {
			t.Errorf("final stats.%s = %d, want %d", name, final.Stats[name], want[name])
		}
	}

	// Each job's events form an unbroken chain from submission to its
	// current status.
	chains := map[string][]events.Event{}
	for _, e := range slices.Backward(final.Events) {
		chains[e.JobID] = append(chains[e.JobID], e)
	}
	for _, id := range ids {
		j, err := q.Get(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		chain := chains[id]
		if len(chain) == 0 || chain[0].Event != "submitted" || chain[0].From != "" {
			t.Errorf("job %s: events start %+v, want a submission", id, chain)
			continue
		}
		for k := 1; k < len(chain); k++ {
			if chain[k].From != chain[k-1].To {
				t.Errorf("job %s: %s from %q follows %s to %q", id, chain[k].Event, chain[k].From, chain[k-1].Event, chain[k-1].To)
			}
		}
		if last := chain[len(chain)-1]; last.To != string(j.Status) {
			t.Errorf("job %s: last event %s leaves it %s, but it is %s", id, last.Event, last.To, j.Status)
		}
	}

	// The crashed worker's job was taken back from it and finished by w1.
	var kinds []string
	for _, e := range chains[crashedJob] {
		kinds = append(kinds, e.Event+" "+e.Worker)
	}
	if len(kinds) != 5 || kinds[1] != "claimed crashed/0" || kinds[2] != "reclaimed crashed/0" ||
		!strings.HasPrefix(kinds[3], "claimed w1/") || !strings.HasPrefix(kinds[4], "succeeded w1/") {
		t.Errorf("crashed job's events = %q, want it claimed by crashed/0, reclaimed from it, then run by w1", kinds)
	}

	// The registry shows the worker alive while it runs, and empty once it
	// has shut down.
	if w := final.Workers; len(w) != 1 || w[0].ID != "w1" || !w[0].Alive || w[0].Concurrency != 4 {
		t.Errorf("workers = %+v, want w1 alive with concurrency 4", w)
	}
	if final.Leader.ID != "sched-1" || final.Leader.Epoch != 1 {
		t.Errorf("leader = %+v, want sched-1 in epoch 1", final.Leader)
	}
	stopPool()
	pool.Wait()
	if w, err := r.Workers(ctx); err != nil || len(w) != 0 {
		t.Errorf("workers after shutdown = %+v, %v; want none", w, err)
	}
}

// TestRegistryCountsRunningJobs checks that a worker's beats report how many
// jobs it is running.
func TestRegistryCountsRunningJobs(t *testing.T) {
	ctx := context.Background()
	q, r, _, _ := setup(t, time.Second)
	release := make(chan struct{})
	poolCtx, stop := context.WithCancel(ctx)
	var pool sync.WaitGroup
	t.Cleanup(func() { stop(); pool.Wait() })
	pool.Go(func() {
		worker.NewPool(worker.Options{ID: "w1", Concurrency: 5, HeartbeatEvery: 250 * time.Millisecond,
			ShutdownGrace: time.Second, BeatEvery: 20 * time.Millisecond}, q, worker.Handlers{
			"wait": func(ctx context.Context, _ *job.Job) error {
				select {
				case <-release:
				case <-ctx.Done():
				}
				return nil
			},
		}, slog.New(slog.DiscardHandler)).Run(poolCtx)
	})

	running := func(want int) func() bool {
		return func() bool {
			w, err := r.Workers(ctx)
			return err == nil && len(w) == 1 && w[0].Running == want
		}
	}
	waitFor(t, 2*time.Second, "w1 to register, running nothing", running(0))
	for range 3 {
		mustEnqueue(t, q, queue.Spec{Type: "wait"})
	}
	waitFor(t, 2*time.Second, "w1 to report 3 running jobs", running(3))
	close(release)
	waitFor(t, 2*time.Second, "w1 to report 0 running jobs", running(0))
}

// TestWorkerStaysAliveWhileDraining checks that a worker keeps beating
// through its shutdown grace period, so it doesn't look dead while it
// finishes its last jobs, and leaves the registry once they are done.
func TestWorkerStaysAliveWhileDraining(t *testing.T) {
	ctx := context.Background()
	q, r, _, _ := setup(t, time.Second)
	release := make(chan struct{})
	poolCtx, stop := context.WithCancel(ctx)
	var pool sync.WaitGroup
	t.Cleanup(func() { stop(); pool.Wait() })
	pool.Go(func() {
		worker.NewPool(worker.Options{ID: "w1", Concurrency: 1, HeartbeatEvery: 250 * time.Millisecond,
			ShutdownGrace: 5 * time.Second, BeatEvery: 20 * time.Millisecond}, q, worker.Handlers{
			"wait": func(context.Context, *job.Job) error {
				<-release
				return nil
			},
		}, slog.New(slog.DiscardHandler)).Run(poolCtx)
	})
	mustEnqueue(t, q, queue.Spec{Type: "wait"})
	worker1 := func() events.Worker {
		w, err := r.Workers(ctx)
		if err != nil || len(w) != 1 {
			return events.Worker{}
		}
		return w[0]
	}
	waitFor(t, 2*time.Second, "w1 to start the job", func() bool { return worker1().Running == 1 })

	stop() // shutdown starts; the job is still running
	stopped := worker1().LastSeen
	waitFor(t, 2*time.Second, "w1 to beat again while draining", func() bool {
		return worker1().LastSeen.After(stopped.Add(50 * time.Millisecond))
	})
	close(release)
	pool.Wait()
	if w, err := r.Workers(ctx); err != nil || len(w) != 0 {
		t.Errorf("workers after the pool stopped = %+v, %v; want none", w, err)
	}
}

// TestRestartedWorkerStartsOver checks that a process reusing a worker ID,
// as a restarted container does, reports its own start time rather than
// that of the process before it.
func TestRestartedWorkerStartsOver(t *testing.T) {
	ctx := context.Background()
	q, r, rdb, keys := setup(t, time.Second)
	// What the crashed process left behind: it started an hour ago and was
	// last seen a few seconds ago, so it is not yet forgotten.
	now := rdb.Time(ctx).Val()
	rdb.HSet(ctx, keys.Worker("w1"), "started_at", now.Add(-time.Hour).UnixMilli(), "concurrency", 25, "running", 3)
	rdb.ZAdd(ctx, keys.Workers(), redis.Z{Score: float64(now.Add(-6 * time.Second).UnixMilli()), Member: "w1"})

	poolCtx, stop := context.WithCancel(ctx)
	var pool sync.WaitGroup
	t.Cleanup(func() { stop(); pool.Wait() })
	before := now.Truncate(time.Millisecond)
	pool.Go(func() {
		worker.NewPool(worker.Options{ID: "w1", Concurrency: 2, HeartbeatEvery: 250 * time.Millisecond,
			BeatEvery: 20 * time.Millisecond}, q, worker.Handlers{}, slog.New(slog.DiscardHandler)).Run(poolCtx)
	})
	waitFor(t, 2*time.Second, "w1 to beat", func() bool {
		w, err := r.Workers(ctx)
		return err == nil && len(w) == 1 && w[0].Alive
	})
	w, _ := r.Workers(ctx)
	if w[0].StartedAt.Before(before) || w[0].Concurrency != 2 || w[0].Running != 0 {
		t.Errorf("restarted w1 = %+v, want started at or after %s, concurrency 2, running 0", w[0], before)
	}
}
