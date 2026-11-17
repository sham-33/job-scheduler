package events_test

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/sham-33/job-scheduler/internal/election"
	"github.com/sham-33/job-scheduler/internal/events"
	"github.com/sham-33/job-scheduler/internal/job"
	"github.com/sham-33/job-scheduler/internal/queue"
	"github.com/sham-33/job-scheduler/internal/store"
	"github.com/sham-33/job-scheduler/internal/store/storetest"
)

func setup(t *testing.T, lease time.Duration) (*queue.Queue, *events.Reader, *redis.Client, store.Keys) {
	t.Helper()
	rdb := storetest.Client(t)
	keys := storetest.Keys(t, rdb)
	return queue.New(rdb, keys, lease), events.NewReader(rdb, keys), rdb, keys
}

func mustEnqueue(t *testing.T, q *queue.Queue, s queue.Spec) string {
	t.Helper()
	id, _, err := q.Enqueue(context.Background(), s)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	return id
}

func mustSnapshot(t *testing.T, r *events.Reader, recent int) *events.Snapshot {
	t.Helper()
	s, err := r.Snapshot(context.Background(), recent)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	return s
}

// waitFor polls cond until it holds, or fails the test after timeout.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSnapshotOfEmptySystem(t *testing.T) {
	_, r, rdb, _ := setup(t, time.Second)
	s := mustSnapshot(t, r, 10)
	if len(s.Stats) != 0 || s.Depths != (events.Depths{}) || s.Leader != (events.Leader{}) {
		t.Errorf("snapshot = %+v, want nothing in it", s)
	}
	if s.Workers == nil || len(s.Workers) != 0 || s.Events == nil || len(s.Events) != 0 {
		t.Errorf("workers = %#v, events = %#v; want empty, non-nil lists", s.Workers, s.Events)
	}
	if now := rdb.Time(context.Background()).Val(); now.Sub(s.At).Abs() > time.Second {
		t.Errorf("snapshot taken at %s, Redis says it is %s", s.At, now)
	}
}

func TestSnapshotCountsAndDepths(t *testing.T) {
	ctx := context.Background()
	q, r, _, _ := setup(t, time.Second)
	mustEnqueue(t, q, queue.Spec{Type: "echo", Delay: time.Hour})
	for range 3 {
		mustEnqueue(t, q, queue.Spec{Type: "echo"})
	}
	claim := func(owner string) *job.Job {
		j, err := q.Claim(ctx, owner)
		if err != nil || j == nil {
			t.Fatalf("Claim(%s) = %v, %v", owner, j, err)
		}
		return j
	}
	claim("w/0") // left running
	retrying := claim("w/1")
	if _, dead, err := q.Fail(ctx, retrying, errors.New("flaked")); err != nil || dead {
		t.Fatalf("Fail = dead %v, %v; want a retry", dead, err)
	}
	if _, dead, err := q.Fail(ctx, claim("w/2"), job.Permanent(errors.New("bad input"))); err != nil || !dead {
		t.Fatalf("Fail = dead %v, %v; want dead", dead, err)
	}

	s := mustSnapshot(t, r, 100)
	if want := (events.Depths{Scheduled: 2, Ready: 0, Running: 1, Dead: 1}); s.Depths != want {
		t.Errorf("depths = %+v, want %+v", s.Depths, want)
	}
	for name, want := range map[string]int64{"submitted": 4, "claimed": 3, "retried": 1, "dead": 1} {
		if s.Stats[name] != want {
			t.Errorf("stats.%s = %d, want %d", name, s.Stats[name], want)
		}
	}
	var kinds []string
	for _, e := range s.Events {
		kinds = append(kinds, e.Event)
	}
	if got, want := strings.Join(kinds, ","), "dead,claimed,retried,claimed,claimed,submitted,submitted,submitted,submitted"; got != want {
		t.Errorf("events, newest first = %s, want %s", got, want)
	}
	if e := s.Events[2]; e.JobID != retrying.ID || e.Type != "echo" || e.From != "running" ||
		e.To != "scheduled" || e.Worker != "w/1" || e.At.IsZero() || e.ID == "" {
		t.Errorf("retry event = %+v, want w/1's retry of %s", e, retrying.ID)
	}

	recent, err := r.Recent(ctx, 2)
	if err != nil || len(recent) != 2 || recent[0] != s.Events[0] || recent[1] != s.Events[1] {
		t.Errorf("Recent(2) = %+v, %v; want the two newest", recent, err)
	}
	if none, err := r.Recent(ctx, 0); err != nil || len(none) != 0 {
		t.Errorf("Recent(0) = %v, %v; want none", none, err)
	}
}

func TestWorkersAliveAndDead(t *testing.T) {
	ctx := context.Background()
	q, r, rdb, keys := setup(t, time.Second)
	for _, id := range []string{"b-alive", "a-dead", "c-quiet"} {
		if _, err := q.BeatWorker(ctx, queue.WorkerBeat{ID: id, Concurrency: 25, Running: 7, First: true}); err != nil {
			t.Fatal(err)
		}
	}
	now := rdb.Time(ctx).Val()
	// a-dead has gone unseen for longer than DeadAfter, c-quiet for less.
	rdb.ZAdd(ctx, keys.Workers(), redis.Z{Score: float64(now.Add(-events.DeadAfter - time.Second).UnixMilli()), Member: "a-dead"})
	rdb.ZAdd(ctx, keys.Workers(), redis.Z{Score: float64(now.Add(-events.DeadAfter + time.Second).UnixMilli()), Member: "c-quiet"})

	workers, err := r.Workers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(workers) != 3 {
		t.Fatalf("workers = %+v, want 3", workers)
	}
	for i, want := range []struct {
		id    string
		alive bool
	}{{"a-dead", false}, {"b-alive", true}, {"c-quiet", true}} {
		w := workers[i]
		if w.ID != want.id || w.Alive != want.alive {
			t.Errorf("worker %d = %s alive %v, want %s alive %v", i, w.ID, w.Alive, want.id, want.alive)
		}
		if w.Concurrency != 25 || w.Running != 7 || w.StartedAt.IsZero() || w.LastSeen.IsZero() {
			t.Errorf("worker %s = %+v, want concurrency 25, running 7 and both times set", w.ID, w)
		}
	}
}

func TestLeaderFollowsElection(t *testing.T) {
	ctx := context.Background()
	_, r, rdb, keys := setup(t, time.Second)
	runCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	elector := election.New(rdb, keys, election.Options{ID: "sched-1", TTL: time.Second, RenewEvery: 250 * time.Millisecond},
		slog.New(slog.DiscardHandler))
	wg.Go(func() { elector.Run(runCtx, func(ctx context.Context, _ int64) { <-ctx.Done() }) })

	waitFor(t, 2*time.Second, "a leader", func() bool {
		l, err := r.Leader(ctx)
		return err == nil && l.ID != ""
	})
	if l, _ := r.Leader(ctx); l != (events.Leader{ID: "sched-1", Epoch: 1}) {
		t.Errorf("leader = %+v, want sched-1 in epoch 1", l)
	}

	// Stepping down hands the seat back; the epoch stays that of the last term.
	cancel()
	wg.Wait()
	if l, err := r.Leader(ctx); err != nil || l != (events.Leader{Epoch: 1}) {
		t.Errorf("leader after stepping down = %+v, %v; want none, epoch 1", l, err)
	}
}
