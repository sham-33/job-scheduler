package monitor_test

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
	"github.com/sham-33/job-scheduler/internal/job"
	"github.com/sham-33/job-scheduler/internal/monitor"
	"github.com/sham-33/job-scheduler/internal/queue"
	"github.com/sham-33/job-scheduler/internal/scheduler"
	"github.com/sham-33/job-scheduler/internal/store"
	"github.com/sham-33/job-scheduler/internal/store/storetest"
	"github.com/sham-33/job-scheduler/internal/worker"
)

var discard = slog.New(slog.DiscardHandler)

func setup(t *testing.T) (*redis.Client, store.Keys, *monitor.Monitor) {
	t.Helper()
	rdb := storetest.Client(t)
	keys := storetest.Keys(t, rdb)
	return rdb, keys, monitor.New(rdb, keys)
}

func mustSnapshot(t *testing.T, m *monitor.Monitor) monitor.Snapshot {
	t.Helper()
	s, err := m.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	return s
}

// waitSnapshot polls until ok accepts a snapshot, and returns it.
func waitSnapshot(t *testing.T, m *monitor.Monitor, what string, ok func(monitor.Snapshot) bool) monitor.Snapshot {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		s := mustSnapshot(t, m)
		if ok(s) {
			return s
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s; last snapshot: %+v", what, s)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func mustWorkers(t *testing.T, m *monitor.Monitor) []monitor.Worker {
	t.Helper()
	ws, err := m.Workers(context.Background())
	if err != nil {
		t.Fatalf("Workers: %v", err)
	}
	return ws
}

func redisNow(t *testing.T, rdb *redis.Client) time.Time {
	t.Helper()
	now, err := rdb.Time(context.Background()).Result()
	if err != nil {
		t.Fatal(err)
	}
	return now
}

// lastSeen moves a worker's last check-in to at, in both places a check-in
// records it.
func lastSeen(t *testing.T, rdb *redis.Client, keys store.Keys, id string, at time.Time) {
	t.Helper()
	ctx := context.Background()
	ms := at.UnixMilli()
	if err := rdb.ZAdd(ctx, keys.Workers(), redis.Z{Score: float64(ms), Member: id}).Err(); err != nil {
		t.Fatal(err)
	}
	if rdb.Exists(ctx, keys.Worker(id)).Val() == 1 {
		if err := rdb.HSet(ctx, keys.Worker(id), "last_seen", ms).Err(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestEmptySnapshot(t *testing.T) {
	_, _, m := setup(t)
	s := mustSnapshot(t, m)
	if len(s.Counters) != 0 || s.Queues != (monitor.QueueSizes{}) || s.Leader != "" || s.Epoch != 0 || len(s.Workers) != 0 {
		t.Errorf("snapshot of an empty cluster = %+v", s)
	}
	if time.Since(s.At).Abs() > 5*time.Second {
		t.Errorf("At = %s, want about now", s.At)
	}
}

func TestRegistry(t *testing.T) {
	rdb, keys, m := setup(t)
	ctx := context.Background()
	reg := monitor.NewRegistry(rdb, keys, discard)

	if err := reg.Beat(ctx, "w1", monitor.WorkerStatus{Concurrency: 25, Busy: 3}, true); err != nil {
		t.Fatal(err)
	}
	if err := reg.Beat(ctx, "w2", monitor.WorkerStatus{Concurrency: 5, Draining: true}, true); err != nil {
		t.Fatal(err)
	}
	ws := mustWorkers(t, m)
	if len(ws) != 2 {
		t.Fatalf("workers = %+v, want 2", ws)
	}
	w1 := ws[0]
	if w1.ID != "w1" || w1.Concurrency != 25 || w1.Busy != 3 || w1.Draining || !w1.Alive ||
		w1.StartedAt.IsZero() || w1.StartedAt.After(w1.LastSeen) {
		t.Errorf("w1 = %+v", w1)
	}
	if w2 := ws[1]; w2.ID != "w2" || w2.Concurrency != 5 || !w2.Draining || !w2.Alive {
		t.Errorf("w2 = %+v", w2)
	}
	started := w1.StartedAt

	// w1 goes quiet for longer than DeadAfter: it is listed dead, after the
	// live ones.
	now := redisNow(t, rdb)
	lastSeen(t, rdb, keys, "w1", now.Add(-monitor.DeadAfter-time.Second))
	ws = mustWorkers(t, m)
	if ws[0].ID != "w2" || !ws[0].Alive || ws[1].ID != "w1" || ws[1].Alive {
		t.Errorf("workers = %+v, want w2 alive then w1 dead", ws)
	}
	// Just inside DeadAfter it still counts as alive.
	lastSeen(t, rdb, keys, "w1", now.Add(-monitor.DeadAfter+time.Second))
	if ws = mustWorkers(t, m); !ws[0].Alive || !ws[1].Alive {
		t.Errorf("workers = %+v, want both alive", ws)
	}

	// Later beats keep the start time; a first beat (a restart) resets it.
	time.Sleep(5 * time.Millisecond)
	if err := reg.Beat(ctx, "w1", monitor.WorkerStatus{Concurrency: 25}, false); err != nil {
		t.Fatal(err)
	}
	if w := mustWorkers(t, m)[0]; !w.StartedAt.Equal(started) || w.Busy != 0 {
		t.Errorf("after a later beat w1 = %+v, want start %s kept and busy 0", w, started)
	}
	if err := reg.Beat(ctx, "w1", monitor.WorkerStatus{Concurrency: 25}, true); err != nil {
		t.Fatal(err)
	}
	if w := mustWorkers(t, m)[0]; !w.StartedAt.After(started) {
		t.Errorf("after a restart w1 started %s, want later than %s", w.StartedAt, started)
	}

	// A worker silent for over an hour is forgotten at the next beat.
	lastSeen(t, rdb, keys, "w2", now.Add(-time.Hour-time.Minute))
	if err := reg.Beat(ctx, "w1", monitor.WorkerStatus{}, false); err != nil {
		t.Fatal(err)
	}
	if ws = mustWorkers(t, m); len(ws) != 1 || ws[0].ID != "w1" {
		t.Errorf("workers = %+v, want only w1", ws)
	}

	// When the listing and the hash disagree, as when a check-in lands
	// between reading one and the other, the hash wins: it was written in
	// the same step as the worker's other details.
	if err := rdb.ZAdd(ctx, keys.Workers(), redis.Z{Score: 1, Member: "w1"}).Err(); err != nil {
		t.Fatal(err)
	}
	if w := mustWorkers(t, m)[0]; w.ID != "w1" || !w.Alive || w.LastSeen.Year() < 2000 {
		t.Errorf("w1 = %+v, want its last check-in from the hash", w)
	}

	// A worker whose hash expired before its listing is still shown, with
	// the listing's time.
	if err := rdb.Del(ctx, keys.Worker("w1")).Err(); err != nil {
		t.Fatal(err)
	}
	lastSeen(t, rdb, keys, "w1", now.Add(-time.Minute))
	if ws = mustWorkers(t, m); len(ws) != 1 || ws[0].Alive || ws[0].Concurrency != 0 ||
		ws[0].LastSeen.UnixMilli() != now.Add(-time.Minute).UnixMilli() {
		t.Errorf("workers = %+v, want w1 dead, seen a minute ago, with no details", ws)
	}

	if err := reg.Leave(ctx, "w1"); err != nil {
		t.Fatal(err)
	}
	if ws = mustWorkers(t, m); len(ws) != 0 {
		t.Errorf("workers after Leave = %+v, want none", ws)
	}
	if n := rdb.Exists(ctx, keys.Worker("w1")).Val(); n != 0 {
		t.Error("worker hash left behind after Leave")
	}
	// The worker hash expires with the listing.
	if err := reg.Beat(ctx, "w3", monitor.WorkerStatus{}, true); err != nil {
		t.Fatal(err)
	}
	if ttl := rdb.PTTL(ctx, keys.Worker("w3")).Val(); ttl < 59*time.Minute || ttl > time.Hour {
		t.Errorf("worker hash TTL = %s, want an hour", ttl)
	}
}

func TestRegistryRunChecksInUntilStopped(t *testing.T) {
	rdb, keys, m := setup(t)
	reg := monitor.NewRegistry(rdb, keys, discard)
	ctx, cancel := context.WithCancel(context.Background())
	var mu sync.Mutex
	busy := 1
	var wg sync.WaitGroup
	wg.Go(func() {
		reg.Run(ctx, "w1", func() monitor.WorkerStatus {
			mu.Lock()
			defer mu.Unlock()
			return monitor.WorkerStatus{Concurrency: 4, Busy: busy}
		})
	})
	defer func() { cancel(); wg.Wait() }()

	// It checks in at once, then every BeatEvery with fresh status.
	waitSnapshot(t, m, "w1 to check in", func(s monitor.Snapshot) bool {
		return len(s.Workers) == 1 && s.Workers[0].Busy == 1
	})
	first := mustWorkers(t, m)[0].LastSeen
	mu.Lock()
	busy = 2
	mu.Unlock()
	s := waitSnapshot(t, m, "a second check-in", func(s monitor.Snapshot) bool {
		return len(s.Workers) == 1 && s.Workers[0].Busy == 2
	})
	if gap := s.Workers[0].LastSeen.Sub(first); gap < monitor.BeatEvery/2 || gap > 2*monitor.BeatEvery {
		t.Errorf("check-ins %s apart, want about %s", gap, monitor.BeatEvery)
	}

	cancel()
	wg.Wait()
	if ws := mustWorkers(t, m); len(ws) != 0 {
		t.Errorf("workers after stopping = %+v, want none", ws)
	}
}

func TestEventsAfterAndRecentEvents(t *testing.T) {
	rdb, keys, m := setup(t)
	ctx := context.Background()
	q := queue.New(rdb, keys, time.Second)

	ids := make([]string, 5)
	for i := range ids {
		id, _, err := q.Enqueue(ctx, queue.Spec{Type: "echo"})
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = id
	}
	j, err := q.Claim(ctx, "w1/7")
	if err != nil || j == nil {
		t.Fatalf("Claim = %v, %v", j, err)
	}
	if _, _, err := q.Fail(ctx, j, job.Permanent(errors.New("nope"))); err != nil {
		t.Fatal(err)
	}

	// Following the stream in pages sees every event once, in order.
	var all []monitor.Event
	after := ""
	for {
		page, err := m.EventsAfter(ctx, after, 3)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		all = append(all, page...)
		after = page[len(page)-1].ID
	}
	if len(all) != 7 {
		t.Fatalf("read %d events, want 7: %+v", len(all), all)
	}
	for i, id := range ids {
		if e := all[i]; e.Name != monitor.EventSubmitted || e.JobID != id || e.JobType != "echo" ||
			e.From != "" || e.To != "ready" || e.Attempt != 0 || e.Worker != "" {
			t.Errorf("event %d = %+v, want %s submitted", i, e, id)
		}
	}
	claimed, dead := all[5], all[6]
	if claimed.Name != monitor.EventClaimed || claimed.JobID != ids[0] || claimed.Worker != "w1/7" || claimed.Attempt != 1 {
		t.Errorf("claimed event = %+v", claimed)
	}
	if dead.Name != monitor.EventDead || dead.From != "running" || dead.To != "dead" || dead.Error != "nope" || dead.Worker != "w1/7" {
		t.Errorf("dead event = %+v", dead)
	}
	for i := 1; i < len(all); i++ {
		if all[i].At.Before(all[i-1].At) || all[i].ID == all[i-1].ID {
			t.Errorf("events %d and %d out of order: %s then %s", i-1, i, all[i-1].ID, all[i].ID)
		}
	}
	if d := time.Since(dead.At).Abs(); d > 5*time.Second {
		t.Errorf("event time %s is %s from now", dead.At, d)
	}

	recent, err := m.RecentEvents(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(recent) != 2 || recent[0].ID != dead.ID || recent[1].ID != claimed.ID {
		t.Errorf("RecentEvents(2) = %+v, want the dead then the claimed event", recent)
	}
}

// TestSnapshotFollowsJobsThroughTheCluster runs a real pool and an elected
// scheduler, and watches counters, queues, workers and events change as
// jobs flow through.
func TestSnapshotFollowsJobsThroughTheCluster(t *testing.T) {
	rdb, keys, m := setup(t)
	ctx := context.Background()
	q := queue.New(rdb, keys, time.Second)

	release := make(chan struct{})
	handlers := worker.Handlers{
		"ok":  func(context.Context, *job.Job) error { return nil },
		"bad": func(context.Context, *job.Job) error { return job.Permanent(errors.New("bad input")) },
		"slow": func(ctx context.Context, _ *job.Job) error {
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}

	runCtx, stop := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Go(func() {
		election.New(rdb, keys, election.Options{ID: "sched-1", TTL: 3 * time.Second, RenewEvery: time.Second}, discard).
			Run(runCtx, func(ctx context.Context, _ int64) { scheduler.Lead(ctx, q, 20*time.Millisecond, discard) })
	})
	wg.Go(func() {
		worker.NewPool(worker.Options{
			ID: "worker-1", Concurrency: 4, HeartbeatEvery: 250 * time.Millisecond,
			ShutdownGrace: time.Second, Registry: monitor.NewRegistry(rdb, keys, discard),
		}, q, handlers, discard).Run(runCtx)
	})
	stopped := sync.OnceFunc(func() { stop(); wg.Wait() })
	defer stopped()

	s := waitSnapshot(t, m, "a leader and a live worker", func(s monitor.Snapshot) bool {
		return s.Leader == "sched-1" && len(s.Workers) == 1 && s.Workers[0].Alive
	})
	if s.Epoch != 1 || s.Workers[0].ID != "worker-1" || s.Workers[0].Concurrency != 4 || s.Workers[0].Busy != 0 {
		t.Errorf("snapshot = %+v, want epoch 1 and an idle worker-1 with 4 slots", s)
	}

	enqueue := func(spec queue.Spec) {
		if _, _, err := q.Enqueue(ctx, spec); err != nil {
			t.Fatal(err)
		}
	}
	for range 10 {
		enqueue(queue.Spec{Type: "ok"})
	}
	enqueue(queue.Spec{Type: "bad"})
	enqueue(queue.Spec{Type: "bad"})
	enqueue(queue.Spec{Type: "ok", Delay: 300 * time.Millisecond})
	enqueue(queue.Spec{Type: "slow"})
	enqueue(queue.Spec{Type: "slow"})

	s = waitSnapshot(t, m, "the quick jobs to finish while the slow ones run", func(s monitor.Snapshot) bool {
		return s.Counters["succeeded"] == 11 && s.Counters["dead"] == 2 && s.Queues.Running == 2 &&
			len(s.Workers) == 1 && s.Workers[0].Busy == 2
	})
	if s.Counters["submitted"] != 15 || s.Counters["claimed"] != 15 ||
		s.Queues != (monitor.QueueSizes{Running: 2, Dead: 2}) {
		t.Errorf("mid-run snapshot = %+v, want 15 submitted and claimed, 2 running and 2 dead", s)
	}

	close(release)
	s = waitSnapshot(t, m, "every job to finish", func(s monitor.Snapshot) bool {
		return s.Counters["succeeded"] == 13 && s.Queues.Running == 0 && len(s.Workers) == 1 && s.Workers[0].Busy == 0
	})
	if s.Queues != (monitor.QueueSizes{Dead: 2}) {
		t.Errorf("final queues = %+v, want only 2 dead", s.Queues)
	}

	events, err := m.EventsAfter(ctx, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	count := map[string]int{}
	for _, e := range events {
		count[e.Name]++
		if e.Name == monitor.EventClaimed && !strings.HasPrefix(e.Worker, "worker-1/") {
			t.Errorf("claimed by %q, want a worker-1 slot", e.Worker)
		}
	}
	want := map[string]int{"submitted": 15, "promoted": 1, "claimed": 15, "succeeded": 13, "dead": 2}
	for name, n := range want {
		if count[name] != n {
			t.Errorf("%d %s events, want %d (all: %v)", count[name], name, n, count)
		}
	}
	if len(count) != len(want) {
		t.Errorf("event counts = %v, want only %v", count, want)
	}

	// A worker that shuts down cleanly leaves the registry, and a scheduler
	// that does gives up the seat.
	stopped()
	s = mustSnapshot(t, m)
	if len(s.Workers) != 0 || s.Leader != "" || s.Epoch != 1 {
		t.Errorf("after shutdown = %+v, want no workers, no leader, epoch 1", s)
	}
}

func TestRegistryOnBeatOnlyAfterReachingRedis(t *testing.T) {
	rdb, keys, _ := setup(t)
	run := func(reg *monitor.Registry) int {
		var mu sync.Mutex
		beats := 0
		reg.OnBeat(func() { mu.Lock(); beats++; mu.Unlock() })
		ctx, cancel := context.WithTimeout(context.Background(), monitor.BeatEvery+300*time.Millisecond)
		defer cancel()
		reg.Run(ctx, "w1", func() monitor.WorkerStatus { return monitor.WorkerStatus{Concurrency: 1} })
		mu.Lock()
		defer mu.Unlock()
		return beats
	}
	if n := run(monitor.NewRegistry(rdb, keys, discard)); n != 2 {
		t.Errorf("OnBeat ran %d times in 1.3 s, want 2 (at once, then after 1 s)", n)
	}
	broken := redis.NewClient(&redis.Options{Addr: "localhost:1", MaxRetries: -1})
	t.Cleanup(func() { _ = broken.Close() })
	if n := run(monitor.NewRegistry(broken, keys, discard)); n != 0 {
		t.Errorf("OnBeat ran %d times with Redis unreachable, want 0", n)
	}
}
