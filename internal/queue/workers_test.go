package queue

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func mustBeat(t *testing.T, q *Queue, b WorkerBeat) int {
	t.Helper()
	n, err := q.BeatWorker(context.Background(), b)
	if err != nil {
		t.Fatalf("BeatWorker(%s): %v", b.ID, err)
	}
	return n
}

func TestBeatWorker(t *testing.T) {
	ctx := context.Background()
	q, rdb, keys := newTestQueue(t)

	before := mustRedisNow(t, q)
	mustBeat(t, q, WorkerBeat{ID: "w1", Concurrency: 25, Running: 3, First: true})
	seen, ok := inZSet(t, rdb, keys.Workers(), "w1")
	if !ok {
		t.Fatal("w1 is not in the workers set")
	}
	if at := time.UnixMilli(int64(seen)); at.Before(before) || at.After(mustRedisNow(t, q)) {
		t.Errorf("w1 last seen at %s, want Redis's clock at the beat (after %s)", at, before)
	}
	h := rdb.HGetAll(ctx, keys.Worker("w1")).Val()
	if h["concurrency"] != "25" || h["running"] != "3" || h["started_at"] != strconv64(seen) {
		t.Errorf("w1 hash = %v, want concurrency 25, running 3, started_at %d", h, int64(seen))
	}

	// Later beats move last_seen on and update running, but keep started_at.
	time.Sleep(5 * time.Millisecond)
	mustBeat(t, q, WorkerBeat{ID: "w1", Concurrency: 25, Running: 0})
	if later, _ := inZSet(t, rdb, keys.Workers(), "w1"); later <= seen {
		t.Errorf("last seen %v after a later beat, want after %v", later, seen)
	}
	h = rdb.HGetAll(ctx, keys.Worker("w1")).Val()
	if h["running"] != "0" || h["started_at"] != strconv64(seen) {
		t.Errorf("w1 hash after a later beat = %v, want running 0 and the first started_at", h)
	}

	// A restarted process that reuses the ID starts over.
	mustBeat(t, q, WorkerBeat{ID: "w1", Concurrency: 5, First: true})
	if h := rdb.HGetAll(ctx, keys.Worker("w1")).Val(); h["started_at"] == strconv64(seen) || h["concurrency"] != "5" {
		t.Errorf("w1 hash after a restart = %v, want a new started_at and concurrency 5", h)
	}

	if err := q.RemoveWorker(ctx, "w1"); err != nil {
		t.Fatalf("RemoveWorker: %v", err)
	}
	if _, ok := inZSet(t, rdb, keys.Workers(), "w1"); ok {
		t.Error("removed worker is still in the workers set")
	}
	if n := rdb.Exists(ctx, keys.Worker("w1")).Val(); n != 0 {
		t.Error("removed worker's hash still exists")
	}
}

func TestBeatForgetsLongDeadWorkers(t *testing.T) {
	ctx := context.Background()
	q, rdb, keys := newTestQueue(t)
	now := mustRedisNow(t, q)
	lastSeen := map[string]time.Time{
		"crashed-long-ago": now.Add(-ForgetWorkersAfter - time.Second),
		"crashed-recently": now.Add(-ForgetWorkersAfter + time.Minute),
	}
	// Register both first, then backdate them, so neither beat forgets the
	// other.
	for id := range lastSeen {
		mustBeat(t, q, WorkerBeat{ID: id, Concurrency: 1, First: true})
	}
	for id, at := range lastSeen {
		rdb.ZAdd(ctx, keys.Workers(), redis.Z{Score: float64(at.UnixMilli()), Member: id})
	}

	if n := mustBeat(t, q, WorkerBeat{ID: "alive", Concurrency: 1, First: true}); n != 1 {
		t.Errorf("beat forgot %d workers, want 1", n)
	}
	if _, ok := inZSet(t, rdb, keys.Workers(), "crashed-long-ago"); ok {
		t.Error("worker unseen for longer than ForgetWorkersAfter is still registered")
	}
	if rdb.Exists(ctx, keys.Worker("crashed-long-ago")).Val() != 0 {
		t.Error("forgotten worker's hash still exists")
	}
	// One unseen for less time stays, so readers can still show it as dead.
	if _, ok := inZSet(t, rdb, keys.Workers(), "crashed-recently"); !ok {
		t.Error("recently crashed worker was forgotten too soon")
	}
	if rdb.Exists(ctx, keys.Worker("crashed-recently")).Val() != 1 {
		t.Error("recently crashed worker's hash is gone")
	}
}

func strconv64(f float64) string {
	return strconv.FormatInt(int64(f), 10)
}
