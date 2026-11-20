package queue

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/sham-33/job-scheduler/internal/job"
	"github.com/sham-33/job-scheduler/internal/store"
)

// ev is the part of an event the tests compare. Each field is the stream
// entry's field of the same name, and "-" means the field must be absent.
type ev struct {
	event, job, from, to, attempt, worker, err string
}

func (e ev) String() string {
	return fmt.Sprintf("%s %s %q→%q attempt=%s worker=%s err=%q", e.event, e.job, e.from, e.to, e.attempt, e.worker, e.err)
}

// readEvents returns every event in the stream, oldest first.
func readEvents(t *testing.T, rdb *redis.Client, keys store.Keys) []ev {
	t.Helper()
	msgs, err := rdb.XRange(context.Background(), keys.Events(), "-", "+").Result()
	if err != nil {
		t.Fatalf("XRANGE events: %v", err)
	}
	out := make([]ev, len(msgs))
	for i, m := range msgs {
		field := func(k string) string {
			if v, ok := m.Values[k].(string); ok {
				return v
			}
			return "-"
		}
		if got := field("job_type"); got != "work" {
			t.Errorf("event %d job_type = %q, want work", i, got)
		}
		out[i] = ev{field("event"), field("job_id"), field("from"), field("to"),
			field("attempt"), field("worker"), field("error")}
	}
	return out
}

// eventLog checks the events stream step by step.
type eventLog struct {
	t    *testing.T
	rdb  *redis.Client
	keys store.Keys
	seen int
}

// expect checks that the events added since the last call are exactly want.
func (l *eventLog) expect(step string, want ...ev) {
	l.t.Helper()
	all := readEvents(l.t, l.rdb, l.keys)
	got := all[l.seen:]
	l.seen = len(all)
	if len(got) != len(want) {
		l.t.Errorf("%s: got %d events, want %d:\n got  %v\n want %v", step, len(got), len(want), got, want)
		return
	}
	for i := range want {
		if got[i] != want[i] {
			l.t.Errorf("%s: event %d\n got  %v\n want %v", step, i, got[i], want[i])
		}
	}
}

// expireLease makes a running job's lease look expired to the reaper.
func expireLease(t *testing.T, rdb *redis.Client, keys store.Keys, id string) {
	t.Helper()
	if err := rdb.ZAdd(context.Background(), keys.Leases(), redis.Z{Score: 0, Member: id}).Err(); err != nil {
		t.Fatal(err)
	}
}

func claimAs(t *testing.T, q *Queue, worker, wantID string) *job.Job {
	t.Helper()
	j, err := q.Claim(context.Background(), worker)
	if err != nil || j == nil || j.ID != wantID {
		t.Fatalf("Claim = %v, %v; want job %s", j, err, wantID)
	}
	return j
}

func TestEventsForRetryAndSuccess(t *testing.T) {
	q, rdb, keys := newTestQueue(t)
	ctx := context.Background()
	log := &eventLog{t: t, rdb: rdb, keys: keys}

	id := mustEnqueue(t, q, Spec{Type: "work", Delay: 30 * time.Millisecond, Backoff: Backoff{Base: time.Millisecond, Max: time.Millisecond}})
	log.expect("enqueue", ev{"submitted", id, "", "scheduled", "0", "-", "-"})

	// A duplicate submission changes nothing, so it adds no event.
	if _, created, err := q.Enqueue(ctx, Spec{Type: "work", IdempotencyKey: "k"}); err != nil || !created {
		t.Fatalf("Enqueue: %v %v", created, err)
	}
	dup := readEvents(t, rdb, keys)[1].job
	if _, created, _ := q.Enqueue(ctx, Spec{Type: "work", IdempotencyKey: "k"}); created {
		t.Fatal("duplicate created a job")
	}
	log.expect("duplicate enqueue", ev{"submitted", dup, "", "ready", "0", "-", "-"})
	if err := q.Cancel(ctx, dup); err != nil {
		t.Fatal(err)
	}
	log.expect("cancel ready job", ev{"cancelled", dup, "ready", "cancelled", "0", "-", "-"})

	time.Sleep(40 * time.Millisecond)
	if n, err := q.Promote(ctx, 10); err != nil || n != 1 {
		t.Fatalf("Promote = %d, %v", n, err)
	}
	log.expect("promote", ev{"promoted", id, "scheduled", "ready", "0", "-", "-"})

	j := claimAs(t, q, "w1/0", id)
	log.expect("claim", ev{"claimed", id, "ready", "running", "1", "w1/0", "-"})

	// Heartbeats are not state changes.
	if err := q.Heartbeat(ctx, j); err != nil {
		t.Fatal(err)
	}
	log.expect("heartbeat")

	if _, dead, err := q.Fail(ctx, j, errors.New("boom")); err != nil || dead {
		t.Fatalf("Fail = %v, %v", dead, err)
	}
	log.expect("fail", ev{"failed", id, "running", "scheduled", "1", "w1/0", "boom"})

	time.Sleep(10 * time.Millisecond)
	if n, err := q.Promote(ctx, 10); err != nil || n != 1 {
		t.Fatalf("Promote = %d, %v", n, err)
	}
	j2 := claimAs(t, q, "w2/3", id)
	if err := q.Complete(ctx, j2); err != nil {
		t.Fatal(err)
	}
	log.expect("retry and succeed",
		ev{"promoted", id, "scheduled", "ready", "1", "-", "-"},
		ev{"claimed", id, "ready", "running", "2", "w2/3", "-"},
		ev{"succeeded", id, "running", "succeeded", "2", "w2/3", "-"},
	)

	// The first worker, woken up late, is fenced off: no event.
	if err := q.Complete(ctx, j); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale Complete = %v, want ErrLeaseLost", err)
	}
	log.expect("fenced complete")
}

func TestEventsForReclaimDeathReleaseCancelAndReplay(t *testing.T) {
	q, rdb, keys := newTestQueue(t)
	ctx := context.Background()
	log := &eventLog{t: t, rdb: rdb, keys: keys}

	id := mustEnqueue(t, q, Spec{Type: "work", MaxAttempts: 2})
	claimAs(t, q, "w1/0", id)
	log.seen += 2 // submitted, claimed

	// Its worker dies: the event names the worker that went silent.
	expireLease(t, rdb, keys, id)
	if n, err := q.Reap(ctx, 10); err != nil || n != 1 {
		t.Fatalf("Reap = %d, %v", n, err)
	}
	log.expect("reclaim", ev{"reclaimed", id, "running", "ready", "1", "w1/0", "lease expired"})

	// On its last attempt, the reaper buries it instead.
	claimAs(t, q, "w2/0", id)
	expireLease(t, rdb, keys, id)
	if n, err := q.Reap(ctx, 10); err != nil || n != 1 {
		t.Fatalf("Reap = %d, %v", n, err)
	}
	log.expect("reap last attempt",
		ev{"claimed", id, "ready", "running", "2", "w2/0", "-"},
		ev{"dead", id, "running", "dead", "2", "w2/0", "lease expired"},
	)

	if err := q.Replay(ctx, id); err != nil {
		t.Fatal(err)
	}
	log.expect("replay", ev{"replayed", id, "dead", "ready", "0", "-", "-"})

	// Shutdown hands it back. The event names the interrupted attempt,
	// although that attempt is then given back.
	j := claimAs(t, q, "w3/1", id)
	if err := q.Release(ctx, j); err != nil {
		t.Fatal(err)
	}
	log.expect("release",
		ev{"claimed", id, "ready", "running", "1", "w3/1", "-"},
		ev{"released", id, "running", "ready", "1", "w3/1", "-"},
	)

	// Cancelled while running: the event names the worker running it.
	claimAs(t, q, "w4/0", id)
	if err := q.Cancel(ctx, id); err != nil {
		t.Fatal(err)
	}
	log.expect("cancel running job",
		ev{"claimed", id, "ready", "running", "1", "w4/0", "-"},
		ev{"cancelled", id, "running", "cancelled", "1", "w4/0", "-"},
	)

	// A permanent failure goes straight to the dead-letter queue.
	id2 := mustEnqueue(t, q, Spec{Type: "work"})
	j2 := claimAs(t, q, "w5/0", id2)
	if _, dead, err := q.Fail(ctx, j2, job.Permanent(errors.New("bad payload"))); err != nil || !dead {
		t.Fatalf("Fail = %v, %v", dead, err)
	}
	log.expect("permanent failure",
		ev{"submitted", id2, "", "ready", "0", "-", "-"},
		ev{"claimed", id2, "ready", "running", "1", "w5/0", "-"},
		ev{"dead", id2, "running", "dead", "1", "w5/0", "bad payload"},
	)
}

func TestCronFireEmitsSubmitted(t *testing.T) {
	q, rdb, keys := newTestQueue(t)
	mustPutCron(t, q, CronSpec{Name: "c", Schedule: "*/1 * * * *", Job: Spec{Type: "work"}})
	makeDue(t, rdb, keys, "c", currentMinute(t, q))
	fires := mustFire(t, q)
	if len(fires) != 1 {
		t.Fatalf("fired %d times, want once", len(fires))
	}
	// run_at is the slot, which has passed, so the job is ready at once.
	(&eventLog{t: t, rdb: rdb, keys: keys}).expect("cron fire",
		ev{"submitted", fires[0].JobID, "", "ready", "0", "-", "-"})
}

func TestEventsStreamIsCapped(t *testing.T) {
	q, rdb, keys := newTestQueue(t)
	ctx := context.Background()
	const jobs = 10_500
	for range jobs {
		if _, _, err := q.Enqueue(ctx, Spec{Type: "work"}); err != nil {
			t.Fatal(err)
		}
	}
	n, err := rdb.XLen(ctx, keys.Events()).Result()
	if err != nil {
		t.Fatal(err)
	}
	// Trimming is approximate: whole blocks of up to 100 entries go at once.
	if n < 10_000 || n > 10_100 {
		t.Errorf("events stream holds %d entries after %d events, want about 10,000", n, jobs)
	}
}
