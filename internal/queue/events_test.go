package queue

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/sham-33/job-scheduler/internal/store"
)

// event is a stream entry without its timestamp.
type event struct{ event, job, typ, from, to, worker string }

func (e event) String() string {
	return fmt.Sprintf("%s %s(%s) %q→%q by %q", e.event, e.job, e.typ, e.from, e.to, e.worker)
}

// streamEvents returns every entry in the events stream, oldest first, and
// checks that each has a timestamp no earlier than the one before it.
func streamEvents(t *testing.T, rdb *redis.Client, keys store.Keys) []event {
	t.Helper()
	msgs, err := rdb.XRange(context.Background(), keys.Events(), "-", "+").Result()
	if err != nil {
		t.Fatalf("XRANGE events: %v", err)
	}
	var out []event
	var last int64
	for _, m := range msgs {
		v := func(k string) string { s, _ := m.Values[k].(string); return s }
		ts, err := strconv.ParseInt(v("ts"), 10, 64)
		if err != nil || ts < last {
			t.Errorf("event %s has ts %q after %d", m.ID, v("ts"), last)
		}
		last = ts
		out = append(out, event{v("event"), v("job_id"), v("type"), v("from"), v("to"), v("worker")})
	}
	return out
}

func TestEveryJobChangeIsRecorded(t *testing.T) {
	ctx := context.Background()
	const lease = 100 * time.Millisecond
	q, rdb, keys := leaseTestQueue(t, lease)
	fast := Backoff{Base: time.Millisecond, Max: time.Millisecond}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	reap := func(want int) {
		t.Helper()
		time.Sleep(lease + 10*time.Millisecond)
		if n, err := q.Reap(ctx, 10); err != nil || n != want {
			t.Fatalf("Reap = %d, %v; want %d", n, err, want)
		}
	}

	// a runs and succeeds. Repeating its submission, or its result, changes
	// nothing, so records nothing.
	a := mustEnqueue(t, q, Spec{Type: "echo", IdempotencyKey: "a"})
	if _, created, err := q.Enqueue(ctx, Spec{Type: "echo", IdempotencyKey: "a"}); err != nil || created {
		t.Fatalf("duplicate Enqueue = %v, %v", created, err)
	}
	ja := mustClaim(t, q, "w/0")
	must(q.Complete(ctx, ja))
	if err := q.Complete(ctx, ja); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("second Complete = %v, want ErrLeaseLost", err)
	}

	// b is delayed, fails, is retried, dies, is replayed and is cancelled
	// before it runs again.
	b := mustEnqueue(t, q, Spec{Type: "flaky", Delay: 20 * time.Millisecond, MaxAttempts: 2, Backoff: fast})
	time.Sleep(25 * time.Millisecond)
	if n, err := q.Promote(ctx, 10); err != nil || n != 1 {
		t.Fatalf("Promote = %d, %v", n, err)
	}
	jb := mustClaim(t, q, "w/1")
	if _, dead, err := q.Fail(ctx, jb, errors.New("boom")); err != nil || dead {
		t.Fatalf("first Fail = dead %v, %v", dead, err)
	}
	jb = claimAgain(t, q, time.Millisecond) // claimed by "w1"
	if _, dead, err := q.Fail(ctx, jb, errors.New("boom")); err != nil || !dead {
		t.Fatalf("second Fail = dead %v, %v", dead, err)
	}
	must(q.Replay(ctx, b))
	must(q.Cancel(ctx, b))

	// c is released by a worker shutting down, then cancelled while running.
	c := mustEnqueue(t, q, Spec{Type: "echo"})
	must(q.Release(ctx, mustClaim(t, q, "w/2")))
	mustClaim(t, q, "w/3")
	must(q.Cancel(ctx, c))

	// d's worker dies, so it is reclaimed. Its next worker dies too, on its
	// last attempt, so it is dead.
	d := mustEnqueue(t, q, Spec{Type: "echo", MaxAttempts: 2})
	mustClaim(t, q, "w/4")
	reap(1)
	mustClaim(t, q, "w/5")
	reap(1)

	// e is cancelled while it waits for its run_at.
	e := mustEnqueue(t, q, Spec{Type: "echo", Delay: time.Hour})
	must(q.Cancel(ctx, e))

	// f is made by a cron.
	mustPutCron(t, q, CronSpec{Name: "hourly", Schedule: "@every 1h", Job: Spec{Type: "echo"}})
	makeDue(t, rdb, keys, "hourly", mustRedisNow(t, q).Truncate(time.Second))
	f := mustFire(t, q)[0].JobID

	want := []event{
		{"submitted", a, "echo", "", "ready", ""},
		{"claimed", a, "echo", "ready", "running", "w/0"},
		{"succeeded", a, "echo", "running", "succeeded", "w/0"},

		{"submitted", b, "flaky", "", "scheduled", ""},
		{"promoted", b, "flaky", "scheduled", "ready", ""},
		{"claimed", b, "flaky", "ready", "running", "w/1"},
		{"retried", b, "flaky", "running", "scheduled", "w/1"},
		{"promoted", b, "flaky", "scheduled", "ready", ""},
		{"claimed", b, "flaky", "ready", "running", "w1"},
		{"dead", b, "flaky", "running", "dead", "w1"},
		{"replayed", b, "flaky", "dead", "ready", ""},
		{"cancelled", b, "flaky", "ready", "cancelled", ""},

		{"submitted", c, "echo", "", "ready", ""},
		{"claimed", c, "echo", "ready", "running", "w/2"},
		{"released", c, "echo", "running", "ready", "w/2"},
		{"claimed", c, "echo", "ready", "running", "w/3"},
		{"cancelled", c, "echo", "running", "cancelled", "w/3"},

		{"submitted", d, "echo", "", "ready", ""},
		{"claimed", d, "echo", "ready", "running", "w/4"},
		{"reclaimed", d, "echo", "running", "ready", "w/4"},
		{"claimed", d, "echo", "ready", "running", "w/5"},
		{"dead", d, "echo", "running", "dead", "w/5"},

		{"submitted", e, "echo", "", "scheduled", ""},
		{"cancelled", e, "echo", "scheduled", "cancelled", ""},

		{"submitted", f, "echo", "", "ready", ""},
	}
	got := streamEvents(t, rdb, keys)
	for i := range max(len(got), len(want)) {
		var g, w event
		if i < len(got) {
			g = got[i]
		}
		if i < len(want) {
			w = want[i]
		}
		if g != w {
			t.Errorf("event %d:\n got  %v\n want %v", i, g, w)
		}
	}

	// Every event is counted under its own name, and only events are.
	counts := map[string]int{}
	for _, e := range got {
		counts[e.event]++
	}
	for _, name := range []string{"submitted", "promoted", "claimed", "succeeded", "retried",
		"dead", "reclaimed", "released", "cancelled", "replayed"} {
		if s := stat(t, rdb, keys, name); s != counts[name] {
			t.Errorf("stats.%s = %d, but the stream has %d %s events", name, s, counts[name], name)
		}
	}
	if s := stat(t, rdb, keys, "fenced"); s != 1 {
		t.Errorf("stats.fenced = %d, want 1 for the repeated Complete", s)
	}
}

func TestEventsStreamIsCapped(t *testing.T) {
	ctx := context.Background()
	q, rdb, keys := newTestQueue(t)
	const jobs = 12_000
	oldest := mustEnqueue(t, q, Spec{Type: "echo"})
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for w := range 8 {
		wg.Go(func() {
			for range jobs / 8 {
				if _, _, err := q.Enqueue(ctx, Spec{Type: "echo"}); err != nil {
					errs <- fmt.Errorf("worker %d: %w", w, err)
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	n, err := rdb.XLen(ctx, keys.Events()).Result()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("stream holds %d events after %d", n, jobs+1)
	// MAXLEN ~ trims in whole blocks of up to 100 entries, never below the cap.
	if n < 10_000 || n > 10_100 {
		t.Errorf("stream holds %d events, want about 10,000", n)
	}
	if s := stat(t, rdb, keys, "submitted"); s != jobs+1 {
		t.Errorf("stats.submitted = %d, want %d: counters must not be capped", s, jobs+1)
	}
	// The oldest entries are the ones dropped.
	for _, e := range streamEvents(t, rdb, keys) {
		if e.job == oldest {
			t.Fatalf("the first job's event %v is still in the stream", e)
		}
	}
}
