package queue

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/sham-33/job-scheduler/internal/job"
)

// mustKill runs a new job into the dead-letter queue and returns its ID.
func mustKill(t *testing.T, q *Queue, s Spec) string {
	t.Helper()
	s.Type = "work"
	id := mustEnqueue(t, q, s)
	j := claimAs(t, q, "w/0", id)
	if _, dead, err := q.Fail(context.Background(), j, job.Permanent(errors.New("boom "+id))); err != nil || !dead {
		t.Fatalf("Fail = %v, %v; want dead", dead, err)
	}
	return id
}

func TestListDLQ(t *testing.T) {
	q, _, _ := newTestQueue(t)
	ctx := context.Background()
	var ids []string
	for range 3 {
		ids = append(ids, mustKill(t, q, Spec{}))
		time.Sleep(2 * time.Millisecond) // distinct times of death
	}

	jobs, total, err := q.ListDLQ(ctx, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if total != 3 || len(jobs) != 3 {
		t.Fatalf("ListDLQ = %d jobs of %d, want 3 of 3", len(jobs), total)
	}
	for i, j := range jobs {
		want := ids[2-i] // most recent death first
		if j.ID != want || j.Status != job.StatusDead || j.LastError != "boom "+want {
			t.Errorf("job %d = %s %s %q, want %s dead", i, j.ID, j.Status, j.LastError, want)
		}
	}

	jobs, total, err = q.ListDLQ(ctx, 1, 1)
	if err != nil || total != 3 || len(jobs) != 1 || jobs[0].ID != ids[1] {
		t.Errorf("ListDLQ(1, 1) = %v of %d, %v; want only %s", jobs, total, err, ids[1])
	}
	jobs, _, err = q.ListDLQ(ctx, 5, 10)
	if err != nil || len(jobs) != 0 {
		t.Errorf("ListDLQ past the end = %v, %v; want none", jobs, err)
	}
	for _, bad := range [][2]int{{-1, 10}, {0, 0}} {
		if _, _, err := q.ListDLQ(ctx, bad[0], bad[1]); !errors.Is(err, ErrInvalid) {
			t.Errorf("ListDLQ(%d, %d) error = %v, want ErrInvalid", bad[0], bad[1], err)
		}
	}
}

func TestPurgeDLQ(t *testing.T) {
	q, rdb, keys := newTestQueue(t)
	ctx := context.Background()
	keyed := mustKill(t, q, Spec{IdempotencyKey: "order-1"})
	plain := mustKill(t, q, Spec{})
	// A ready job whose ID is in the dead-letter set by mistake is only
	// dropped from the set, never deleted.
	alive := mustEnqueue(t, q, Spec{Type: "work"})
	if err := rdb.ZAdd(ctx, keys.DLQ(), redis.Z{Score: 1, Member: alive}).Err(); err != nil {
		t.Fatal(err)
	}
	log := &eventLog{t: t, rdb: rdb, keys: keys}
	log.seen = len(readEvents(t, rdb, keys))

	n, err := q.PurgeDLQ(ctx)
	if err != nil || n != 2 {
		t.Fatalf("PurgeDLQ = %d, %v; want 2", n, err)
	}
	for _, id := range []string{keyed, plain} {
		if _, err := q.Get(ctx, id); !errors.Is(err, ErrNotFound) {
			t.Errorf("Get(%s) after purge = %v, want ErrNotFound", id, err)
		}
	}
	if j := mustGet(t, q, alive); j.Status != job.StatusReady {
		t.Errorf("ready job is %s after purge, want ready", j.Status)
	}
	if n := rdb.ZCard(ctx, keys.DLQ()).Val(); n != 0 {
		t.Errorf("dead-letter queue holds %d after purge, want 0", n)
	}
	if got := stat(t, rdb, keys, "purged"); got != 2 {
		t.Errorf("stats.purged = %d, want 2", got)
	}
	// The events name what was deleted, oldest death first.
	log.expect("purge",
		ev{"purged", keyed, "dead", "", "1", "-", "-"},
		ev{"purged", plain, "dead", "", "1", "-", "-"},
	)

	// The key was freed, so the same key makes a new job.
	id, created, err := q.Enqueue(ctx, Spec{Type: "work", IdempotencyKey: "order-1"})
	if err != nil || !created || id == keyed {
		t.Errorf("Enqueue with a purged job's key = %s, %v, %v; want a new job", id, created, err)
	}
	if n, err := q.PurgeDLQ(ctx); err != nil || n != 0 {
		t.Errorf("PurgeDLQ on an empty queue = %d, %v; want 0", n, err)
	}
}

func TestPurgeDLQInBatches(t *testing.T) {
	q, rdb, keys := newTestQueue(t)
	ctx := context.Background()
	const jobs = 2*purgeBatch + 100
	// Faster than failing each job: mark them dead directly.
	p := rdb.Pipeline()
	for range jobs {
		id, _, err := q.Enqueue(ctx, Spec{Type: "work"})
		if err != nil {
			t.Fatal(err)
		}
		p.ZRem(ctx, keys.Ready(), id)
		p.HSet(ctx, keys.Job(id), "status", "dead")
		p.ZAdd(ctx, keys.DLQ(), redis.Z{Score: 1, Member: id})
	}
	if _, err := p.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if n, err := q.PurgeDLQ(ctx); err != nil || n != jobs {
		t.Fatalf("PurgeDLQ = %d, %v; want %d", n, err, jobs)
	}
	if n := rdb.ZCard(ctx, keys.DLQ()).Val(); n != 0 {
		t.Errorf("dead-letter queue holds %d after purge, want 0", n)
	}
}

func TestBadInputIsErrInvalid(t *testing.T) {
	q, _, _ := newTestQueue(t)
	ctx := context.Background()
	if _, _, err := q.Enqueue(ctx, Spec{}); !errors.Is(err, ErrInvalid) || err.Error() != "job type is required" {
		t.Errorf("Enqueue with no type: %v, want ErrInvalid with the validation message", err)
	}
	long := strings.Repeat("k", MaxIdempotencyKeyLen+1)
	if _, _, err := q.Enqueue(ctx, Spec{Type: "work", IdempotencyKey: long}); !errors.Is(err, ErrInvalid) {
		t.Errorf("Enqueue with a %d-byte key: %v, want ErrInvalid", len(long), err)
	}
	if _, _, err := q.Enqueue(ctx, Spec{Type: "work", IdempotencyKey: long[:MaxIdempotencyKeyLen]}); err != nil {
		t.Errorf("Enqueue with a key of exactly the limit: %v", err)
	}
	for name, s := range map[string]CronSpec{
		"bad name":     {Name: "no spaces", Schedule: "@hourly", Job: Spec{Type: "work"}},
		"bad schedule": {Name: "c", Schedule: "61 * * * *", Job: Spec{Type: "work"}},
		"delay":        {Name: "c", Schedule: "@hourly", Job: Spec{Type: "work", Delay: time.Second}},
		"no type":      {Name: "c", Schedule: "@hourly"},
		"never fires":  {Name: "c", Schedule: "0 0 30 2 *", Job: Spec{Type: "work"}},
	} {
		if _, err := q.PutCron(ctx, s); !errors.Is(err, ErrInvalid) {
			t.Errorf("PutCron with %s: %v, want ErrInvalid", name, err)
		}
	}

	// A failure to reach Redis is not the caller's fault.
	broken := New(redis.NewClient(&redis.Options{Addr: "localhost:1", MaxRetries: -1}), q.keys, time.Second)
	if _, _, err := broken.Enqueue(ctx, Spec{Type: "work"}); err == nil || errors.Is(err, ErrInvalid) {
		t.Errorf("Enqueue with Redis unreachable: %v, want an error that is not ErrInvalid", err)
	}
}
