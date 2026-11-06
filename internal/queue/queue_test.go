package queue

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/sham-33/job-scheduler/internal/job"
	"github.com/sham-33/job-scheduler/internal/store"
	"github.com/sham-33/job-scheduler/internal/store/storetest"
)

const testLeaseTTL = 2 * time.Second

func newTestQueue(t *testing.T) (*Queue, *redis.Client, store.Keys) {
	t.Helper()
	rdb := storetest.Client(t)
	keys := storetest.Keys(t, rdb)
	return New(rdb, keys, testLeaseTTL), rdb, keys
}

func mustEnqueue(t *testing.T, q *Queue, s Spec) string {
	t.Helper()
	id, created, err := q.Enqueue(context.Background(), s)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if !created {
		t.Fatalf("Enqueue: created = false, want true")
	}
	return id
}

func mustGet(t *testing.T, q *Queue, id string) *job.Job {
	t.Helper()
	j, err := q.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get(%s): %v", id, err)
	}
	return j
}

// inZSet reports whether id is in the sorted set and returns its score.
func inZSet(t *testing.T, rdb *redis.Client, key, id string) (float64, bool) {
	t.Helper()
	score, err := rdb.ZScore(context.Background(), key, id).Result()
	if errors.Is(err, redis.Nil) {
		return 0, false
	}
	if err != nil {
		t.Fatalf("ZSCORE %s %s: %v", key, id, err)
	}
	return score, true
}

func stat(t *testing.T, rdb *redis.Client, keys store.Keys, name string) int {
	t.Helper()
	n, err := rdb.HGet(context.Background(), keys.Stats(), name).Int()
	if err != nil && !errors.Is(err, redis.Nil) {
		t.Fatalf("HGET stats %s: %v", name, err)
	}
	return n
}

func TestEnqueueImmediate(t *testing.T) {
	q, rdb, keys := newTestQueue(t)

	id := mustEnqueue(t, q, Spec{Type: "echo", Payload: json.RawMessage(`{"msg":"hi"}`)})

	j := mustGet(t, q, id)
	if j.Status != job.StatusReady || j.Type != "echo" || string(j.Payload) != `{"msg":"hi"}` || j.Attempts != 0 {
		t.Errorf("job = %+v, want a ready echo job with 0 attempts", j)
	}
	if !j.RunAt.Equal(j.CreatedAt) {
		t.Errorf("RunAt = %v, want CreatedAt %v for an immediate job", j.RunAt, j.CreatedAt)
	}
	if _, ok := inZSet(t, rdb, keys.Ready(), id); !ok {
		t.Error("job is not in the ready set")
	}
	if _, ok := inZSet(t, rdb, keys.Scheduled(), id); ok {
		t.Error("immediate job is in the scheduled set")
	}
	if got := stat(t, rdb, keys, "submitted"); got != 1 {
		t.Errorf("stats.submitted = %d, want 1", got)
	}
}

func TestEnqueueDelayed(t *testing.T) {
	q, rdb, keys := newTestQueue(t)

	id := mustEnqueue(t, q, Spec{Type: "echo", Delay: 5 * time.Second})

	j := mustGet(t, q, id)
	if j.Status != job.StatusScheduled {
		t.Errorf("Status = %s, want scheduled", j.Status)
	}
	if got := j.RunAt.Sub(j.CreatedAt); got != 5*time.Second {
		t.Errorf("RunAt - CreatedAt = %s, want 5s", got)
	}
	score, ok := inZSet(t, rdb, keys.Scheduled(), id)
	if !ok {
		t.Fatal("job is not in the scheduled set")
	}
	if int64(score) != j.RunAt.UnixMilli() {
		t.Errorf("scheduled score = %v, want run_at %d", score, j.RunAt.UnixMilli())
	}
	if _, ok := inZSet(t, rdb, keys.Ready(), id); ok {
		t.Error("delayed job is already in the ready set")
	}
}

func TestEnqueueIdempotent(t *testing.T) {
	q, rdb, keys := newTestQueue(t)
	ctx := context.Background()
	spec := Spec{Type: "echo", IdempotencyKey: "order-42"}

	first := mustEnqueue(t, q, spec)
	second, created, err := q.Enqueue(ctx, spec)
	if err != nil {
		t.Fatalf("second Enqueue: %v", err)
	}
	if created || second != first {
		t.Errorf("second Enqueue = (%s, created=%v), want (%s, created=false)", second, created, first)
	}
	if n := rdb.ZCard(ctx, keys.Ready()).Val(); n != 1 {
		t.Errorf("ready set has %d jobs, want 1", n)
	}
	if ttl := rdb.TTL(ctx, keys.Idempotency("order-42")).Val(); ttl <= 0 || ttl > IdempotencyTTL {
		t.Errorf("idempotency key TTL = %s, want (0, %s]", ttl, IdempotencyTTL)
	}

	other := mustEnqueue(t, q, Spec{Type: "echo", IdempotencyKey: "order-43"})
	if other == first {
		t.Error("a different idempotency key returned the same job")
	}
	if got := stat(t, rdb, keys, "submitted"); got != 2 {
		t.Errorf("stats.submitted = %d, want 2 (duplicates are not counted)", got)
	}
}

func TestEnqueueValidation(t *testing.T) {
	q, rdb, keys := newTestQueue(t)
	tests := map[string]Spec{
		"missing type":   {},
		"negative delay": {Type: "echo", Delay: -time.Second},
		"invalid JSON":   {Type: "echo", Payload: json.RawMessage(`{not json`)},
	}
	for name, spec := range tests {
		t.Run(name, func(t *testing.T) {
			if _, _, err := q.Enqueue(context.Background(), spec); err == nil {
				t.Error("Enqueue succeeded, want error")
			}
		})
	}
	if got := stat(t, rdb, keys, "submitted"); got != 0 {
		t.Errorf("stats.submitted = %d, want 0 after only invalid specs", got)
	}
}

func TestPromote(t *testing.T) {
	q, rdb, keys := newTestQueue(t)
	ctx := context.Background()
	id := mustEnqueue(t, q, Spec{Type: "echo", Delay: 200 * time.Millisecond})

	if n, err := q.Promote(ctx, 100); err != nil || n != 0 {
		t.Fatalf("Promote before due = (%d, %v), want (0, nil)", n, err)
	}

	time.Sleep(250 * time.Millisecond)
	if n, err := q.Promote(ctx, 100); err != nil || n != 1 {
		t.Fatalf("Promote after due = (%d, %v), want (1, nil)", n, err)
	}
	j := mustGet(t, q, id)
	if j.Status != job.StatusReady {
		t.Errorf("Status = %s, want ready", j.Status)
	}
	score, ok := inZSet(t, rdb, keys.Ready(), id)
	if !ok {
		t.Fatal("promoted job is not in the ready set")
	}
	if int64(score) != j.RunAt.UnixMilli() {
		t.Errorf("ready score = %v, want run_at %d", score, j.RunAt.UnixMilli())
	}
	if _, ok := inZSet(t, rdb, keys.Scheduled(), id); ok {
		t.Error("promoted job is still in the scheduled set")
	}
}

func TestPromoteRespectsLimit(t *testing.T) {
	q, _, _ := newTestQueue(t)
	ctx := context.Background()
	for range 5 {
		mustEnqueue(t, q, Spec{Type: "echo", Delay: time.Millisecond})
	}
	time.Sleep(20 * time.Millisecond)

	if n, _ := q.Promote(ctx, 3); n != 3 {
		t.Errorf("first Promote moved %d, want 3", n)
	}
	if n, _ := q.Promote(ctx, 3); n != 2 {
		t.Errorf("second Promote moved %d, want 2", n)
	}
}

func TestPromoteDropsJobsThatLeftScheduled(t *testing.T) {
	q, rdb, keys := newTestQueue(t)
	ctx := context.Background()
	id := mustEnqueue(t, q, Spec{Type: "echo", Delay: time.Millisecond})
	rdb.HSet(ctx, keys.Job(id), "status", string(job.StatusCancelled))
	time.Sleep(20 * time.Millisecond)

	if n, _ := q.Promote(ctx, 100); n != 0 {
		t.Errorf("Promote moved %d, want 0 for a cancelled job", n)
	}
	if _, ok := inZSet(t, rdb, keys.Scheduled(), id); ok {
		t.Error("cancelled job is still in the scheduled set")
	}
	if got := mustGet(t, q, id).Status; got != job.StatusCancelled {
		t.Errorf("Status = %s, want cancelled to be left alone", got)
	}
}

func TestClaim(t *testing.T) {
	q, rdb, keys := newTestQueue(t)
	ctx := context.Background()
	id := mustEnqueue(t, q, Spec{Type: "echo"})

	j, err := q.Claim(ctx, "worker-1")
	if err != nil || j == nil {
		t.Fatalf("Claim = (%v, %v), want a job", j, err)
	}
	if j.ID != id || j.Status != job.StatusRunning || j.Attempts != 1 || j.LeaseOwner != "worker-1" || j.LeaseToken <= 0 {
		t.Errorf("claimed job = %+v, want running, attempt 1, owner worker-1, a token", j)
	}
	if j.StartedAt.IsZero() {
		t.Error("StartedAt is not set")
	}
	expiry, ok := inZSet(t, rdb, keys.Leases(), id)
	if !ok {
		t.Fatal("claimed job has no lease")
	}
	if got := time.UnixMilli(int64(expiry)).Sub(j.StartedAt); got != testLeaseTTL {
		t.Errorf("lease expiry - StartedAt = %s, want %s", got, testLeaseTTL)
	}
	if _, ok := inZSet(t, rdb, keys.Ready(), id); ok {
		t.Error("claimed job is still in the ready set")
	}
	if got := stat(t, rdb, keys, "claimed"); got != 1 {
		t.Errorf("stats.claimed = %d, want 1", got)
	}

	if again, err := q.Claim(ctx, "worker-2"); err != nil || again != nil {
		t.Errorf("second Claim = (%v, %v), want (nil, nil) on an empty queue", again, err)
	}
}

func TestClaimOrderAndTokens(t *testing.T) {
	q, _, _ := newTestQueue(t)
	ctx := context.Background()
	first := mustEnqueue(t, q, Spec{Type: "echo"})
	time.Sleep(5 * time.Millisecond)
	second := mustEnqueue(t, q, Spec{Type: "echo"})

	a, _ := q.Claim(ctx, "w")
	b, _ := q.Claim(ctx, "w")
	if a == nil || b == nil {
		t.Fatal("expected two claims")
	}
	if a.ID != first || b.ID != second {
		t.Errorf("claim order = %s, %s, want the oldest first: %s, %s", a.ID, b.ID, first, second)
	}
	if b.LeaseToken <= a.LeaseToken {
		t.Errorf("tokens = %d then %d, want strictly increasing", a.LeaseToken, b.LeaseToken)
	}
}

func TestClaimSkipsJobsNoLongerReady(t *testing.T) {
	q, rdb, keys := newTestQueue(t)
	ctx := context.Background()
	stale := mustEnqueue(t, q, Spec{Type: "echo"})
	rdb.HSet(ctx, keys.Job(stale), "status", string(job.StatusCancelled))
	time.Sleep(5 * time.Millisecond)
	live := mustEnqueue(t, q, Spec{Type: "echo"})

	j, err := q.Claim(ctx, "w")
	if err != nil || j == nil || j.ID != live {
		t.Fatalf("Claim = (%v, %v), want the live job %s", j, err, live)
	}
	if got := mustGet(t, q, stale); got.Status != job.StatusCancelled || got.Attempts != 0 {
		t.Errorf("stale job = %+v, want it untouched", got)
	}
}

func TestComplete(t *testing.T) {
	q, rdb, keys := newTestQueue(t)
	ctx := context.Background()
	id := mustEnqueue(t, q, Spec{Type: "echo"})
	j, _ := q.Claim(ctx, "w")

	if err := q.Complete(ctx, j); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	done := mustGet(t, q, id)
	if done.Status != job.StatusSucceeded || done.FinishedAt.IsZero() {
		t.Errorf("job = %+v, want succeeded with FinishedAt set", done)
	}
	if _, ok := inZSet(t, rdb, keys.Leases(), id); ok {
		t.Error("lease was not removed")
	}
	if got := stat(t, rdb, keys, "succeeded"); got != 1 {
		t.Errorf("stats.succeeded = %d, want 1", got)
	}

	if err := q.Complete(ctx, j); !errors.Is(err, ErrLeaseLost) {
		t.Errorf("second Complete = %v, want ErrLeaseLost", err)
	}
}

func TestCompleteRejectsStaleToken(t *testing.T) {
	q, rdb, keys := newTestQueue(t)
	ctx := context.Background()
	id := mustEnqueue(t, q, Spec{Type: "echo"})
	j, _ := q.Claim(ctx, "w")

	// Simulate another worker taking the job over with a newer token.
	rdb.HSet(ctx, keys.Job(id), "lease_token", j.LeaseToken+1)

	if err := q.Complete(ctx, j); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Complete with stale token = %v, want ErrLeaseLost", err)
	}
	if got := mustGet(t, q, id).Status; got != job.StatusRunning {
		t.Errorf("Status = %s, want running (unchanged)", got)
	}
	if got := stat(t, rdb, keys, "succeeded"); got != 0 {
		t.Errorf("stats.succeeded = %d, want 0", got)
	}
}

func TestGetNotFound(t *testing.T) {
	q, _, _ := newTestQueue(t)
	if _, err := q.Get(context.Background(), "nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get = %v, want ErrNotFound", err)
	}
}
