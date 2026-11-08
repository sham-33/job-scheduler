package queue

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
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

// leaseTestQueue returns a queue with a short lease, for tests that wait for
// one to expire.
func leaseTestQueue(t *testing.T, lease time.Duration) (*Queue, *redis.Client, store.Keys) {
	t.Helper()
	rdb := storetest.Client(t)
	keys := storetest.Keys(t, rdb)
	return New(rdb, keys, lease), rdb, keys
}

func TestHeartbeatExtendsLease(t *testing.T) {
	q, rdb, keys := newTestQueue(t)
	ctx := context.Background()
	id := mustEnqueue(t, q, Spec{Type: "echo"})
	j, _ := q.Claim(ctx, "w")
	before, _ := inZSet(t, rdb, keys.Leases(), id)

	time.Sleep(20 * time.Millisecond)
	if err := q.Heartbeat(ctx, j); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	after, ok := inZSet(t, rdb, keys.Leases(), id)
	if !ok {
		t.Fatal("lease disappeared")
	}
	if after-before < 20 {
		t.Errorf("lease moved by %vms, want at least 20ms", after-before)
	}
}

func TestHeartbeatRejectsStaleToken(t *testing.T) {
	q, rdb, keys := newTestQueue(t)
	ctx := context.Background()
	id := mustEnqueue(t, q, Spec{Type: "echo"})
	j, _ := q.Claim(ctx, "w")
	before, _ := inZSet(t, rdb, keys.Leases(), id)
	rdb.HSet(ctx, keys.Job(id), "lease_token", j.LeaseToken+1)

	time.Sleep(5 * time.Millisecond)
	if err := q.Heartbeat(ctx, j); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Heartbeat with stale token = %v, want ErrLeaseLost", err)
	}
	if after, _ := inZSet(t, rdb, keys.Leases(), id); after != before {
		t.Errorf("lease moved from %v to %v, want it untouched", before, after)
	}
	if got := stat(t, rdb, keys, "fenced"); got != 1 {
		t.Errorf("stats.fenced = %d, want 1", got)
	}
}

func TestHeartbeatRejectedOnceFinished(t *testing.T) {
	q, rdb, keys := newTestQueue(t)
	ctx := context.Background()
	id := mustEnqueue(t, q, Spec{Type: "echo"})
	j, _ := q.Claim(ctx, "w")
	if err := q.Complete(ctx, j); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	if err := q.Heartbeat(ctx, j); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Heartbeat after Complete = %v, want ErrLeaseLost", err)
	}
	if _, ok := inZSet(t, rdb, keys.Leases(), id); ok {
		t.Error("heartbeat brought a finished job's lease back")
	}
}

func TestReapReclaimsExpiredLease(t *testing.T) {
	const lease = 50 * time.Millisecond
	q, rdb, keys := leaseTestQueue(t, lease)
	ctx := context.Background()
	id := mustEnqueue(t, q, Spec{Type: "echo"})
	dead, _ := q.Claim(ctx, "dead-worker")

	if n, err := q.Reap(ctx, 100); err != nil || n != 0 {
		t.Fatalf("Reap before expiry = (%d, %v), want (0, nil)", n, err)
	}

	time.Sleep(lease + 30*time.Millisecond)
	if n, err := q.Reap(ctx, 100); err != nil || n != 1 {
		t.Fatalf("Reap after expiry = (%d, %v), want (1, nil)", n, err)
	}
	j := mustGet(t, q, id)
	if j.Status != job.StatusReady || j.Attempts != 1 || j.LastError != "lease expired" {
		t.Errorf("job = %+v, want ready, 1 attempt so far, last error 'lease expired'", j)
	}
	if _, ok := inZSet(t, rdb, keys.Leases(), id); ok {
		t.Error("expired lease was not removed")
	}
	if got := stat(t, rdb, keys, "reclaimed"); got != 1 {
		t.Errorf("stats.reclaimed = %d, want 1", got)
	}
	if n, _ := q.Reap(ctx, 100); n != 0 {
		t.Errorf("second Reap reclaimed %d, want 0", n)
	}

	next, err := q.Claim(ctx, "new-worker")
	if err != nil || next == nil || next.ID != id {
		t.Fatalf("Claim after Reap = (%v, %v), want job %s", next, err, id)
	}
	if next.Attempts != 2 || next.LeaseOwner != "new-worker" || next.LeaseToken <= dead.LeaseToken {
		t.Errorf("re-claimed job = %+v, want attempt 2, new owner, a token above %d", next, dead.LeaseToken)
	}
}

func TestReapSparesRenewedLease(t *testing.T) {
	const lease = 150 * time.Millisecond
	q, _, _ := leaseTestQueue(t, lease)
	ctx := context.Background()
	id := mustEnqueue(t, q, Spec{Type: "echo"})
	j, _ := q.Claim(ctx, "w")

	// Stay alive well past the original expiry by renewing in time.
	for range 4 {
		time.Sleep(lease / 3)
		if err := q.Heartbeat(ctx, j); err != nil {
			t.Fatalf("Heartbeat: %v", err)
		}
		if n, _ := q.Reap(ctx, 100); n != 0 {
			t.Fatalf("Reap took a job whose lease was being renewed")
		}
	}
	if got := mustGet(t, q, id).Status; got != job.StatusRunning {
		t.Errorf("Status = %s, want running", got)
	}
}

func TestReapRespectsLimit(t *testing.T) {
	const lease = 30 * time.Millisecond
	q, _, _ := leaseTestQueue(t, lease)
	ctx := context.Background()
	for range 5 {
		mustEnqueue(t, q, Spec{Type: "echo"})
		q.Claim(ctx, "w")
	}
	time.Sleep(lease + 30*time.Millisecond)

	if n, _ := q.Reap(ctx, 3); n != 3 {
		t.Errorf("first Reap reclaimed %d, want 3", n)
	}
	if n, _ := q.Reap(ctx, 3); n != 2 {
		t.Errorf("second Reap reclaimed %d, want 2", n)
	}
}

func TestReapedJobGoesAheadOfNewerWork(t *testing.T) {
	const lease = 30 * time.Millisecond
	q, _, _ := leaseTestQueue(t, lease)
	ctx := context.Background()
	old := mustEnqueue(t, q, Spec{Type: "echo"})
	q.Claim(ctx, "dead-worker")
	time.Sleep(lease + 30*time.Millisecond)
	newer := mustEnqueue(t, q, Spec{Type: "echo"})
	if n, _ := q.Reap(ctx, 100); n != 1 {
		t.Fatalf("Reap reclaimed %d, want 1", n)
	}

	first, _ := q.Claim(ctx, "w")
	if first == nil || first.ID != old {
		t.Fatalf("first claim = %+v, want the reclaimed job %s ahead of %s", first, old, newer)
	}
}

func TestReapOnlyDropsLeaseOfJobNoLongerRunning(t *testing.T) {
	const lease = 30 * time.Millisecond
	q, rdb, keys := leaseTestQueue(t, lease)
	ctx := context.Background()
	id := mustEnqueue(t, q, Spec{Type: "echo"})
	q.Claim(ctx, "w")
	rdb.HSet(ctx, keys.Job(id), "status", string(job.StatusCancelled))
	time.Sleep(lease + 30*time.Millisecond)

	if n, _ := q.Reap(ctx, 100); n != 0 {
		t.Errorf("Reap reclaimed %d, want 0 for a job that isn't running", n)
	}
	if _, ok := inZSet(t, rdb, keys.Leases(), id); ok {
		t.Error("stale lease was not removed")
	}
	if got := mustGet(t, q, id).Status; got != job.StatusCancelled {
		t.Errorf("Status = %s, want cancelled to be left alone", got)
	}
}

// Several schedulers can briefly run at once during a failover, so reaping
// has to stay correct when it races with itself.
func TestConcurrentReapersReclaimEachJobOnce(t *testing.T) {
	const (
		lease   = 30 * time.Millisecond
		jobs    = 200
		reapers = 10
	)
	q, rdb, keys := leaseTestQueue(t, lease)
	ctx := context.Background()
	for range jobs {
		mustEnqueue(t, q, Spec{Type: "echo"})
		q.Claim(ctx, "w")
	}
	time.Sleep(lease + 30*time.Millisecond)

	var total atomic.Int64
	var wg sync.WaitGroup
	for range reapers {
		wg.Go(func() {
			n, err := q.Reap(ctx, 7)
			for err == nil && n > 0 {
				total.Add(int64(n))
				n, err = q.Reap(ctx, 7)
			}
			if err != nil {
				t.Errorf("Reap: %v", err)
			}
		})
	}
	wg.Wait()

	if total.Load() != jobs {
		t.Errorf("reapers reclaimed %d jobs in total, want %d", total.Load(), jobs)
	}
	if got := stat(t, rdb, keys, "reclaimed"); got != jobs {
		t.Errorf("stats.reclaimed = %d, want %d", got, jobs)
	}
	if n := rdb.ZCard(ctx, keys.Ready()).Val(); n != jobs {
		t.Errorf("ready set has %d jobs, want %d", n, jobs)
	}
}

// A worker that freezes, loses its job and later wakes up must not be able
// to renew the lease or record a result.
func TestZombieCannotFinishAfterTakeover(t *testing.T) {
	const lease = 30 * time.Millisecond
	q, rdb, keys := leaseTestQueue(t, lease)
	ctx := context.Background()
	id := mustEnqueue(t, q, Spec{Type: "echo"})
	zombie, _ := q.Claim(ctx, "zombie")

	time.Sleep(lease + 30*time.Millisecond)
	if n, _ := q.Reap(ctx, 100); n != 1 {
		t.Fatalf("Reap reclaimed %d, want 1", n)
	}

	// Waking up before anyone has taken the job over.
	if err := q.Complete(ctx, zombie); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Complete on a reclaimed job = %v, want ErrLeaseLost", err)
	}
	if got := mustGet(t, q, id).Status; got != job.StatusReady {
		t.Fatalf("Status = %s, want ready", got)
	}

	// Waking up after another worker took the job over.
	fresh, _ := q.Claim(ctx, "fresh")
	if err := q.Heartbeat(ctx, zombie); !errors.Is(err, ErrLeaseLost) {
		t.Errorf("zombie Heartbeat = %v, want ErrLeaseLost", err)
	}
	if err := q.Complete(ctx, zombie); !errors.Is(err, ErrLeaseLost) {
		t.Errorf("zombie Complete = %v, want ErrLeaseLost", err)
	}
	j := mustGet(t, q, id)
	if j.Status != job.StatusRunning || j.LeaseOwner != "fresh" {
		t.Errorf("job = %+v, want it still running under fresh", j)
	}

	if err := q.Heartbeat(ctx, fresh); err != nil {
		t.Errorf("fresh Heartbeat: %v", err)
	}
	if err := q.Complete(ctx, fresh); err != nil {
		t.Errorf("fresh Complete: %v", err)
	}
	if got := stat(t, rdb, keys, "succeeded"); got != 1 {
		t.Errorf("stats.succeeded = %d, want 1", got)
	}
	if got := stat(t, rdb, keys, "fenced"); got != 3 {
		t.Errorf("stats.fenced = %d, want 3 (the zombie's three rejected calls)", got)
	}
}
