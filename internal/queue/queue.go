// Package queue is the job queue. Every change to a job's state is one Lua
// script that Redis runs atomically, so concurrent workers and schedulers
// never see, or make, a half-finished update.
package queue

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/sham-33/job-scheduler/internal/job"
	"github.com/sham-33/job-scheduler/internal/store"
)

// IdempotencyTTL is how long an idempotency key keeps returning the job it
// first created. After that, the same key creates a new job.
const IdempotencyTTL = 24 * time.Hour

var (
	ErrNotFound  = errors.New("job not found")
	ErrLeaseLost = errors.New("job is no longer leased to this worker")
)

var (
	//go:embed scripts/lib.lua
	libSrc string
	//go:embed scripts/enqueue.lua
	enqueueSrc string
	//go:embed scripts/promote.lua
	promoteSrc string
	//go:embed scripts/claim.lua
	claimSrc string
	//go:embed scripts/complete.lua
	completeSrc string

	enqueueScript  = newScript(enqueueSrc)
	promoteScript  = newScript(promoteSrc)
	claimScript    = newScript(claimSrc)
	completeScript = newScript(completeSrc)
)

// newScript prepends the shared helpers. go-redis sends the script's SHA
// first (EVALSHA) and only uploads the full source when Redis doesn't have it.
func newScript(src string) *redis.Script {
	return redis.NewScript(libSrc + "\n" + src)
}

// Queue reads and changes jobs in Redis.
type Queue struct {
	rdb      *redis.Client
	keys     store.Keys
	leaseTTL time.Duration
}

// New returns a Queue whose claims lease a job for leaseTTL.
func New(rdb *redis.Client, keys store.Keys, leaseTTL time.Duration) *Queue {
	return &Queue{rdb: rdb, keys: keys, leaseTTL: leaseTTL}
}

// Spec describes a job to enqueue.
type Spec struct {
	Type    string
	Payload json.RawMessage
	// Delay postpones the job; zero means run as soon as a worker is free.
	// It is measured on Redis's clock.
	Delay time.Duration
	// IdempotencyKey, if set, makes repeated submissions return the first
	// job instead of creating duplicates (for example when a client retries).
	IdempotencyKey string
}

func (s Spec) validate() error {
	switch {
	case s.Type == "":
		return errors.New("job type is required")
	case s.Delay < 0:
		return fmt.Errorf("delay must not be negative, got %s", s.Delay)
	case len(s.Payload) > 0 && !json.Valid(s.Payload):
		return errors.New("payload is not valid JSON")
	}
	return nil
}

// Enqueue creates a job and returns its ID. When the spec's idempotency key
// was already used, it returns the existing job's ID and created is false.
func (q *Queue) Enqueue(ctx context.Context, s Spec) (id string, created bool, err error) {
	if err := s.validate(); err != nil {
		return "", false, err
	}
	// Version 7 UUIDs start with a timestamp, so IDs sort by creation time.
	uid, err := uuid.NewV7()
	if err != nil {
		return "", false, fmt.Errorf("new job id: %w", err)
	}
	id = uid.String()

	keys := []string{q.keys.Job(id), q.keys.Ready(), q.keys.Scheduled(), q.keys.Idempotency(s.IdempotencyKey), q.keys.Stats()}
	reply, err := enqueueScript.Run(ctx, q.rdb, keys,
		id, s.Type, string(s.Payload), s.Delay.Milliseconds(), s.IdempotencyKey, int(IdempotencyTTL.Seconds()),
	).Slice()
	if err != nil {
		return "", false, fmt.Errorf("enqueue: %w", err)
	}
	if len(reply) != 2 {
		return "", false, fmt.Errorf("enqueue: unexpected reply %v", reply)
	}
	gotID, ok1 := reply[0].(string)
	flag, ok2 := reply[1].(int64)
	if !ok1 || !ok2 {
		return "", false, fmt.Errorf("enqueue: unexpected reply %v", reply)
	}
	return gotID, flag == 1, nil
}

// Promote moves up to limit due jobs from scheduled to ready and returns how
// many it moved.
func (q *Queue) Promote(ctx context.Context, limit int) (int, error) {
	n, err := promoteScript.Run(ctx, q.rdb, []string{q.keys.Scheduled(), q.keys.Ready()},
		q.keys.JobPrefix(), limit,
	).Int()
	if err != nil {
		return 0, fmt.Errorf("promote: %w", err)
	}
	return n, nil
}

// Claim leases the longest-waiting ready job to workerID. It returns
// (nil, nil) when no job is ready.
func (q *Queue) Claim(ctx context.Context, workerID string) (*job.Job, error) {
	keys := []string{q.keys.Ready(), q.keys.Leases(), q.keys.LeaseToken(), q.keys.Stats()}
	fields, err := claimScript.Run(ctx, q.rdb, keys,
		q.keys.JobPrefix(), workerID, q.leaseTTL.Milliseconds(),
	).StringSlice()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	return job.FromHash(pairsToMap(fields))
}

// Complete marks a claimed job as succeeded. It returns ErrLeaseLost if j's
// fencing token is no longer the job's current one.
func (q *Queue) Complete(ctx context.Context, j *job.Job) error {
	keys := []string{q.keys.Job(j.ID), q.keys.Leases(), q.keys.Stats()}
	ok, err := completeScript.Run(ctx, q.rdb, keys, j.ID, j.LeaseToken).Int()
	if err != nil {
		return fmt.Errorf("complete: %w", err)
	}
	if ok == 0 {
		return ErrLeaseLost
	}
	return nil
}

// Get returns the job with the given ID, or ErrNotFound.
func (q *Queue) Get(ctx context.Context, id string) (*job.Job, error) {
	fields, err := q.rdb.HGetAll(ctx, q.keys.Job(id)).Result()
	if err != nil {
		return nil, fmt.Errorf("get job %s: %w", id, err)
	}
	if len(fields) == 0 {
		return nil, ErrNotFound
	}
	return job.FromHash(fields)
}

// pairsToMap turns Redis's flat [field, value, field, value, …] reply into a map.
func pairsToMap(pairs []string) map[string]string {
	m := make(map[string]string, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		m[pairs[i]] = pairs[i+1]
	}
	return m
}
