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
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/sham-33/job-scheduler/internal/job"
	"github.com/sham-33/job-scheduler/internal/store"
)

// IdempotencyTTL is how long an idempotency key keeps returning the job it
// first created. After that, the same key creates a new job.
const IdempotencyTTL = 24 * time.Hour

// Defaults for the Spec fields that control retries.
const (
	DefaultMaxAttempts = 5
	DefaultBackoffBase = time.Second
	DefaultBackoffMax  = time.Minute
)

// maxErrorLen caps how much of a failure message is stored on the job.
const maxErrorLen = 1000

// MaxIdempotencyKeyLen caps an idempotency key's length in bytes.
const MaxIdempotencyKeyLen = 200

var (
	ErrNotFound  = errors.New("job not found")
	ErrLeaseLost = errors.New("job is no longer leased to this worker")
	ErrCancelled = errors.New("job was cancelled")
	ErrFinished  = errors.New("job has already finished")
	ErrNotDead   = errors.New("job is not in the dead-letter queue")
	// ErrInvalid marks an error caused by bad input, such as a spec that
	// fails validation, as opposed to a failure talking to Redis. Check for
	// it with errors.Is.
	ErrInvalid = errors.New("invalid input")
)

// invalidError is bad input. Its message is the underlying error's alone.
type invalidError struct{ error }

func (e invalidError) Is(target error) bool { return target == ErrInvalid }
func (e invalidError) Unwrap() error        { return e.error }

// invalid marks err as caused by bad input.
func invalid(err error) error {
	if err == nil {
		return nil
	}
	return invalidError{err}
}

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
	//go:embed scripts/heartbeat.lua
	heartbeatSrc string
	//go:embed scripts/reap.lua
	reapSrc string
	//go:embed scripts/fail.lua
	failSrc string
	//go:embed scripts/cancel.lua
	cancelSrc string
	//go:embed scripts/replay.lua
	replaySrc string
	//go:embed scripts/release.lua
	releaseSrc string
	//go:embed scripts/purge.lua
	purgeSrc string

	enqueueScript   = newScript(enqueueSrc)
	promoteScript   = newScript(promoteSrc)
	claimScript     = newScript(claimSrc)
	completeScript  = newScript(completeSrc)
	heartbeatScript = newScript(heartbeatSrc)
	reapScript      = newScript(reapSrc)
	failScript      = newScript(failSrc)
	cancelScript    = newScript(cancelSrc)
	replayScript    = newScript(replaySrc)
	releaseScript   = newScript(releaseSrc)
	purgeScript     = newScript(purgeSrc)
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

// LeaseTTL is how long a claim or heartbeat keeps a job leased.
func (q *Queue) LeaseTTL() time.Duration {
	return q.leaseTTL
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
	// MaxAttempts is how many times the job may start before it goes to the
	// dead-letter queue. Zero means DefaultMaxAttempts.
	MaxAttempts int
	// Timeout cancels a single attempt that runs longer, which then counts
	// as a failure. Zero means no limit.
	Timeout time.Duration
	Backoff Backoff
}

// Backoff controls the wait between a failed attempt and the next one.
type Backoff struct {
	// Base is the wait before the first retry. It doubles on each further
	// retry. Zero means DefaultBackoffBase.
	Base time.Duration
	// Max caps the wait. Zero means DefaultBackoffMax, or Base if larger.
	Max time.Duration
}

// withDefaults fills in the fields the caller left at zero.
func (s Spec) withDefaults() Spec {
	if s.MaxAttempts == 0 {
		s.MaxAttempts = DefaultMaxAttempts
	}
	if s.Backoff.Base == 0 {
		s.Backoff.Base = DefaultBackoffBase
	}
	if s.Backoff.Max == 0 {
		s.Backoff.Max = max(DefaultBackoffMax, s.Backoff.Base)
	}
	return s
}

func (s Spec) validate() error {
	switch {
	case s.Type == "":
		return errors.New("job type is required")
	case s.Delay < 0:
		return fmt.Errorf("delay must not be negative, got %s", s.Delay)
	case len(s.Payload) > 0 && !json.Valid(s.Payload):
		return errors.New("payload is not valid JSON")
	case len(s.IdempotencyKey) > MaxIdempotencyKeyLen:
		return fmt.Errorf("idempotency key is %d bytes, longer than the limit of %d", len(s.IdempotencyKey), MaxIdempotencyKeyLen)
	case strings.HasPrefix(s.IdempotencyKey, cronIdemPrefix):
		return fmt.Errorf("idempotency keys starting with %q are reserved for jobs made by crons", cronIdemPrefix)
	case s.MaxAttempts < 1:
		return fmt.Errorf("max attempts must be at least 1, got %d", s.MaxAttempts)
	case s.Timeout < 0:
		return fmt.Errorf("timeout must not be negative, got %s", s.Timeout)
	case s.Backoff.Base < 0:
		return fmt.Errorf("backoff base must not be negative, got %s", s.Backoff.Base)
	case s.Backoff.Max < s.Backoff.Base:
		return fmt.Errorf("backoff max %s is shorter than backoff base %s", s.Backoff.Max, s.Backoff.Base)
	}
	return nil
}

// Enqueue creates a job and returns its ID. When the spec's idempotency key
// was already used, it returns the existing job's ID and created is false.
func (q *Queue) Enqueue(ctx context.Context, s Spec) (id string, created bool, err error) {
	s = s.withDefaults()
	if err := s.validate(); err != nil {
		return "", false, invalid(err)
	}
	// Version 7 UUIDs start with a timestamp, so IDs sort by creation time.
	uid, err := uuid.NewV7()
	if err != nil {
		return "", false, fmt.Errorf("new job id: %w", err)
	}
	id = uid.String()

	keys := []string{q.keys.Job(id), q.keys.Ready(), q.keys.Scheduled(), q.keys.Idempotency(s.IdempotencyKey), q.keys.Stats(), q.keys.Events()}
	reply, err := enqueueScript.Run(ctx, q.rdb, keys,
		id, s.Type, string(s.Payload), s.Delay.Milliseconds(), s.IdempotencyKey, int(IdempotencyTTL.Seconds()),
		s.MaxAttempts, s.Timeout.Milliseconds(), s.Backoff.Base.Milliseconds(), s.Backoff.Max.Milliseconds(),
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
	n, err := promoteScript.Run(ctx, q.rdb, []string{q.keys.Scheduled(), q.keys.Ready(), q.keys.Events()},
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
	keys := []string{q.keys.Ready(), q.keys.Leases(), q.keys.LeaseToken(), q.keys.Stats(), q.keys.Events()}
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
// fencing token is no longer the job's current one, or ErrCancelled if the
// job was cancelled while it ran.
func (q *Queue) Complete(ctx context.Context, j *job.Job) error {
	keys := []string{q.keys.Job(j.ID), q.keys.Leases(), q.keys.Stats(), q.keys.Events()}
	code, err := completeScript.Run(ctx, q.rdb, keys, j.ID, j.LeaseToken).Int()
	if err != nil {
		return fmt.Errorf("complete: %w", err)
	}
	return leaseErr(code)
}

// Fail records that an attempt at a claimed job failed with cause. The job
// is retried after retryIn, or is dead if it is out of attempts or cause is
// a job.Permanent error. Like Complete, it returns ErrLeaseLost or
// ErrCancelled if the caller no longer owns the job.
func (q *Queue) Fail(ctx context.Context, j *job.Job, cause error) (retryIn time.Duration, dead bool, err error) {
	msg := cause.Error()
	if len(msg) > maxErrorLen {
		msg = msg[:maxErrorLen]
	}
	permanent := 0
	if job.IsPermanent(cause) {
		permanent = 1
	}
	// Redis stores whole milliseconds, so report exactly what it will use.
	retryIn = retryDelay(j.BackoffBase, j.BackoffMax, j.Attempts).Truncate(time.Millisecond)

	keys := []string{q.keys.Job(j.ID), q.keys.Leases(), q.keys.Scheduled(), q.keys.DLQ(), q.keys.Stats(), q.keys.Events()}
	code, err := failScript.Run(ctx, q.rdb, keys, j.ID, j.LeaseToken, msg, retryIn.Milliseconds(), permanent).Int()
	if err != nil {
		return 0, false, fmt.Errorf("fail: %w", err)
	}
	if code == 2 {
		return 0, true, nil
	}
	return retryIn, false, leaseErr(code)
}

// Release hands a claimed job straight back to the ready set, without
// counting the attempt, for a worker that is shutting down and can't finish
// it. Like Complete, it returns ErrLeaseLost or ErrCancelled if the caller no
// longer owns the job.
func (q *Queue) Release(ctx context.Context, j *job.Job) error {
	keys := []string{q.keys.Job(j.ID), q.keys.Leases(), q.keys.Ready(), q.keys.Stats(), q.keys.Events()}
	code, err := releaseScript.Run(ctx, q.rdb, keys, j.ID, j.LeaseToken).Int()
	if err != nil {
		return fmt.Errorf("release: %w", err)
	}
	return leaseErr(code)
}

// Heartbeat renews the lease on a claimed job. It returns ErrLeaseLost if j's
// fencing token is no longer the job's current one, which means the worker
// must stop because another worker may already be running the job. It
// returns ErrCancelled if the job was cancelled.
func (q *Queue) Heartbeat(ctx context.Context, j *job.Job) error {
	keys := []string{q.keys.Job(j.ID), q.keys.Leases(), q.keys.Stats()}
	code, err := heartbeatScript.Run(ctx, q.rdb, keys, j.ID, j.LeaseToken, q.leaseTTL.Milliseconds()).Int()
	if err != nil {
		return fmt.Errorf("heartbeat: %w", err)
	}
	return leaseErr(code)
}

// leaseErr turns the reply of a script that checks the caller's lease into
// an error.
func leaseErr(code int) error {
	switch code {
	case 0:
		return ErrLeaseLost
	case -1:
		return ErrCancelled
	}
	return nil
}

// Reap takes back up to limit jobs whose leases expired and returns how
// many. Each goes back to the ready set, or to the dead-letter queue if it
// has used up its attempts.
func (q *Queue) Reap(ctx context.Context, limit int) (int, error) {
	keys := []string{q.keys.Leases(), q.keys.Ready(), q.keys.DLQ(), q.keys.Stats(), q.keys.Events()}
	n, err := reapScript.Run(ctx, q.rdb, keys, q.keys.JobPrefix(), limit).Int()
	if err != nil {
		return 0, fmt.Errorf("reap: %w", err)
	}
	return n, nil
}

// Cancel stops a job that hasn't finished from running, or from running
// again. If it is running right now, its worker stops it at the next
// heartbeat. It returns ErrFinished for a job that already succeeded, died
// or was cancelled.
func (q *Queue) Cancel(ctx context.Context, id string) error {
	keys := []string{q.keys.Job(id), q.keys.Scheduled(), q.keys.Ready(), q.keys.Leases(), q.keys.Stats(), q.keys.Events()}
	code, err := cancelScript.Run(ctx, q.rdb, keys, id).Int()
	switch {
	case err != nil:
		return fmt.Errorf("cancel: %w", err)
	case code == -1:
		return ErrNotFound
	case code == 0:
		return ErrFinished
	}
	return nil
}

// Replay moves a job from the dead-letter queue back to the ready set with
// its attempts reset. It returns ErrNotDead for a job in any other state.
func (q *Queue) Replay(ctx context.Context, id string) error {
	keys := []string{q.keys.Job(id), q.keys.DLQ(), q.keys.Ready(), q.keys.Stats(), q.keys.Events()}
	code, err := replayScript.Run(ctx, q.rdb, keys, id).Int()
	switch {
	case err != nil:
		return fmt.Errorf("replay: %w", err)
	case code == -1:
		return ErrNotFound
	case code == 0:
		return ErrNotDead
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

// ListDLQ returns up to limit jobs from the dead-letter queue, the most
// recent death first, skipping the first offset. total is how many jobs the
// queue holds.
func (q *Queue) ListDLQ(ctx context.Context, offset, limit int) (jobs []*job.Job, total int64, err error) {
	if offset < 0 || limit < 1 {
		return nil, 0, invalid(fmt.Errorf("offset must not be negative and limit must be at least 1, got %d and %d", offset, limit))
	}
	p := q.rdb.Pipeline()
	ids := p.ZRevRange(ctx, q.keys.DLQ(), int64(offset), int64(offset+limit-1))
	count := p.ZCard(ctx, q.keys.DLQ())
	if _, err := p.Exec(ctx); err != nil {
		return nil, 0, fmt.Errorf("list dead-letter queue: %w", err)
	}
	jobs = make([]*job.Job, 0, len(ids.Val()))
	if len(ids.Val()) == 0 {
		return jobs, count.Val(), nil
	}
	p = q.rdb.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(ids.Val()))
	for i, id := range ids.Val() {
		cmds[i] = p.HGetAll(ctx, q.keys.Job(id))
	}
	if _, err := p.Exec(ctx); err != nil {
		return nil, 0, fmt.Errorf("list dead-letter queue: %w", err)
	}
	for _, cmd := range cmds {
		if len(cmd.Val()) == 0 {
			continue // purged since the IDs were read
		}
		j, err := job.FromHash(cmd.Val())
		if err != nil {
			return nil, 0, err
		}
		jobs = append(jobs, j)
	}
	return jobs, count.Val(), nil
}

// PurgeDLQ deletes every job in the dead-letter queue for good, with its
// idempotency key, and returns how many it deleted. It works in batches, so
// a job that dies while it runs may or may not be deleted.
func (q *Queue) PurgeDLQ(ctx context.Context) (int, error) {
	total := 0
	for {
		reply, err := purgeScript.Run(ctx, q.rdb, []string{q.keys.DLQ(), q.keys.Stats(), q.keys.Events()},
			q.keys.JobPrefix(), purgeBatch, q.keys.Idempotency("")).Int64Slice()
		if err != nil {
			return total, fmt.Errorf("purge dead-letter queue: %w", err)
		}
		if len(reply) != 2 {
			return total, fmt.Errorf("purge dead-letter queue: unexpected reply %v", reply)
		}
		total += int(reply[0])
		if reply[1] < purgeBatch {
			return total, nil
		}
	}
}

// purgeBatch is how many jobs one purge.lua call looks at, which keeps each
// call short.
const purgeBatch = 500

// pairsToMap turns Redis's flat [field, value, field, value, …] reply into a map.
func pairsToMap(pairs []string) map[string]string {
	m := make(map[string]string, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		m[pairs[i]] = pairs[i+1]
	}
	return m
}
