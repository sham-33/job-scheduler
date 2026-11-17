// Package events reads what the system is doing: the stats counters, the
// stream of job events, queue depths, the worker registry and the current
// leader. It only reads. The queue's Lua scripts write the counters and
// events in the same atomic step as each job change, workers keep the
// registry up to date, and the election keeps the leader key.
package events

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/sham-33/job-scheduler/internal/store"
)

// DeadAfter is how long a worker can go unseen before it counts as dead.
// Workers beat every second (worker.DefaultBeatEvery), so this allows for a
// few missed beats, such as a slow Redis round trip or a GC pause.
const DeadAfter = 5 * time.Second

// Event is one change to a job's status.
type Event struct {
	// ID is the stream entry's ID. IDs increase with time, so they order
	// events and can be used to read on from where a reader left off.
	ID string `json:"id"`
	// Event names the change: submitted, promoted, claimed, succeeded,
	// retried, dead, reclaimed, released, cancelled or replayed. Each has a
	// counter of the same name in Stats.
	Event string `json:"event"`
	JobID string `json:"job_id"`
	// Type is the job's type, such as "echo".
	Type string `json:"type"`
	// From and To are the job's status before and after. From is empty for
	// a new job.
	From string `json:"from,omitempty"`
	To   string `json:"to"`
	// Worker is the lease owner ("<worker id>/<slot>") involved: the one
	// that claimed, finished or released the job, or for a reclaimed job,
	// the one whose lease expired. Empty when no worker was involved.
	Worker string `json:"worker,omitempty"`
	// At is when it happened, on Redis's clock.
	At time.Time `json:"at"`
}

// Worker is one worker process in the registry.
type Worker struct {
	ID string `json:"id"`
	// Alive is false once the worker has gone unseen for more than
	// DeadAfter. A worker that shuts down cleanly leaves the registry
	// instead, so a dead one most likely crashed.
	Alive       bool      `json:"alive"`
	LastSeen    time.Time `json:"last_seen"`
	StartedAt   time.Time `json:"started_at,omitzero"`
	Concurrency int       `json:"concurrency"`
	// Running is how many jobs it was running at its last beat.
	Running int `json:"running"`
}

// Leader is the scheduler that leads, if any.
type Leader struct {
	// ID is the leading scheduler's candidate ID, or empty when no scheduler
	// leads right now.
	ID string `json:"id,omitempty"`
	// Epoch is the number of the latest leadership term.
	Epoch int64 `json:"epoch"`
}

// Depths counts the jobs in each queue.
type Depths struct {
	Scheduled int64 `json:"scheduled"` // waiting for run_at, or a retry
	Ready     int64 `json:"ready"`     // due, waiting for a worker
	Running   int64 `json:"running"`   // leased to a worker
	Dead      int64 `json:"dead"`      // in the dead-letter queue
}

// Snapshot is everything a reader shows, as of one moment.
type Snapshot struct {
	// At is Redis's clock when the snapshot was taken.
	At     time.Time        `json:"at"`
	Stats  map[string]int64 `json:"stats"`
	Depths Depths           `json:"depths"`
	Leader Leader           `json:"leader"`
	// Workers is sorted by ID.
	Workers []Worker `json:"workers"`
	// Events holds the most recent events, newest first.
	Events []Event `json:"events"`
}

// Reader reads the system's state from Redis.
type Reader struct {
	rdb  *redis.Client
	keys store.Keys
}

// NewReader returns a Reader for the keys under keys' prefix.
func NewReader(rdb *redis.Client, keys store.Keys) *Reader {
	return &Reader{rdb: rdb, keys: keys}
}

// Snapshot reads everything at once. The counters, depths, leader, worker
// list and events are read in one MULTI/EXEC, so they agree with each other:
// no job change lands between reading one and the next. recent is how many
// of the latest events to include.
func (r *Reader) Snapshot(ctx context.Context, recent int) (*Snapshot, error) {
	pipe := r.rdb.TxPipeline()
	now := pipe.Time(ctx)
	stats := r.queueStats(ctx, pipe)
	depths := r.queueDepths(ctx, pipe)
	leader := r.queueLeader(ctx, pipe)
	workers := r.queueWorkers(ctx, pipe)
	events := r.queueEvents(ctx, pipe, recent)
	// A missing key, such as the leader's when no scheduler leads, shows up
	// as redis.Nil. Each part handles its own.
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("snapshot: %w", err)
	}

	s := &Snapshot{}
	var errs [6]error
	s.At, errs[0] = now.Result()
	s.Stats, errs[1] = stats()
	s.Depths, errs[2] = depths()
	s.Leader, errs[3] = leader()
	s.Workers, errs[4] = workers()
	s.Events, errs[5] = events()
	if err := errors.Join(errs[:]...); err != nil {
		return nil, fmt.Errorf("snapshot: %w", err)
	}
	s.At = s.At.Truncate(time.Millisecond)
	return s, nil
}

// Stats returns every counter in the stats hash.
func (r *Reader) Stats(ctx context.Context) (map[string]int64, error) {
	return read(ctx, r.rdb, r.queueStats)
}

// Depths returns how many jobs are in each queue.
func (r *Reader) Depths(ctx context.Context) (Depths, error) {
	return read(ctx, r.rdb, r.queueDepths)
}

// Leader returns the leading scheduler, if any.
func (r *Reader) Leader(ctx context.Context) (Leader, error) {
	return read(ctx, r.rdb, r.queueLeader)
}

// Workers returns every worker process in the registry, alive or dead,
// sorted by ID.
func (r *Reader) Workers(ctx context.Context) ([]Worker, error) {
	return read(ctx, r.rdb, r.queueWorkers)
}

// Recent returns up to n of the latest events, newest first.
func (r *Reader) Recent(ctx context.Context, n int) ([]Event, error) {
	return read(ctx, r.rdb, func(ctx context.Context, p redis.Pipeliner) func() ([]Event, error) {
		return r.queueEvents(ctx, p, n)
	})
}

// Each queueX method below adds its reads to a pipeline and returns a
// function that parses their results once the pipeline has run. Snapshot
// runs all of them in one transaction, and each single-part method runs one.

// read runs one part on its own.
func read[T any](ctx context.Context, rdb *redis.Client, part func(context.Context, redis.Pipeliner) func() (T, error)) (T, error) {
	pipe := rdb.TxPipeline()
	result := part(ctx, pipe)
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		var zero T
		return zero, err
	}
	return result()
}

func (r *Reader) queueStats(ctx context.Context, p redis.Pipeliner) func() (map[string]int64, error) {
	cmd := p.HGetAll(ctx, r.keys.Stats())
	return func() (map[string]int64, error) {
		raw, err := cmd.Result()
		if err != nil {
			return nil, fmt.Errorf("stats: %w", err)
		}
		stats := make(map[string]int64, len(raw))
		for name, v := range raw {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("stats: counter %s is %q, not an integer", name, v)
			}
			stats[name] = n
		}
		return stats, nil
	}
}

func (r *Reader) queueDepths(ctx context.Context, p redis.Pipeliner) func() (Depths, error) {
	scheduled := p.ZCard(ctx, r.keys.Scheduled())
	ready := p.ZCard(ctx, r.keys.Ready())
	running := p.ZCard(ctx, r.keys.Leases())
	dead := p.ZCard(ctx, r.keys.DLQ())
	return func() (Depths, error) {
		d := Depths{
			Scheduled: scheduled.Val(),
			Ready:     ready.Val(),
			Running:   running.Val(),
			Dead:      dead.Val(),
		}
		if err := errors.Join(scheduled.Err(), ready.Err(), running.Err(), dead.Err()); err != nil {
			return Depths{}, fmt.Errorf("queue depths: %w", err)
		}
		return d, nil
	}
}

func (r *Reader) queueLeader(ctx context.Context, p redis.Pipeliner) func() (Leader, error) {
	id := p.Get(ctx, r.keys.Leader())
	epoch := p.Get(ctx, r.keys.Epoch())
	return func() (Leader, error) {
		var l Leader
		var err error
		if l.ID, err = id.Result(); err != nil && !errors.Is(err, redis.Nil) {
			return Leader{}, fmt.Errorf("leader: %w", err)
		}
		switch s, err := epoch.Result(); {
		case errors.Is(err, redis.Nil):
			// No scheduler has ever led.
		case err != nil:
			return Leader{}, fmt.Errorf("leader epoch: %w", err)
		default:
			if l.Epoch, err = strconv.ParseInt(s, 10, 64); err != nil {
				return Leader{}, fmt.Errorf("leader epoch %q is not an integer", s)
			}
		}
		return l, nil
	}
}

// queueWorkers reads the registry. Each worker's hash is read in a second
// round trip, since the IDs aren't known until the first one returns.
func (r *Reader) queueWorkers(ctx context.Context, p redis.Pipeliner) func() ([]Worker, error) {
	now := p.Time(ctx)
	seen := p.ZRangeWithScores(ctx, r.keys.Workers(), 0, -1)
	return func() ([]Worker, error) {
		if err := errors.Join(now.Err(), seen.Err()); err != nil {
			return nil, fmt.Errorf("workers: %w", err)
		}
		entries := seen.Val()
		if len(entries) == 0 {
			return []Worker{}, nil
		}
		hashes := make([]*redis.MapStringStringCmd, len(entries))
		_, err := r.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
			for i, e := range entries {
				hashes[i] = p.HGetAll(ctx, r.keys.Worker(e.Member.(string)))
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("workers: %w", err)
		}

		workers := make([]Worker, len(entries))
		for i, e := range entries {
			h := hashes[i].Val()
			lastSeen := time.UnixMilli(int64(e.Score))
			workers[i] = Worker{
				ID:          e.Member.(string),
				Alive:       now.Val().Sub(lastSeen) <= DeadAfter,
				LastSeen:    lastSeen,
				StartedAt:   millis(h["started_at"]),
				Concurrency: atoi(h["concurrency"]),
				Running:     atoi(h["running"]),
			}
		}
		slices.SortFunc(workers, func(a, b Worker) int { return cmp.Compare(a.ID, b.ID) })
		return workers, nil
	}
}

func (r *Reader) queueEvents(ctx context.Context, p redis.Pipeliner, n int) func() ([]Event, error) {
	if n <= 0 {
		return func() ([]Event, error) { return []Event{}, nil }
	}
	cmd := p.XRevRangeN(ctx, r.keys.Events(), "+", "-", int64(n))
	return func() ([]Event, error) {
		msgs, err := cmd.Result()
		if err != nil {
			return nil, fmt.Errorf("events: %w", err)
		}
		events := make([]Event, len(msgs))
		for i, m := range msgs {
			events[i] = fromMessage(m)
		}
		return events, nil
	}
}

func fromMessage(m redis.XMessage) Event {
	field := func(name string) string {
		s, _ := m.Values[name].(string)
		return s
	}
	return Event{
		ID:     m.ID,
		Event:  field("event"),
		JobID:  field("job_id"),
		Type:   field("type"),
		From:   field("from"),
		To:     field("to"),
		Worker: field("worker"),
		At:     millis(field("ts")),
	}
}

// millis reads a Unix-milliseconds field, or returns the zero time if it is
// missing or malformed.
func millis(s string) time.Time {
	ms, err := strconv.ParseInt(s, 10, 64)
	if err != nil || ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms)
}

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}
