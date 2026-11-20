// Package monitor reports what the cluster is doing: job counters, queue
// sizes, the leading scheduler, live workers, and the stream of job events.
// It only reads, apart from the worker registry that workers write to
// (see Registry). The API and dashboard are built on it.
package monitor

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/sham-33/job-scheduler/internal/store"
)

// Event names, one per kind of job state change. Each comes from one Lua
// script in internal/queue/scripts.
const (
	EventSubmitted = "submitted" // created: '' → ready or scheduled
	EventPromoted  = "promoted"  // fell due: scheduled → ready
	EventClaimed   = "claimed"   // a worker took it: ready → running
	EventSucceeded = "succeeded" // running → succeeded
	EventFailed    = "failed"    // will be retried: running → scheduled
	EventDead      = "dead"      // out of attempts or failed for good: running → dead
	EventReclaimed = "reclaimed" // its worker stopped renewing the lease: running → ready
	EventReleased  = "released"  // its worker shut down: running → ready
	EventCancelled = "cancelled" // any unfinished status → cancelled
	EventReplayed  = "replayed"  // dead → ready
)

// Event is one job state change.
type Event struct {
	// ID is the stream entry's ID. It orders events, and EventsAfter takes
	// it to continue from where an earlier read stopped.
	ID      string
	At      time.Time // when it happened, on Redis's clock
	Name    string    // one of the Event* constants
	JobID   string
	JobType string
	From    string // status before; "" for a new job
	To      string // status after
	Attempt int    // the job's attempt count; for a change out of running, the attempt that ended
	// Worker is the lease owner ("<process>/<slot>") for a change into or
	// out of running. For a reclaimed job it is the worker that went silent.
	Worker string
	Error  string // why it failed, died or was reclaimed
}

// Snapshot is the state of the whole cluster at one moment.
type Snapshot struct {
	At time.Time // Redis's clock
	// Counters counts what has happened since Redis was last emptied, by
	// name: submitted, claimed, succeeded, retried, dead, reclaimed,
	// released, fenced, cancelled, replayed, cron_fired. A counter that has
	// never moved is missing.
	Counters map[string]int64
	Queues   QueueSizes
	// Leader is the ID of the leading scheduler, or "" if none leads.
	Leader string
	// Epoch is the number of the latest leadership term.
	Epoch   int64
	Workers []Worker
}

// QueueSizes counts the jobs in each queue.
type QueueSizes struct {
	Scheduled int64 // waiting for their run_at, including retries
	Ready     int64 // due, waiting for a worker
	Running   int64 // leased to a worker
	Dead      int64 // in the dead-letter queue
}

// Worker is one worker process as it last reported itself.
type Worker struct {
	ID          string
	Concurrency int
	Busy        int
	Draining    bool
	StartedAt   time.Time
	LastSeen    time.Time
	// Alive is false once the worker has gone DeadAfter without checking in.
	Alive bool
}

// Monitor reads the cluster's state from Redis.
type Monitor struct {
	rdb  *redis.Client
	keys store.Keys
}

// New returns a Monitor.
func New(rdb *redis.Client, keys store.Keys) *Monitor {
	return &Monitor{rdb: rdb, keys: keys}
}

// Snapshot reads counters, queue sizes, the leader and the workers. The
// reads go in one round trip but not one atomic step, so for a busy cluster
// the numbers can disagree slightly with each other.
func (m *Monitor) Snapshot(ctx context.Context) (Snapshot, error) {
	p := m.rdb.Pipeline()
	now := p.Time(ctx)
	counters := p.HGetAll(ctx, m.keys.Stats())
	scheduled := p.ZCard(ctx, m.keys.Scheduled())
	ready := p.ZCard(ctx, m.keys.Ready())
	running := p.ZCard(ctx, m.keys.Leases())
	dead := p.ZCard(ctx, m.keys.DLQ())
	leader := p.MGet(ctx, m.keys.Leader(), m.keys.Epoch())
	seen := p.ZRangeWithScores(ctx, m.keys.Workers(), 0, -1)
	if _, err := p.Exec(ctx); err != nil {
		return Snapshot{}, fmt.Errorf("snapshot: %w", err)
	}

	s := Snapshot{
		At:       now.Val().Truncate(time.Millisecond),
		Counters: make(map[string]int64, len(counters.Val())),
		Queues: QueueSizes{
			Scheduled: scheduled.Val(), Ready: ready.Val(),
			Running: running.Val(), Dead: dead.Val(),
		},
	}
	for name, v := range counters.Val() {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return Snapshot{}, fmt.Errorf("snapshot: counter %s is %q, not a number", name, v)
		}
		s.Counters[name] = n
	}
	vals := leader.Val()
	s.Leader, _ = vals[0].(string)
	if v, ok := vals[1].(string); ok {
		epoch, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return Snapshot{}, fmt.Errorf("snapshot: epoch %q is not a number", v)
		}
		s.Epoch = epoch
	}
	workers, err := m.workers(ctx, s.At, seen.Val())
	if err != nil {
		return Snapshot{}, err
	}
	s.Workers = workers
	return s, nil
}

// Workers lists the worker processes that checked in during the last hour,
// live ones first, then by ID.
func (m *Monitor) Workers(ctx context.Context) ([]Worker, error) {
	p := m.rdb.Pipeline()
	now := p.Time(ctx)
	seen := p.ZRangeWithScores(ctx, m.keys.Workers(), 0, -1)
	if _, err := p.Exec(ctx); err != nil {
		return nil, fmt.Errorf("list workers: %w", err)
	}
	return m.workers(ctx, now.Val().Truncate(time.Millisecond), seen.Val())
}

func (m *Monitor) workers(ctx context.Context, now time.Time, seen []redis.Z) ([]Worker, error) {
	if len(seen) == 0 {
		return []Worker{}, nil
	}
	p := m.rdb.Pipeline()
	details := make([]*redis.MapStringStringCmd, len(seen))
	for i, z := range seen {
		details[i] = p.HGetAll(ctx, m.keys.Worker(z.Member.(string)))
	}
	if _, err := p.Exec(ctx); err != nil {
		return nil, fmt.Errorf("list workers: %w", err)
	}

	workers := make([]Worker, len(seen))
	for i, z := range seen {
		// The hash and the set are read in separate round trips, so a
		// check-in can land in between. last_seen comes from the hash, which
		// that check-in wrote in the same step as the other fields, so they
		// always agree. The set's score is only a fallback, for a hash that
		// expired first.
		h := details[i].Val()
		last := time.UnixMilli(int64(z.Score))
		if ms := atoi(h["last_seen"]); ms > 0 {
			last = time.UnixMilli(int64(ms))
		}
		w := Worker{
			ID:          z.Member.(string),
			Concurrency: atoi(h["concurrency"]),
			Busy:        atoi(h["busy"]),
			Draining:    h["draining"] == "1",
			LastSeen:    last,
			Alive:       now.Sub(last) <= DeadAfter,
		}
		if ms := atoi(h["started_at"]); ms > 0 {
			w.StartedAt = time.UnixMilli(int64(ms))
		}
		workers[i] = w
	}
	sort.SliceStable(workers, func(a, b int) bool {
		if workers[a].Alive != workers[b].Alive {
			return workers[a].Alive
		}
		return workers[a].ID < workers[b].ID
	})
	return workers, nil
}

// RecentEvents returns up to n of the latest events, newest first.
func (m *Monitor) RecentEvents(ctx context.Context, n int) ([]Event, error) {
	msgs, err := m.rdb.XRevRangeN(ctx, m.keys.Events(), "+", "-", int64(n)).Result()
	if err != nil {
		return nil, fmt.Errorf("recent events: %w", err)
	}
	return toEvents(msgs)
}

// EventsAfter returns up to n events that came after the event with ID
// after, oldest first. An empty after starts from the oldest event kept.
// Passing the last ID it returned reads on from there, so a caller can
// follow the stream without missing or repeating events, as long as it
// keeps up with the stream's cap of about 10,000 events.
func (m *Monitor) EventsAfter(ctx context.Context, after string, n int) ([]Event, error) {
	start := "-"
	if after != "" {
		start = "(" + after
	}
	msgs, err := m.rdb.XRangeN(ctx, m.keys.Events(), start, "+", int64(n)).Result()
	if err != nil {
		return nil, fmt.Errorf("events after %q: %w", after, err)
	}
	return toEvents(msgs)
}

func toEvents(msgs []redis.XMessage) ([]Event, error) {
	events := make([]Event, len(msgs))
	for i, msg := range msgs {
		e, err := toEvent(msg)
		if err != nil {
			return nil, err
		}
		events[i] = e
	}
	return events, nil
}

func toEvent(msg redis.XMessage) (Event, error) {
	ms, _, ok := strings.Cut(msg.ID, "-")
	at, err := strconv.ParseInt(ms, 10, 64)
	if !ok || err != nil {
		return Event{}, fmt.Errorf("event %q: unexpected ID", msg.ID)
	}
	str := func(k string) string { s, _ := msg.Values[k].(string); return s }
	e := Event{
		ID:      msg.ID,
		At:      time.UnixMilli(at),
		Name:    str("event"),
		JobID:   str("job_id"),
		JobType: str("job_type"),
		From:    str("from"),
		To:      str("to"),
		Attempt: atoi(str("attempt")),
		Worker:  str("worker"),
		Error:   str("error"),
	}
	if e.Name == "" || e.JobID == "" {
		return Event{}, errors.New("event " + msg.ID + ": missing its name or job ID")
	}
	return e, nil
}

// atoi reads a number that a script wrote, treating a missing one as 0.
func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}
