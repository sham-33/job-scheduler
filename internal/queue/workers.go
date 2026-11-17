package queue

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// ForgetWorkersAfter is how long a worker that stopped beating, because it
// crashed, stays in the registry (shown as dead) before another worker's beat
// removes it. A worker that shuts down cleanly removes itself at once.
const ForgetWorkersAfter = 10 * time.Minute

var (
	//go:embed scripts/worker_beat.lua
	workerBeatSrc    string
	workerBeatScript = newScript(workerBeatSrc)
)

// WorkerBeat is what a worker process reports about itself on each beat.
type WorkerBeat struct {
	ID          string
	Concurrency int
	// Running is how many jobs the process is running right now.
	Running int
	// First marks the process's first beat, which sets its start time.
	First bool
}

// BeatWorker records that a worker process is alive, on Redis's clock, and
// forgets workers unseen for ForgetWorkersAfter. It returns how many it
// forgot.
func (q *Queue) BeatWorker(ctx context.Context, b WorkerBeat) (forgotten int, err error) {
	first := 0
	if b.First {
		first = 1
	}
	n, err := workerBeatScript.Run(ctx, q.rdb, []string{q.keys.Workers(), q.keys.Worker(b.ID)},
		b.ID, b.Concurrency, b.Running, first, ForgetWorkersAfter.Milliseconds(), q.keys.WorkerPrefix(),
	).Int()
	if err != nil {
		return 0, fmt.Errorf("worker beat: %w", err)
	}
	return n, nil
}

// RemoveWorker takes a worker process out of the registry, for one that is
// shutting down cleanly.
func (q *Queue) RemoveWorker(ctx context.Context, id string) error {
	_, err := q.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.ZRem(ctx, q.keys.Workers(), id)
		p.Del(ctx, q.keys.Worker(id))
		return nil
	})
	if err != nil {
		return fmt.Errorf("remove worker %s: %w", id, err)
	}
	return nil
}
