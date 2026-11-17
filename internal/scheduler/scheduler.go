// Package scheduler holds the housekeeping loops that keep jobs moving:
// releasing delayed jobs when they fall due, reclaiming jobs from dead
// workers, and firing crons. The scheduler service runs them only while it
// is the elected leader (see package election).
package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/sham-33/job-scheduler/internal/queue"
)

// Lead runs every housekeeping loop until ctx is cancelled, which happens
// when this scheduler stops being the leader.
func Lead(ctx context.Context, q *queue.Queue, reapEvery time.Duration, logger *slog.Logger) {
	var wg sync.WaitGroup
	wg.Go(func() { RunPromoter(ctx, q, logger) })
	wg.Go(func() { RunReaper(ctx, q, reapEvery, logger) })
	wg.Go(func() { RunCron(ctx, q, logger) })
	wg.Wait()
}

// batchSize is the most jobs one step of a loop handles at a time.
const batchSize = 1000

// loop calls step on a timer. It is safe to run several copies at once,
// because every step is one atomic Lua script.
type loop struct {
	every time.Duration
	step  func(ctx context.Context, limit int) (int, error)
	name  string
	level slog.Level // how visible a successful step should be
	moved string     // logged, with a count, when a step moved jobs
}

func (l loop) run(ctx context.Context, logger *slog.Logger) {
	ticker := time.NewTicker(l.every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		// Keep going while full batches come back, so a burst of work doesn't
		// trickle out one batch per tick.
		for {
			n, err := l.step(ctx, batchSize)
			if err != nil {
				if ctx.Err() == nil {
					logger.Error(l.name+" failed", "err", err)
				}
				break
			}
			if n > 0 {
				logger.Log(ctx, l.level, l.moved, "count", n)
			}
			if n < batchSize {
				break
			}
		}
	}
}
