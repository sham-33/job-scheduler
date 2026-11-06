// Package scheduler holds the housekeeping loops that keep jobs moving:
// releasing delayed jobs when they fall due, and (from Phase 3) reclaiming
// jobs from dead workers.
package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/sham-33/job-scheduler/internal/queue"
)

const (
	// PromoteEvery is how often due jobs move from scheduled to ready. Along
	// with the worker's idle poll, it bounds how late a delayed job starts.
	PromoteEvery = 100 * time.Millisecond
	promoteBatch = 1000
)

// RunPromoter moves due jobs to the ready set every PromoteEvery until ctx
// is cancelled.
func RunPromoter(ctx context.Context, q *queue.Queue, logger *slog.Logger) {
	ticker := time.NewTicker(PromoteEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		// Keep going while full batches come back, so a burst of due jobs
		// doesn't trickle out one batch per tick.
		for {
			n, err := q.Promote(ctx, promoteBatch)
			if err != nil {
				if ctx.Err() == nil {
					logger.Error("promote failed", "err", err)
				}
				break
			}
			if n > 0 {
				logger.Debug("promoted due jobs", "count", n)
			}
			if n < promoteBatch {
				break
			}
		}
	}
}
