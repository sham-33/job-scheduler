package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/sham-33/job-scheduler/internal/queue"
)

// PromoteEvery is how often due jobs move from scheduled to ready. Along with
// the worker's idle poll, it bounds how late a delayed job starts.
const PromoteEvery = 100 * time.Millisecond

// RunPromoter moves due jobs to the ready set every PromoteEvery until ctx
// is cancelled.
func RunPromoter(ctx context.Context, q *queue.Queue, logger *slog.Logger) {
	loop{
		every: PromoteEvery,
		step:  q.Promote,
		name:  "promote",
		level: slog.LevelDebug,
		moved: "promoted due jobs",
	}.run(ctx, logger)
}
