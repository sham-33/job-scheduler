package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/sham-33/job-scheduler/internal/queue"
)

// RunReaper makes jobs with expired leases claimable again, every interval,
// until ctx is cancelled. A job is back in the ready set within about
// LeaseTTL + every of its worker's last heartbeat.
func RunReaper(ctx context.Context, q *queue.Queue, every time.Duration, logger *slog.Logger) {
	loop{
		every: every,
		step:  q.Reap,
		name:  "reap",
		level: slog.LevelWarn,
		moved: "reclaimed jobs from expired leases",
	}.run(ctx, logger)
}
