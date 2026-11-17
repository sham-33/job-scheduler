package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/sham-33/job-scheduler/internal/queue"
)

// CronEvery is how often the leader looks for crons that are due. While a
// leader is running, it bounds how late a cron fires.
const CronEvery = 100 * time.Millisecond

// RunCron fires due crons every CronEvery until ctx is cancelled.
func RunCron(ctx context.Context, q *queue.Queue, logger *slog.Logger) {
	loop{
		every: CronEvery,
		step: func(ctx context.Context, limit int) (int, error) {
			fires, err := q.FireDueCrons(ctx, limit)
			for _, f := range fires {
				logFire(ctx, logger, f)
			}
			return len(fires), err
		},
		name:  "cron",
		level: slog.LevelDebug,
		moved: "fired crons",
	}.run(ctx, logger)
}

func logFire(ctx context.Context, logger *slog.Logger, f queue.CronFire) {
	level, msg := slog.LevelInfo, "fired cron"
	switch {
	case !f.Created:
		level, msg = slog.LevelWarn, "cron slot already had a job; made no new one"
	case f.Skipped:
		level, msg = slog.LevelWarn, "cron fell behind: fired once for its oldest missed slot and skipped the rest"
	}
	logger.Log(ctx, level, msg, "cron", f.Name, "slot", f.Slot.UTC(), "job_id", f.JobID,
		"late", f.Late.Round(time.Millisecond), "next", f.Next.UTC())
}
