// Package worker claims jobs from the queue and runs them.
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/sham-33/job-scheduler/internal/job"
	"github.com/sham-33/job-scheduler/internal/queue"
)

// HandlerFunc does the work for one job type. It should stop early and
// return ctx.Err() when ctx is cancelled.
type HandlerFunc func(ctx context.Context, j *job.Job) error

// Handlers maps a job type, such as "echo", to the code that runs it.
type Handlers map[string]HandlerFunc

const (
	// idlePoll is how long the worker waits before asking again when no job
	// is ready, or after Redis returned an error.
	idlePoll = 100 * time.Millisecond
	// completeTimeout bounds recording a result, which runs even when
	// shutdown has started so finished work isn't thrown away.
	completeTimeout = 5 * time.Second
)

// Worker runs one job at a time.
type Worker struct {
	id       string
	queue    *queue.Queue
	handlers Handlers
	logger   *slog.Logger
}

// New returns a worker that claims jobs under the given ID.
func New(id string, q *queue.Queue, handlers Handlers, logger *slog.Logger) *Worker {
	return &Worker{id: id, queue: q, handlers: handlers, logger: logger}
}

// Run claims and runs jobs until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) {
	for ctx.Err() == nil {
		j, err := w.queue.Claim(ctx, w.id)
		switch {
		case err != nil:
			if ctx.Err() == nil {
				w.logger.Error("claim failed", "err", err)
			}
			sleep(ctx, idlePoll)
		case j == nil:
			sleep(ctx, idlePoll)
		default:
			w.process(ctx, j)
		}
	}
}

func (w *Worker) process(ctx context.Context, j *job.Job) {
	logger := w.logger.With("job_id", j.ID, "type", j.Type, "attempt", j.Attempts)
	// "late" is how long after its run_at the job actually started.
	logger.Info("job started", "late", j.StartedAt.Sub(j.RunAt))
	start := time.Now()

	if err := w.execute(ctx, j); err != nil {
		// Retries and the dead-letter queue come in Phase 4. Until then a
		// failed job stays "running" until its lease is reclaimed (Phase 3).
		logger.Error("job failed", "err", err, "took", time.Since(start))
		return
	}

	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), completeTimeout)
	defer cancel()
	if err := w.queue.Complete(cctx, j); err != nil {
		logger.Error("could not record success", "err", err)
		return
	}
	logger.Info("job succeeded", "took", time.Since(start))
}

func (w *Worker) execute(ctx context.Context, j *job.Job) error {
	handle, ok := w.handlers[j.Type]
	if !ok {
		return fmt.Errorf("no handler for job type %q", j.Type)
	}
	return handle(ctx, j)
}

// sleep waits for d, or less if ctx is cancelled first.
func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
