// Package worker claims jobs from the queue and runs them.
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/sham-33/job-scheduler/internal/job"
	"github.com/sham-33/job-scheduler/internal/queue"
)

// HandlerFunc does the work for one job type. It should stop early and
// return ctx.Err() when ctx is cancelled. Handlers run concurrently, so they
// must be safe to call from many goroutines at once.
type HandlerFunc func(ctx context.Context, j *job.Job) error

// Handlers maps a job type, such as "echo", to the code that runs it.
type Handlers map[string]HandlerFunc

// completeTimeout bounds recording a result, which runs even when shutdown
// has started so finished work isn't thrown away.
const completeTimeout = 5 * time.Second

// Pool runs up to concurrency jobs at once in one process. Each slot is a
// goroutine that claims a job, runs it and repeats. Slots don't coordinate
// with each other at all: claim.lua is what stops two of them, in this
// process or any other, from getting the same job.
type Pool struct {
	id          string
	concurrency int
	queue       *queue.Queue
	handlers    Handlers
	logger      *slog.Logger
}

// NewPool returns a pool that claims jobs under the given ID.
func NewPool(id string, concurrency int, q *queue.Queue, handlers Handlers, logger *slog.Logger) *Pool {
	return &Pool{id: id, concurrency: concurrency, queue: q, handlers: handlers, logger: logger}
}

// Run claims and runs jobs until ctx is cancelled, then waits for every slot
// to stop.
func (p *Pool) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for slot := range p.concurrency {
		owner := fmt.Sprintf("%s/%d", p.id, slot)
		wg.Go(func() { p.runSlot(ctx, owner) })
	}
	wg.Wait()
}

func (p *Pool) runSlot(ctx context.Context, owner string) {
	var idle idleBackoff
	for ctx.Err() == nil {
		j, err := p.queue.Claim(ctx, owner)
		switch {
		case err != nil:
			if ctx.Err() == nil {
				p.logger.Error("claim failed", "owner", owner, "err", err)
			}
			sleep(ctx, idle.next())
		case j == nil:
			sleep(ctx, idle.next())
		default:
			idle.reset()
			p.process(ctx, j)
		}
	}
}

func (p *Pool) process(ctx context.Context, j *job.Job) {
	logger := p.logger.With("job_id", j.ID, "type", j.Type, "attempt", j.Attempts, "owner", j.LeaseOwner)
	// "late" is how long after its run_at the job actually started.
	logger.Info("job started", "late", j.StartedAt.Sub(j.RunAt))
	start := time.Now()

	if err := p.execute(ctx, j); err != nil {
		// Retries and the dead-letter queue come in Phase 4. Until then a
		// failed job stays "running" until its lease is reclaimed (Phase 3).
		logger.Error("job failed", "err", err, "took", time.Since(start))
		return
	}

	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), completeTimeout)
	defer cancel()
	if err := p.queue.Complete(cctx, j); err != nil {
		logger.Error("could not record success", "err", err)
		return
	}
	logger.Info("job succeeded", "took", time.Since(start))
}

// execute runs the job's handler. A panic becomes an ordinary error, because
// one bad job must not crash the process and every other job running in it.
func (p *Pool) execute(ctx context.Context, j *job.Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			p.logger.Error("handler panicked", "job_id", j.ID, "type", j.Type, "panic", r, "stack", string(debug.Stack()))
			err = fmt.Errorf("handler panicked: %v", r)
		}
	}()
	handle, ok := p.handlers[j.Type]
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
