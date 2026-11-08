// Package worker claims jobs from the queue and runs them.
package worker

import (
	"context"
	"errors"
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
// must be safe to call from many goroutines at once. A handler can also run
// more than once for the same job, if a worker dies partway through, so its
// side effects should be safe to repeat.
type HandlerFunc func(ctx context.Context, j *job.Job) error

// Handlers maps a job type, such as "echo", to the code that runs it.
type Handlers map[string]HandlerFunc

const (
	// completeTimeout bounds recording a result, which runs even when
	// shutdown has started so finished work isn't thrown away.
	completeTimeout = 5 * time.Second
	// abandonAfter is how long Run waits for handlers that ignore
	// cancellation before exiting without them.
	abandonAfter = time.Second
)

// Options configures a Pool.
type Options struct {
	// ID names this process. A job's lease owner is "<ID>/<slot>".
	ID          string
	Concurrency int
	// HeartbeatEvery is how often the lease on each running job is renewed.
	HeartbeatEvery time.Duration
	// ShutdownGrace is how long running jobs get to finish after Run's
	// context is cancelled.
	ShutdownGrace time.Duration
}

// Pool runs up to Concurrency jobs at once in one process. Each slot is a
// goroutine that claims a job, runs it and repeats. Slots don't coordinate
// with each other at all: claim.lua is what stops two of them, in this
// process or any other, from getting the same job.
type Pool struct {
	opts     Options
	queue    *queue.Queue
	handlers Handlers
	logger   *slog.Logger
}

// NewPool returns a pool that claims jobs from q.
func NewPool(opts Options, q *queue.Queue, handlers Handlers, logger *slog.Logger) *Pool {
	return &Pool{opts: opts, queue: q, handlers: handlers, logger: logger}
}

// Run claims and runs jobs until ctx is cancelled. It then stops claiming,
// gives running jobs ShutdownGrace to finish, and cancels whatever is left.
// A job still running when Run returns keeps its lease until it expires,
// after which the reaper hands it to another worker.
func (p *Pool) Run(ctx context.Context) {
	// Jobs deliberately outlive ctx, so that shutting down stops new work
	// first and only interrupts running work once the grace period is over.
	jobsCtx, cancelJobs := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelJobs()

	var slots sync.WaitGroup
	for slot := range p.opts.Concurrency {
		owner := fmt.Sprintf("%s/%d", p.opts.ID, slot)
		slots.Go(func() { p.runSlot(ctx, jobsCtx, owner) })
	}
	stopped := make(chan struct{})
	go func() {
		slots.Wait()
		close(stopped)
	}()

	<-ctx.Done()
	if waitFor(stopped, p.opts.ShutdownGrace) {
		return
	}
	p.logger.Warn("grace period over, cancelling running jobs", "grace", p.opts.ShutdownGrace)
	cancelJobs()
	if !waitFor(stopped, abandonAfter) {
		p.logger.Error("some handlers ignored cancellation; exiting without them")
	}
}

// runSlot claims jobs until claimCtx is cancelled. Jobs it runs use jobsCtx.
func (p *Pool) runSlot(claimCtx, jobsCtx context.Context, owner string) {
	var idle idleBackoff
	for claimCtx.Err() == nil {
		j, err := p.queue.Claim(claimCtx, owner)
		switch {
		case err != nil:
			if claimCtx.Err() == nil {
				p.logger.Error("claim failed", "owner", owner, "err", err)
			}
			sleep(claimCtx, idle.next())
		case j == nil:
			sleep(claimCtx, idle.next())
		default:
			idle.reset()
			p.process(jobsCtx, j)
		}
	}
}

func (p *Pool) process(jobsCtx context.Context, j *job.Job) {
	logger := p.logger.With("job_id", j.ID, "type", j.Type, "attempt", j.Attempts, "owner", j.LeaseOwner)
	// "late" is how long after its run_at the job actually started.
	logger.Info("job started", "late", j.StartedAt.Sub(j.RunAt))
	start := time.Now()

	ctx, cancel := context.WithCancelCause(jobsCtx)
	defer cancel(nil)
	var heartbeat sync.WaitGroup
	heartbeat.Go(func() { p.heartbeat(ctx, j, cancel, logger) })

	err := p.execute(ctx, j)
	cancel(nil) // stops the heartbeat; keeps the first cause if there was one
	heartbeat.Wait()

	if errors.Is(context.Cause(ctx), queue.ErrLeaseLost) {
		logger.Warn("lease lost, abandoning job", "took", time.Since(start))
		return
	}
	if err != nil {
		// Retries and the dead-letter queue come in Phase 4. Until then the
		// job's lease expires and the reaper hands it out again.
		logger.Error("job failed", "err", err, "took", time.Since(start))
		return
	}

	cctx, cancelComplete := context.WithTimeout(context.WithoutCancel(ctx), completeTimeout)
	defer cancelComplete()
	switch err := p.queue.Complete(cctx, j); {
	case errors.Is(err, queue.ErrLeaseLost):
		logger.Warn("result rejected: lease lost", "took", time.Since(start))
	case err != nil:
		logger.Error("could not record success", "err", err)
	default:
		logger.Info("job succeeded", "took", time.Since(start))
	}
}

// heartbeat renews j's lease until ctx is cancelled. If the lease is gone it
// cancels the job, because another worker may already be running it.
func (p *Pool) heartbeat(ctx context.Context, j *job.Job, cancel context.CancelCauseFunc, logger *slog.Logger) {
	ticker := time.NewTicker(p.opts.HeartbeatEvery)
	defer ticker.Stop()
	renewed := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		beatCtx, done := context.WithTimeout(ctx, p.opts.HeartbeatEvery)
		err := p.queue.Heartbeat(beatCtx, j)
		done()
		switch {
		case err == nil:
			renewed = time.Now()
		case errors.Is(err, queue.ErrLeaseLost):
			cancel(queue.ErrLeaseLost)
			return
		case ctx.Err() != nil:
			return
		default:
			logger.Warn("heartbeat failed", "err", err)
			// Without Redis we can't tell, but after a full lease with no
			// renewal we have to assume the job was handed to someone else.
			if time.Since(renewed) >= p.queue.LeaseTTL() {
				cancel(queue.ErrLeaseLost)
				return
			}
		}
	}
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

// waitFor reports whether done closed within d.
func waitFor(done <-chan struct{}, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-done:
		return true
	case <-t.C:
		return false
	}
}
