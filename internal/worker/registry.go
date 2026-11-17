package worker

import (
	"context"
	"sync"
	"time"

	"github.com/sham-33/job-scheduler/internal/queue"
)

// DefaultBeatEvery is how often a worker process tells the registry it is
// alive, unless Options.BeatEvery says otherwise. Readers count a worker as
// dead once it has missed a few beats (see events.DeadAfter).
const DefaultBeatEvery = time.Second

// leaveTimeout bounds removing the process from the registry on shutdown.
const leaveTimeout = 2 * time.Second

// startBeating puts this process in the worker registry and keeps it there,
// independently of ctx, until the returned function is called. That function
// stops the beats and takes the process out of the registry.
func (p *Pool) startBeating(ctx context.Context) (stop func()) {
	ctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	var wg sync.WaitGroup
	wg.Go(func() { p.beat(ctx) })
	return func() {
		cancel()
		wg.Wait()
		leaveCtx, done := context.WithTimeout(context.Background(), leaveTimeout)
		defer done()
		if err := p.queue.RemoveWorker(leaveCtx, p.opts.ID); err != nil {
			// The entry ages out instead: it shows as dead, then is forgotten.
			p.logger.Warn("could not leave the worker registry", "err", err)
		}
	}
}

// beat reports this process to the registry right away, then every
// BeatEvery until ctx is cancelled.
func (p *Pool) beat(ctx context.Context) {
	every := p.opts.BeatEvery
	if every <= 0 {
		every = DefaultBeatEvery
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	first := true
	for {
		beatCtx, done := context.WithTimeout(ctx, every)
		forgotten, err := p.queue.BeatWorker(beatCtx, queue.WorkerBeat{
			ID:          p.opts.ID,
			Concurrency: p.opts.Concurrency,
			Running:     int(p.running.Load()),
			First:       first,
		})
		done()
		switch {
		case err == nil:
			first = false
			if forgotten > 0 {
				p.logger.Info("forgot workers unseen for a long time", "count", forgotten, "after", queue.ForgetWorkersAfter)
			}
		case ctx.Err() == nil:
			p.logger.Warn("worker beat failed", "err", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
