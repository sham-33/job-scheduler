package monitor

import (
	"context"
	_ "embed"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/sham-33/job-scheduler/internal/store"
)

const (
	// BeatEvery is how often a worker process checks in.
	BeatEvery = time.Second
	// DeadAfter is how long a worker can go without checking in before it
	// counts as dead. It allows for a few missed beats.
	DeadAfter = 5 * time.Second
	// forgetAfter is how long a worker that stopped checking in stays
	// listed, as dead, before it is dropped.
	forgetAfter = time.Hour
	// leaveTimeout bounds deregistering on shutdown.
	leaveTimeout = 2 * time.Second
)

var (
	//go:embed scripts/beat.lua
	beatSrc    string
	beatScript = redis.NewScript(beatSrc)
)

// WorkerStatus is what a worker process reports each time it checks in.
type WorkerStatus struct {
	Concurrency int
	Busy        int  // jobs running right now
	Draining    bool // shutting down: no longer claiming new jobs
}

// Registry keeps the list of worker processes in Redis. Each one checks in
// every BeatEvery. The registry is only for watching the cluster: nothing
// about jobs depends on it, because leases already handle dead workers.
type Registry struct {
	rdb    *redis.Client
	keys   store.Keys
	logger *slog.Logger
	onBeat func()
}

// NewRegistry returns a Registry.
func NewRegistry(rdb *redis.Client, keys store.Keys, logger *slog.Logger) *Registry {
	return &Registry{rdb: rdb, keys: keys, logger: logger}
}

// OnBeat sets f to be called after every check-in that reached Redis. It
// must be called before Run.
func (r *Registry) OnBeat(f func()) {
	r.onBeat = f
}

// Run checks in as worker id every BeatEvery, reporting status(), until ctx
// is cancelled. It then removes id from the registry, so a worker that shuts
// down cleanly disappears rather than showing as dead.
func (r *Registry) Run(ctx context.Context, id string, status func() WorkerStatus) {
	first := true
	ticker := time.NewTicker(BeatEvery)
	defer ticker.Stop()
	for {
		beatCtx, cancel := context.WithTimeout(ctx, BeatEvery)
		err := r.Beat(beatCtx, id, status(), first)
		cancel()
		switch {
		case err == nil:
			first = false
			if r.onBeat != nil {
				r.onBeat()
			}
		case ctx.Err() == nil:
			r.logger.Warn("worker check-in failed", "err", err)
		}

		select {
		case <-ctx.Done():
			leaveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), leaveTimeout)
			defer cancel()
			if err := r.Leave(leaveCtx, id); err != nil {
				r.logger.Warn("could not leave the worker registry; it will show as dead", "err", err)
			}
			return
		case <-ticker.C:
		}
	}
}

// Beat records one check-in. first resets the start time, for a worker that
// has just started.
func (r *Registry) Beat(ctx context.Context, id string, s WorkerStatus, first bool) error {
	err := beatScript.Run(ctx, r.rdb, []string{r.keys.Workers(), r.keys.Worker(id)},
		id, s.Concurrency, s.Busy, flag(s.Draining), flag(first), forgetAfter.Milliseconds(),
	).Err()
	if err != nil {
		return fmt.Errorf("worker check-in: %w", err)
	}
	return nil
}

// Leave removes a worker from the registry.
func (r *Registry) Leave(ctx context.Context, id string) error {
	_, err := r.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.ZRem(ctx, r.keys.Workers(), id)
		p.Del(ctx, r.keys.Worker(id))
		return nil
	})
	if err != nil {
		return fmt.Errorf("leave worker registry: %w", err)
	}
	return nil
}

func flag(b bool) string {
	if b {
		return "1"
	}
	return "0"
}
