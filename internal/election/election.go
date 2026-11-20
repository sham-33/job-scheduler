// Package election picks one leader among the schedulers sharing a Redis
// server, so the housekeeping loops run once instead of once per scheduler.
//
// Leadership is a Redis key that holds the leader's ID and expires unless the
// leader keeps renewing it. If the leader dies, the key expires and another
// scheduler takes the seat. Every term gets a new epoch number.
//
// The election saves work but doesn't keep jobs correct: every housekeeping
// step is an atomic Lua script that is safe to run twice. So the brief
// overlap that any lease-based election allows, such as a paused leader
// waking up after its term was handed on, does no harm.
package election

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/sham-33/job-scheduler/internal/store"
)

var (
	//go:embed scripts/acquire.lua
	acquireSrc string
	//go:embed scripts/renew.lua
	renewSrc string
	//go:embed scripts/release.lua
	releaseSrc string

	acquireScript = redis.NewScript(acquireSrc)
	renewScript   = redis.NewScript(renewSrc)
	releaseScript = redis.NewScript(releaseSrc)
)

var (
	errSeatTaken    = errors.New("another scheduler holds the seat")
	errLeadReturned = errors.New("the leader's work stopped by itself")
)

// Options configures an Elector.
type Options struct {
	// ID names this candidate. Every scheduler needs a different one: two
	// with the same ID would both believe they lead.
	ID string
	// TTL is how long a term lasts unless the leader renews it. If the leader
	// dies, another candidate takes over within about TTL.
	TTL time.Duration
	// RenewEvery is how often the leader renews its term. A renewal that
	// fails ends the term at once, so it must be at most TTL/2.
	RenewEvery time.Duration
	// PollEvery is how often a candidate that isn't leading checks whether
	// the seat is free. Zero means RenewEvery/4.
	PollEvery time.Duration
	// OnContact, if set, is called after every acquire or renew call that
	// got an answer from Redis, whatever the answer. A health check uses it
	// to tell a working candidate from one cut off from Redis.
	OnContact func()
}

// Elector campaigns for leadership on behalf of one process.
type Elector struct {
	rdb    *redis.Client
	keys   store.Keys
	opts   Options
	logger *slog.Logger
}

// New returns an Elector. Call Run to start campaigning.
func New(rdb *redis.Client, keys store.Keys, opts Options, logger *slog.Logger) *Elector {
	if opts.PollEvery <= 0 {
		opts.PollEvery = opts.RenewEvery / 4
	}
	return &Elector{rdb: rdb, keys: keys, opts: opts, logger: logger}
}

// Run campaigns until ctx is cancelled. Each time this process wins a term,
// Run calls lead with the term's epoch and a context that is cancelled as
// soon as the term ends: when a renewal fails, when another scheduler holds
// the seat, or when ctx is cancelled. lead should do the leader's work until
// its context is cancelled and then return promptly. Run waits for it before
// campaigning again, so one process never runs two terms at once.
//
// When ctx is cancelled, Run gives up the seat, so another candidate takes
// over at once instead of waiting for the term to expire.
func (e *Elector) Run(ctx context.Context, lead func(ctx context.Context, epoch int64)) {
	for ctx.Err() == nil {
		epoch, err := e.acquire(ctx)
		if err == nil {
			e.contact()
		}
		switch {
		case err != nil:
			if ctx.Err() == nil {
				e.logger.Error("leader election failed", "err", err)
			}
		case epoch > 0:
			e.serve(ctx, epoch, lead)
		}
		sleep(ctx, e.opts.PollEvery)
	}
}

// serve runs one term, from winning the seat to giving it up or losing it.
func (e *Elector) serve(ctx context.Context, epoch int64, lead func(context.Context, int64)) {
	logger := e.logger.With("epoch", epoch)
	logger.Info("elected leader")

	termCtx, end := context.WithCancelCause(ctx)
	defer end(nil)
	done := make(chan struct{})
	go func() {
		defer close(done)
		lead(termCtx, epoch)
	}()

	reason := e.keepRenewing(termCtx, done)
	end(reason)
	<-done

	if ctx.Err() == nil && !errors.Is(reason, errLeadReturned) {
		logger.Warn("lost leadership", "reason", reason)
		return
	}
	// Hand over now rather than leave the others waiting out the term.
	relCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), e.opts.RenewEvery)
	defer cancel()
	if err := e.release(relCtx); err != nil {
		logger.Warn("could not give up leadership; it passes on when the term expires", "err", err)
	}
	if ctx.Err() == nil {
		logger.Warn("stepped down", "reason", reason)
	} else {
		logger.Info("stepped down")
	}
}

// keepRenewing renews the term every RenewEvery until the term ends, and
// returns why it ended.
func (e *Elector) keepRenewing(ctx context.Context, done <-chan struct{}) error {
	ticker := time.NewTicker(e.opts.RenewEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-done:
			return errLeadReturned
		case <-ticker.C:
		}
		// The last renewal made the term last TTL from then. Giving up on
		// this one RenewEvery after it starts means a leader that can't
		// reach Redis stops leading before its term expires (RenewEvery*2 is
		// at most TTL), so the next leader doesn't start while it still runs.
		renewCtx, cancel := context.WithTimeout(ctx, e.opts.RenewEvery)
		ok, err := e.renew(renewCtx)
		cancel()
		if err == nil {
			e.contact()
		}
		switch {
		case ctx.Err() != nil:
			return context.Cause(ctx)
		case err != nil:
			return fmt.Errorf("renewal failed: %w", err)
		case !ok:
			return errSeatTaken
		}
	}
}

func (e *Elector) contact() {
	if e.opts.OnContact != nil {
		e.opts.OnContact()
	}
}

func (e *Elector) acquire(ctx context.Context) (epoch int64, err error) {
	epoch, err = acquireScript.Run(ctx, e.rdb, []string{e.keys.Leader(), e.keys.Epoch()},
		e.opts.ID, e.opts.TTL.Milliseconds()).Int64()
	if err != nil {
		return 0, fmt.Errorf("acquire: %w", err)
	}
	return epoch, nil
}

func (e *Elector) renew(ctx context.Context) (bool, error) {
	n, err := renewScript.Run(ctx, e.rdb, []string{e.keys.Leader()}, e.opts.ID, e.opts.TTL.Milliseconds()).Int()
	return n == 1, err
}

func (e *Elector) release(ctx context.Context) error {
	return releaseScript.Run(ctx, e.rdb, []string{e.keys.Leader()}, e.opts.ID).Err()
}

// Leader reports which candidate leads right now, or "" if none does, and
// the epoch of the latest term.
func Leader(ctx context.Context, rdb *redis.Client, keys store.Keys) (id string, epoch int64, err error) {
	vals, err := rdb.MGet(ctx, keys.Leader(), keys.Epoch()).Result()
	if err != nil {
		return "", 0, fmt.Errorf("read leader: %w", err)
	}
	id, _ = vals[0].(string)
	if s, ok := vals[1].(string); ok {
		if epoch, err = strconv.ParseInt(s, 10, 64); err != nil {
			return "", 0, fmt.Errorf("read leader: epoch %q is not a number", s)
		}
	}
	return id, epoch, nil
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
