// Package store owns the Redis connection and the name of every Redis key
// the system uses.
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/sham-33/job-scheduler/internal/config"
)

// pingTimeout bounds the startup check so a wrong REDIS_ADDR fails fast.
const pingTimeout = 3 * time.Second

// poolSize is how many Redis connections a process may open. It covers every
// goroutine that can talk to Redis at once: each job slot can have a claim or
// completion plus a heartbeat (from Phase 3) in flight, and a few background
// loops run alongside them. With fewer connections those calls would queue,
// and a delayed heartbeat could cost a healthy worker its lease.
func poolSize(concurrency int) int {
	const background = 4
	return 2*concurrency + background
}

// Connect opens a Redis client and verifies it with PING, so a bad address
// or password fails at startup rather than on the first job.
func Connect(ctx context.Context, cfg config.Config) (*redis.Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
		PoolSize: poolSize(cfg.Concurrency),
		// Without this, go-redis ignores context deadlines on network reads
		// and waits out its own 3s timeout. That is longer than a lease, so a
		// worker couldn't notice in time that Redis had stopped answering.
		ContextTimeoutEnabled: true,
	})

	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("ping redis at %s: %w", cfg.RedisAddr, err)
	}
	return rdb, nil
}
