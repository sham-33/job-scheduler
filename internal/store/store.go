// Package store owns the Redis connection. Later phases add the key names
// and the Lua scripts that make every job state change atomic.
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/shambutn/job-scheduler/internal/config"
)

// pingTimeout bounds the startup check so a wrong REDIS_ADDR fails fast.
const pingTimeout = 3 * time.Second

// Connect opens a Redis client and verifies it with PING, so a bad address
// or password fails at startup rather than on the first job.
func Connect(ctx context.Context, cfg config.Config) (*redis.Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})

	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("ping redis at %s: %w", cfg.RedisAddr, err)
	}
	return rdb, nil
}
