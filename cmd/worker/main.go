// Command worker claims jobs from Redis and runs them. For now it connects
// to Redis and waits for a shutdown signal; the job loop comes in Phase 1.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/shambutn/job-scheduler/internal/config"
	"github.com/shambutn/job-scheduler/internal/logging"
	"github.com/shambutn/job-scheduler/internal/store"
)

func main() {
	if err := run(); err != nil {
		slog.Error("worker failed", "err", err)
		os.Exit(1)
	}
}

func run() error {
	// Ctrl-C and `docker stop` (SIGTERM) both cancel ctx, which is how every
	// loop in the worker learns it should shut down.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	logger := logging.New(cfg.LogLevel, "worker", cfg.NodeID)

	rdb, err := store.Connect(ctx, cfg)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer rdb.Close()
	logger.Info("connected", "redis", cfg.RedisAddr, "db", cfg.RedisDB, "concurrency", cfg.Concurrency)

	<-ctx.Done()
	logger.Info("shutting down")
	return nil
}
