// Command worker claims jobs from Redis and runs up to CONCURRENCY of them at
// once. It needs a scheduler running too: the scheduler releases delayed jobs
// when they fall due and takes jobs back from workers that died.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/sham-33/job-scheduler/internal/config"
	"github.com/sham-33/job-scheduler/internal/handlers"
	"github.com/sham-33/job-scheduler/internal/logging"
	"github.com/sham-33/job-scheduler/internal/queue"
	"github.com/sham-33/job-scheduler/internal/store"
	"github.com/sham-33/job-scheduler/internal/worker"
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

	q := queue.New(rdb, store.Keys{}, cfg.LeaseTTL)
	pool := worker.NewPool(worker.Options{
		ID:             cfg.NodeID,
		Concurrency:    cfg.Concurrency,
		HeartbeatEvery: cfg.HeartbeatEvery,
		ShutdownGrace:  cfg.ShutdownGrace,
	}, q, handlers.Default(), logger)

	done := make(chan struct{})
	go func() {
		pool.Run(ctx)
		close(done)
	}()

	<-ctx.Done()
	logger.Info("shutting down")
	<-done
	return nil
}
