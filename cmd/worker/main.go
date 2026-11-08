// Command worker claims jobs from Redis and runs up to CONCURRENCY of them at
// once.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/sham-33/job-scheduler/internal/config"
	"github.com/sham-33/job-scheduler/internal/handlers"
	"github.com/sham-33/job-scheduler/internal/logging"
	"github.com/sham-33/job-scheduler/internal/queue"
	"github.com/sham-33/job-scheduler/internal/scheduler"
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

	var wg sync.WaitGroup
	// Temporary: Phase 5 moves the promoter and reaper into the scheduler
	// service, so only the elected leader runs them.
	wg.Go(func() { scheduler.RunPromoter(ctx, q, logger) })
	wg.Go(func() { scheduler.RunReaper(ctx, q, cfg.ReapEvery, logger) })
	wg.Go(func() { pool.Run(ctx) })

	<-ctx.Done()
	logger.Info("shutting down")
	wg.Wait()
	return nil
}
