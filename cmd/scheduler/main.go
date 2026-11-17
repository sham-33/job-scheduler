// Command scheduler keeps jobs moving: it releases delayed jobs when they
// fall due, takes jobs back from dead workers, and fires crons. Run two or
// more for failover. They elect one leader, and only the leader does the
// work.
package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/sham-33/job-scheduler/internal/config"
	"github.com/sham-33/job-scheduler/internal/election"
	"github.com/sham-33/job-scheduler/internal/logging"
	"github.com/sham-33/job-scheduler/internal/queue"
	"github.com/sham-33/job-scheduler/internal/scheduler"
	"github.com/sham-33/job-scheduler/internal/store"
)

func main() {
	if err := run(); err != nil {
		slog.Error("scheduler failed", "err", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	logger := logging.New(cfg.LogLevel, "scheduler", cfg.NodeID)

	rdb, err := store.Connect(ctx, cfg)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer rdb.Close()
	logger.Info("connected", "redis", cfg.RedisAddr, "db", cfg.RedisDB, "leader_ttl", cfg.LeaderTTL)

	keys := store.Keys{}
	q := queue.New(rdb, keys, cfg.LeaseTTL)
	// The random part keeps two schedulers apart even when they share a
	// NODE_ID, as a restarted container does (same hostname, and always pid
	// 1). Two schedulers with one ID would both believe they lead.
	candidate := cfg.NodeID + "/" + rand.Text()[:6]
	logger.Info("campaigning for leader", "candidate", candidate)
	elector := election.New(rdb, keys, election.Options{
		ID:         candidate,
		TTL:        cfg.LeaderTTL,
		RenewEvery: cfg.LeaderRenewEvery,
	}, logger)

	// Run returns once ctx is cancelled and any term in progress has ended,
	// with the seat handed back so another scheduler can take over at once.
	elector.Run(ctx, func(ctx context.Context, epoch int64) {
		scheduler.Lead(ctx, q, cfg.ReapEvery, logger.With("epoch", epoch))
	})
	logger.Info("shut down")
	return nil
}
