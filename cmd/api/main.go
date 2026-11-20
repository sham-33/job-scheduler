// Command api serves the HTTP API for submitting and managing jobs and
// crons and for watching the cluster. It holds no state of its own, so any
// number can run behind a load balancer.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sham-33/job-scheduler/internal/api"
	"github.com/sham-33/job-scheduler/internal/config"
	"github.com/sham-33/job-scheduler/internal/logging"
	"github.com/sham-33/job-scheduler/internal/monitor"
	"github.com/sham-33/job-scheduler/internal/queue"
	"github.com/sham-33/job-scheduler/internal/store"
)

// shutdownGrace is how long requests in flight get to finish on shutdown.
const shutdownGrace = 5 * time.Second

func main() {
	if err := run(); err != nil {
		slog.Error("api failed", "err", err)
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
	logger := logging.New(cfg.LogLevel, "api", cfg.NodeID)

	rdb, err := store.Connect(ctx, cfg)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer rdb.Close()

	keys := store.Keys{}
	server := api.New(queue.New(rdb, keys, cfg.LeaseTTL), monitor.New(rdb, keys),
		api.Options{Token: cfg.APIToken}, logger)
	if cfg.APIToken == "" {
		logger.Warn("API_TOKEN is not set: anyone who can reach the API can submit jobs, " +
			"including http jobs that call any address the workers can reach")
	}

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           server.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	logger.Info("listening", "addr", cfg.HTTPAddr, "redis", cfg.RedisAddr, "db", cfg.RedisDB)

	select {
	case err := <-errc:
		return fmt.Errorf("listen on %s: %w", cfg.HTTPAddr, err)
	case <-ctx.Done():
	}
	logger.Info("shutting down")
	shutCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("shutdown: %w", err)
	}
	logger.Info("shut down")
	return nil
}
