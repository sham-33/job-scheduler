// Package config loads process settings from environment variables.
// Every binary (worker, scheduler, api) shares the same Config, so the whole
// cluster can be tuned from docker-compose without code changes.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"
)

// Config holds every tunable setting. Load fills in a default for anything
// not set in the environment.
type Config struct {
	// NodeID identifies this process in leases, logs and the worker registry.
	NodeID   string
	LogLevel slog.Level

	RedisAddr     string
	RedisPassword string
	RedisDB       int

	// HTTPAddr is where the API and dashboard listen.
	HTTPAddr string

	// Concurrency is the number of jobs one worker process runs at once.
	Concurrency int
	// LeaseTTL is how long a claimed job stays owned without a heartbeat.
	// It bounds worker-failure recovery: a dead worker's jobs are reclaimed
	// roughly LeaseTTL + ReapEvery after its last heartbeat.
	LeaseTTL time.Duration
	// HeartbeatEvery is how often a worker extends the lease on each running job.
	HeartbeatEvery time.Duration
	// ReapEvery is how often the scheduler looks for expired leases.
	ReapEvery time.Duration
	// ShutdownGrace is how long running jobs get to finish after a worker is
	// told to stop. Zero cancels them immediately.
	ShutdownGrace time.Duration
}

// Load reads the environment, applies defaults and validates the result.
// All problems are reported together so a bad deploy shows every typo at once.
func Load() (Config, error) {
	e := &env{}
	cfg := Config{
		NodeID:         e.str("NODE_ID", defaultNodeID()),
		LogLevel:       e.level("LOG_LEVEL", slog.LevelInfo),
		RedisAddr:      e.str("REDIS_ADDR", "localhost:6379"),
		RedisPassword:  e.str("REDIS_PASSWORD", ""),
		RedisDB:        e.integer("REDIS_DB", 0),
		HTTPAddr:       e.str("HTTP_ADDR", ":8080"),
		Concurrency:    e.integer("CONCURRENCY", 25),
		LeaseTTL:       e.duration("LEASE_TTL", 1500*time.Millisecond),
		HeartbeatEvery: e.duration("HEARTBEAT_EVERY", 500*time.Millisecond),
		ReapEvery:      e.duration("REAP_EVERY", 200*time.Millisecond),
		ShutdownGrace:  e.duration("SHUTDOWN_GRACE", 5*time.Second),
	}
	if err := errors.Join(append(e.errs, cfg.validate()...)...); err != nil {
		return Config{}, fmt.Errorf("invalid config: %w", err)
	}
	return cfg, nil
}

func (c Config) validate() []error {
	var errs []error
	if c.RedisDB < 0 {
		errs = append(errs, fmt.Errorf("REDIS_DB must not be negative, got %d", c.RedisDB))
	}
	if c.Concurrency < 1 {
		errs = append(errs, fmt.Errorf("CONCURRENCY must be at least 1, got %d", c.Concurrency))
	}
	for name, d := range map[string]time.Duration{
		"LEASE_TTL":       c.LeaseTTL,
		"HEARTBEAT_EVERY": c.HeartbeatEvery,
		"REAP_EVERY":      c.ReapEvery,
	} {
		if d <= 0 {
			errs = append(errs, fmt.Errorf("%s must be positive, got %s", name, d))
		}
	}
	if c.ShutdownGrace < 0 {
		errs = append(errs, fmt.Errorf("SHUTDOWN_GRACE must not be negative, got %s", c.ShutdownGrace))
	}
	// A lease has to survive one missed heartbeat, or a single slow Redis
	// round trip would make a healthy worker lose its job.
	if c.HeartbeatEvery > 0 && c.HeartbeatEvery*2 > c.LeaseTTL {
		errs = append(errs, fmt.Errorf("HEARTBEAT_EVERY (%s) must be at most half of LEASE_TTL (%s)", c.HeartbeatEvery, c.LeaseTTL))
	}
	return errs
}

// defaultNodeID is unique per process on one machine; inside Docker the
// hostname is the container ID, so it is unique across the cluster too.
func defaultNodeID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "node"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

// env reads typed variables and collects parse errors instead of stopping
// at the first one. An empty variable counts as unset.
type env struct {
	errs []error
}

func (e *env) str(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func (e *env) integer(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %q is not an integer", key, v))
		return def
	}
	return n
}

func (e *env) duration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %q is not a duration (use e.g. 500ms, 2s)", key, v))
		return def
	}
	return d
}

func (e *env) level(key string, def slog.Level) slog.Level {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var l slog.Level
	if err := l.UnmarshalText([]byte(v)); err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %q is not a log level (debug, info, warn, error)", key, v))
		return def
	}
	return l
}
