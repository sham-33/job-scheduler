package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

var allKeys = []string{
	"NODE_ID", "LOG_LEVEL", "REDIS_ADDR", "REDIS_PASSWORD", "REDIS_DB", "HTTP_ADDR",
	"CONCURRENCY", "LEASE_TTL", "HEARTBEAT_EVERY", "REAP_EVERY", "SHUTDOWN_GRACE",
}

// clearEnv blanks every config variable for the test; Load treats empty as unset.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range allKeys {
		t.Setenv(k, "")
	}
}

func TestLoadDefaults(t *testing.T) {
	clearEnv(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.NodeID == "" {
		t.Error("NodeID is empty, want hostname-pid default")
	}
	want := Config{
		NodeID:         cfg.NodeID,
		LogLevel:       slog.LevelInfo,
		RedisAddr:      "localhost:6379",
		HTTPAddr:       ":8080",
		Concurrency:    25,
		LeaseTTL:       1500 * time.Millisecond,
		HeartbeatEvery: 500 * time.Millisecond,
		ReapEvery:      200 * time.Millisecond,
		ShutdownGrace:  5 * time.Second,
	}
	if cfg != want {
		t.Errorf("Load() = %+v\nwant     %+v", cfg, want)
	}
}

func TestLoadOverrides(t *testing.T) {
	clearEnv(t)
	t.Setenv("NODE_ID", "worker-7")
	t.Setenv("LOG_LEVEL", "DEBUG")
	t.Setenv("REDIS_ADDR", "redis:6379")
	t.Setenv("REDIS_PASSWORD", "secret")
	t.Setenv("REDIS_DB", "3")
	t.Setenv("HTTP_ADDR", ":9090")
	t.Setenv("CONCURRENCY", "5")
	t.Setenv("LEASE_TTL", "3s")
	t.Setenv("HEARTBEAT_EVERY", "1s")
	t.Setenv("REAP_EVERY", "250ms")
	t.Setenv("SHUTDOWN_GRACE", "30s")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := Config{
		NodeID:         "worker-7",
		LogLevel:       slog.LevelDebug,
		RedisAddr:      "redis:6379",
		RedisPassword:  "secret",
		RedisDB:        3,
		HTTPAddr:       ":9090",
		Concurrency:    5,
		LeaseTTL:       3 * time.Second,
		HeartbeatEvery: time.Second,
		ReapEvery:      250 * time.Millisecond,
		ShutdownGrace:  30 * time.Second,
	}
	if cfg != want {
		t.Errorf("Load() = %+v\nwant     %+v", cfg, want)
	}
}

func TestLoadInvalid(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr string
	}{
		{"non-numeric concurrency", map[string]string{"CONCURRENCY": "lots"}, "CONCURRENCY"},
		{"zero concurrency", map[string]string{"CONCURRENCY": "0"}, "CONCURRENCY must be at least 1"},
		{"negative db", map[string]string{"REDIS_DB": "-1"}, "REDIS_DB"},
		{"bad duration", map[string]string{"LEASE_TTL": "fast"}, "LEASE_TTL"},
		{"negative duration", map[string]string{"REAP_EVERY": "-1s"}, "REAP_EVERY must be positive"},
		{"negative grace", map[string]string{"SHUTDOWN_GRACE": "-1s"}, "SHUTDOWN_GRACE"},
		{"bad log level", map[string]string{"LOG_LEVEL": "loud"}, "LOG_LEVEL"},
		{"heartbeat too slow for lease", map[string]string{"LEASE_TTL": "1s", "HEARTBEAT_EVERY": "600ms"}, "at most half of LEASE_TTL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearEnv(t)
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			_, err := Load()
			if err == nil {
				t.Fatalf("Load() succeeded, want error containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Load() error = %q, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

func TestLoadReportsAllErrors(t *testing.T) {
	clearEnv(t)
	t.Setenv("CONCURRENCY", "lots")
	t.Setenv("LEASE_TTL", "fast")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() succeeded, want error")
	}
	for _, key := range []string{"CONCURRENCY", "LEASE_TTL"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error %q does not mention %s", err, key)
		}
	}
}
