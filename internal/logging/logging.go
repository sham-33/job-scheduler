// Package logging builds the structured logger every binary uses.
package logging

import (
	"log/slog"
	"os"
)

// New returns a logger tagged with the component (worker, scheduler, api)
// and node ID, and installs it as the slog default so code that logs through
// the slog package functions gets the same fields.
func New(level slog.Level, component, nodeID string) *slog.Logger {
	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	logger := slog.New(handler).With("component", component, "node", nodeID)
	slog.SetDefault(logger)
	return logger
}
