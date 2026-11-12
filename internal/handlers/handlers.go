// Package handlers holds example job types. They stand in for real work
// (sending email, resizing images) in demos, tests and load runs.
package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/sham-33/job-scheduler/internal/job"
	"github.com/sham-33/job-scheduler/internal/worker"
)

// Default returns every example job type.
func Default() worker.Handlers {
	return worker.Handlers{
		"echo":  Echo,
		"sleep": Sleep,
		"flaky": Flaky,
		"http":  HTTP,
	}
}

// Echo logs the job's payload. It is the simplest possible job.
func Echo(ctx context.Context, j *job.Job) error {
	slog.InfoContext(ctx, "echo", "job_id", j.ID, "payload", string(j.Payload))
	return nil
}

// SleepPayload is the payload of a "sleep" job, for example {"ms": 250}.
type SleepPayload struct {
	MS int `json:"ms"`
}

// Sleep waits for the payload's number of milliseconds, stopping early if
// ctx is cancelled. An empty payload sleeps for zero.
func Sleep(ctx context.Context, j *job.Job) error {
	var p SleepPayload
	if len(j.Payload) > 0 {
		if err := json.Unmarshal(j.Payload, &p); err != nil {
			return job.Permanent(fmt.Errorf("sleep: bad payload: %w", err))
		}
	}
	if p.MS < 0 {
		return job.Permanent(fmt.Errorf("sleep: ms must not be negative, got %d", p.MS))
	}
	t := time.NewTimer(time.Duration(p.MS) * time.Millisecond)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// FlakyPayload is the payload of a "flaky" job, for example {"p": 0.5}.
type FlakyPayload struct {
	// P is the chance, from 0 to 1, that an attempt fails.
	P float64 `json:"p"`
}

// Flaky fails at random, to stand in for work that depends on an unreliable
// service. An empty payload never fails.
func Flaky(_ context.Context, j *job.Job) error {
	var p FlakyPayload
	if len(j.Payload) > 0 {
		if err := json.Unmarshal(j.Payload, &p); err != nil {
			return job.Permanent(fmt.Errorf("flaky: bad payload: %w", err))
		}
	}
	if p.P < 0 || p.P > 1 {
		return job.Permanent(fmt.Errorf("flaky: p must be between 0 and 1, got %v", p.P))
	}
	if rand.Float64() < p.P {
		return fmt.Errorf("flaky: unlucky on attempt %d", j.Attempts)
	}
	return nil
}
