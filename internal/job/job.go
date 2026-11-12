// Package job defines what a job is and how it is stored in a Redis hash.
package job

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// Status is where a job is in its lifecycle:
//
//	scheduled → ready → running → succeeded
//	running → (failed or lease expired) → scheduled (retry) … → dead
//	any non-terminal status → cancelled
type Status string

const (
	StatusScheduled Status = "scheduled" // waiting for run_at
	StatusReady     Status = "ready"     // due, waiting for a worker
	StatusRunning   Status = "running"   // leased to a worker
	StatusSucceeded Status = "succeeded"
	StatusDead      Status = "dead" // out of retries, in the dead-letter queue
	StatusCancelled Status = "cancelled"
)

// Job is one unit of work. Every field is written by the Lua scripts in
// package queue; Go code only reads jobs back with FromHash.
type Job struct {
	ID             string          `json:"id"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload,omitempty"`
	Status         Status          `json:"status"`
	Attempts       int             `json:"attempts"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`

	// MaxAttempts is how many times the job may start before it is moved to
	// the dead-letter queue. Zero means no limit.
	MaxAttempts int `json:"max_attempts,omitempty"`
	// Timeout cancels a single attempt that runs longer. Zero means no limit.
	Timeout time.Duration `json:"-"`
	// BackoffBase is the wait before the first retry. It doubles on each
	// further retry, up to BackoffMax.
	BackoffBase time.Duration `json:"-"`
	BackoffMax  time.Duration `json:"-"`

	// LeaseOwner is the worker that holds (or last held) the job.
	LeaseOwner string `json:"lease_owner,omitempty"`
	// LeaseToken is the fencing token from the latest claim. Only the holder
	// of the current token may finish the job.
	LeaseToken int64 `json:"lease_token,omitempty"`
	// LastError says why the most recent attempt didn't succeed.
	LastError string `json:"last_error,omitempty"`

	// All times come from Redis's clock, not the caller's.
	RunAt      time.Time `json:"run_at"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
	StartedAt  time.Time `json:"started_at,omitzero"`
	FinishedAt time.Time `json:"finished_at,omitzero"`
}

// FromHash builds a Job from the fields of its Redis hash. Times are stored
// as Unix milliseconds; missing optional fields are left at their zero value.
func FromHash(h map[string]string) (*Job, error) {
	if h["id"] == "" {
		return nil, errors.New("job hash has no id")
	}
	p := hashParser{h: h}
	j := &Job{
		ID:             h["id"],
		Type:           h["type"],
		Status:         Status(h["status"]),
		Attempts:       int(p.int("attempts")),
		IdempotencyKey: h["idem_key"],
		MaxAttempts:    int(p.int("max_attempts")),
		Timeout:        p.duration("timeout_ms"),
		BackoffBase:    p.duration("backoff_base_ms"),
		BackoffMax:     p.duration("backoff_max_ms"),
		LeaseOwner:     h["lease_owner"],
		LeaseToken:     p.int("lease_token"),
		LastError:      h["last_error"],
		RunAt:          p.millis("run_at"),
		CreatedAt:      p.millis("created_at"),
		UpdatedAt:      p.millis("updated_at"),
		StartedAt:      p.millis("started_at"),
		FinishedAt:     p.millis("finished_at"),
	}
	if payload := h["payload"]; payload != "" {
		j.Payload = json.RawMessage(payload)
	}
	if p.err != nil {
		return nil, fmt.Errorf("job %s: %w", j.ID, p.err)
	}
	return j, nil
}

// hashParser reads numeric fields and keeps the first error it hits.
type hashParser struct {
	h   map[string]string
	err error
}

func (p *hashParser) int(field string) int64 {
	v := p.h[field]
	if v == "" {
		return 0
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil && p.err == nil {
		p.err = fmt.Errorf("field %s: %q is not an integer", field, v)
	}
	return n
}

func (p *hashParser) duration(field string) time.Duration {
	return time.Duration(p.int(field)) * time.Millisecond
}

func (p *hashParser) millis(field string) time.Time {
	ms := p.int(field)
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms)
}
