package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/sham-33/job-scheduler/internal/job"
	"github.com/sham-33/job-scheduler/internal/monitor"
	"github.com/sham-33/job-scheduler/internal/queue"
)

// Duration is a time.Duration written in JSON as a string such as "500ms",
// "30s" or "1m30s".
type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return errors.New(`a duration must be a string such as "500ms", "30s" or "5m"`)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf(`%q is not a duration; use a string such as "500ms", "30s" or "5m"`, s)
	}
	*d = Duration(v)
	return nil
}

// JobRequest is the body of POST /jobs, and the job template of a cron.
// Every field but Type is optional.
type JobRequest struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
	// Delay postpones the job. A cron's template can't have one.
	Delay       Duration `json:"delay,omitempty"`
	MaxAttempts int      `json:"max_attempts,omitempty"`
	Timeout     Duration `json:"timeout,omitempty"`
	Backoff     Backoff  `json:"backoff,omitzero"`
}

// Backoff is the wait before each retry. It starts at Base and doubles, up
// to Max.
type Backoff struct {
	Base Duration `json:"base,omitempty"`
	Max  Duration `json:"max,omitempty"`
}

func (r JobRequest) spec(idempotencyKey string) queue.Spec {
	return queue.Spec{
		Type:           r.Type,
		Payload:        r.Payload,
		Delay:          time.Duration(r.Delay),
		IdempotencyKey: idempotencyKey,
		MaxAttempts:    r.MaxAttempts,
		Timeout:        time.Duration(r.Timeout),
		Backoff:        queue.Backoff{Base: time.Duration(r.Backoff.Base), Max: time.Duration(r.Backoff.Max)},
	}
}

func templateOf(s queue.Spec) JobRequest {
	return JobRequest{
		Type:        s.Type,
		Payload:     s.Payload,
		MaxAttempts: s.MaxAttempts,
		Timeout:     Duration(s.Timeout),
		Backoff:     Backoff{Base: Duration(s.Backoff.Base), Max: Duration(s.Backoff.Max)},
	}
}

// Job is a job as the API returns it. All times are on Redis's clock.
type Job struct {
	ID             string          `json:"id"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload,omitempty"`
	Status         job.Status      `json:"status"`
	Attempts       int             `json:"attempts"`
	MaxAttempts    int             `json:"max_attempts"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	Timeout        Duration        `json:"timeout,omitempty"`
	Backoff        Backoff         `json:"backoff"`
	// LeaseOwner is the worker slot running the job, or the last one that did.
	LeaseOwner string    `json:"lease_owner,omitempty"`
	LastError  string    `json:"last_error,omitempty"`
	RunAt      time.Time `json:"run_at"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
	StartedAt  time.Time `json:"started_at,omitzero"`
	FinishedAt time.Time `json:"finished_at,omitzero"`
}

func jobOf(j *job.Job) Job {
	return Job{
		ID:             j.ID,
		Type:           j.Type,
		Payload:        j.Payload,
		Status:         j.Status,
		Attempts:       j.Attempts,
		MaxAttempts:    j.MaxAttempts,
		IdempotencyKey: j.IdempotencyKey,
		Timeout:        Duration(j.Timeout),
		Backoff:        Backoff{Base: Duration(j.BackoffBase), Max: Duration(j.BackoffMax)},
		LeaseOwner:     j.LeaseOwner,
		LastError:      j.LastError,
		RunAt:          j.RunAt,
		CreatedAt:      j.CreatedAt,
		UpdatedAt:      j.UpdatedAt,
		StartedAt:      j.StartedAt,
		FinishedAt:     j.FinishedAt,
	}
}

// DLQPage is one page of the dead-letter queue, the most recent death first.
type DLQPage struct {
	Jobs []Job `json:"jobs"`
	// Total is how many jobs the dead-letter queue holds.
	Total int64 `json:"total"`
}

// PurgeResult is the reply to DELETE /dlq.
type PurgeResult struct {
	Purged int `json:"purged"`
}

// CronRequest is the body of POST /crons. Saving under a name that exists
// replaces that cron.
type CronRequest struct {
	Name     string     `json:"name"`
	Schedule string     `json:"schedule"`
	Job      JobRequest `json:"job"`
}

// CronPatch is the body of PATCH /crons/{name}.
type CronPatch struct {
	Enabled *bool `json:"enabled"`
}

// Cron is a cron as the API returns it.
type Cron struct {
	Name     string     `json:"name"`
	Schedule string     `json:"schedule"`
	Job      JobRequest `json:"job"`
	Enabled  bool       `json:"enabled"`
	// NextRun is the slot it fires for next. It is missing while disabled.
	NextRun   time.Time `json:"next_run,omitzero"`
	LastFired time.Time `json:"last_fired,omitzero"`
	LastJobID string    `json:"last_job_id,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func cronOf(c *queue.Cron) Cron {
	return Cron{
		Name:      c.Name,
		Schedule:  c.Schedule,
		Job:       templateOf(c.Job),
		Enabled:   c.Enabled,
		NextRun:   c.NextRun,
		LastFired: c.LastFired,
		LastJobID: c.LastJobID,
		CreatedAt: c.CreatedAt,
		UpdatedAt: c.UpdatedAt,
	}
}

// CronList is the reply to GET /crons: the cron that fires soonest first,
// disabled ones last.
type CronList struct {
	Crons []Cron `json:"crons"`
}

// EventsPage is the reply to GET /events, oldest event first. Pass LastID
// as ?after= to get the events that come next.
type EventsPage struct {
	Events []monitor.Event `json:"events"`
	LastID string          `json:"last_id"`
}

// ErrorBody is the body of every error reply.
type ErrorBody struct {
	Error string `json:"error"`
}
