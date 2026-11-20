package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/sham-33/job-scheduler/internal/monitor"
)

// Client calls the API.
type Client struct {
	base  string
	token string
	http  *http.Client
}

// NewClient returns a Client for the API at baseURL, such as
// "http://localhost:8080". token is sent as a bearer token if not empty.
func NewClient(baseURL, token string) *Client {
	return &Client{
		base:  strings.TrimRight(baseURL, "/"),
		token: token,
		http:  &http.Client{Timeout: 30 * time.Second},
	}
}

// Error is an error reply from the API.
type Error struct {
	Status  int    // HTTP status code
	Message string // the reply's error message
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s (HTTP %d)", e.Message, e.Status)
}

// IsNotFound reports whether err is a 404 reply.
func IsNotFound(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.Status == http.StatusNotFound
}

// SubmitJob submits a job. With an idempotency key that was already used,
// it returns the job that key made, and created is false.
func (c *Client) SubmitJob(ctx context.Context, req JobRequest, idempotencyKey string) (j Job, created bool, err error) {
	h := http.Header{}
	if idempotencyKey != "" {
		h.Set("Idempotency-Key", idempotencyKey)
	}
	code, err := c.do(ctx, http.MethodPost, "/jobs", h, req, &j)
	return j, code == http.StatusCreated, err
}

func (c *Client) GetJob(ctx context.Context, id string) (Job, error) {
	var j Job
	_, err := c.do(ctx, http.MethodGet, "/jobs/"+url.PathEscape(id), nil, nil, &j)
	return j, err
}

// CancelJob cancels a job that hasn't finished, and returns it.
func (c *Client) CancelJob(ctx context.Context, id string) (Job, error) {
	var j Job
	_, err := c.do(ctx, http.MethodDelete, "/jobs/"+url.PathEscape(id), nil, nil, &j)
	return j, err
}

// ListDLQ returns a page of the dead-letter queue, the most recent death
// first. A zero limit means the server's default.
func (c *Client) ListDLQ(ctx context.Context, offset, limit int) (DLQPage, error) {
	q := url.Values{}
	if offset > 0 {
		q.Set("offset", strconv.Itoa(offset))
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	var p DLQPage
	_, err := c.do(ctx, http.MethodGet, "/dlq?"+q.Encode(), nil, nil, &p)
	return p, err
}

// ReplayJob moves a dead job back to the ready queue with its attempts
// reset, and returns it.
func (c *Client) ReplayJob(ctx context.Context, id string) (Job, error) {
	var j Job
	_, err := c.do(ctx, http.MethodPost, "/dlq/"+url.PathEscape(id)+"/replay", nil, nil, &j)
	return j, err
}

// PurgeDLQ deletes every dead job and returns how many it deleted.
func (c *Client) PurgeDLQ(ctx context.Context) (int, error) {
	var r PurgeResult
	_, err := c.do(ctx, http.MethodDelete, "/dlq", nil, nil, &r)
	return r.Purged, err
}

// PutCron saves a cron, replacing one with the same name.
func (c *Client) PutCron(ctx context.Context, req CronRequest) (Cron, error) {
	var cr Cron
	_, err := c.do(ctx, http.MethodPost, "/crons", nil, req, &cr)
	return cr, err
}

func (c *Client) GetCron(ctx context.Context, name string) (Cron, error) {
	var cr Cron
	_, err := c.do(ctx, http.MethodGet, "/crons/"+url.PathEscape(name), nil, nil, &cr)
	return cr, err
}

func (c *Client) ListCrons(ctx context.Context) ([]Cron, error) {
	var l CronList
	_, err := c.do(ctx, http.MethodGet, "/crons", nil, nil, &l)
	return l.Crons, err
}

// SetCronEnabled turns a cron on or off and returns it.
func (c *Client) SetCronEnabled(ctx context.Context, name string, enabled bool) (Cron, error) {
	var cr Cron
	_, err := c.do(ctx, http.MethodPatch, "/crons/"+url.PathEscape(name), nil, CronPatch{Enabled: &enabled}, &cr)
	return cr, err
}

func (c *Client) DeleteCron(ctx context.Context, name string) error {
	_, err := c.do(ctx, http.MethodDelete, "/crons/"+url.PathEscape(name), nil, nil, nil)
	return err
}

// Stats returns counters, queue sizes, the leader and the workers.
func (c *Client) Stats(ctx context.Context) (monitor.Snapshot, error) {
	var s monitor.Snapshot
	_, err := c.do(ctx, http.MethodGet, "/stats", nil, nil, &s)
	return s, err
}

// Events returns up to limit events, oldest first: the latest ones if after
// is empty, otherwise the ones after that event ID. Pass the page's LastID
// as after to read on. A zero limit means the server's default.
func (c *Client) Events(ctx context.Context, after string, limit int) (EventsPage, error) {
	q := url.Values{}
	if after != "" {
		q.Set("after", after)
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	var p EventsPage
	_, err := c.do(ctx, http.MethodGet, "/events?"+q.Encode(), nil, nil, &p)
	return p, err
}

// Health checks that the API is up and can reach Redis.
func (c *Client) Health(ctx context.Context) error {
	_, err := c.do(ctx, http.MethodGet, "/healthz", nil, nil, nil)
	return err
}

// do sends a request with an optional JSON body and decodes a JSON reply
// into out. A reply of 400 or more becomes an *Error.
func (c *Client) do(ctx context.Context, method, path string, h http.Header, body, out any) (int, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return 0, err
	}
	for k, v := range h {
		req.Header[k] = v
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return resp.StatusCode, fmt.Errorf("read reply: %w", err)
	}
	if resp.StatusCode >= 400 {
		var eb ErrorBody
		if json.Unmarshal(data, &eb) != nil || eb.Error == "" {
			eb.Error = strings.TrimSpace(string(data))
			if eb.Error == "" {
				eb.Error = http.StatusText(resp.StatusCode)
			}
		}
		return resp.StatusCode, &Error{Status: resp.StatusCode, Message: eb.Error}
	}
	if out != nil && resp.StatusCode != http.StatusNoContent {
		if err := json.Unmarshal(data, out); err != nil {
			return resp.StatusCode, fmt.Errorf("decode reply: %w", err)
		}
	}
	return resp.StatusCode, nil
}
