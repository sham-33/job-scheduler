package handlers

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

	"github.com/sham-33/job-scheduler/internal/job"
)

const (
	// httpTimeout bounds one call for jobs that set no timeout of their own,
	// so a server that never answers can't hold a worker slot forever.
	httpTimeout = 30 * time.Second
	// maxReplySnippet is how much of a failed reply's body goes in the error.
	maxReplySnippet = 200
)

// httpClient has no Timeout of its own: that would also cut off jobs that
// asked for a longer one. callHTTP applies httpTimeout instead.
var httpClient = &http.Client{}

// HTTPPayload is the payload of an "http" job, for example
// {"url": "https://example.com/hooks/send-receipt", "body": {"order": 1042}}.
type HTTPPayload struct {
	URL string `json:"url"`
	// Method defaults to POST.
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers"`
	// Body is sent as it is, as JSON. Leaving it out, or null, sends no body.
	Body json.RawMessage `json:"body"`
}

// HTTP calls the payload's URL, so the real work can live in any service,
// written in any language, without changing the worker. The reply decides
// what happens to the job:
//
//   - 2xx: the job succeeded.
//   - 5xx, 408, 429, a network error or a timeout: the job is retried,
//     because the receiver is likely to recover.
//   - anything else: the job fails for good, because the same request would
//     only be refused again.
//
// Each call carries X-Job-Id and X-Job-Attempt headers. A job can be
// delivered more than once, and the receiver can use the ID to spot a repeat.
//
// Errors name only the URL's scheme and host. Webhook URLs often carry a
// secret in the path or query string, and errors end up in logs and on the
// job, where more people can read them than can read the payload.
func HTTP(ctx context.Context, j *job.Job) error {
	return callHTTP(ctx, j, httpTimeout)
}

// callHTTP is HTTP with the limit for jobs that set no timeout as a
// parameter, so tests don't have to wait 30s.
func callHTTP(ctx context.Context, j *job.Job, defaultTimeout time.Duration) error {
	var p HTTPPayload
	if err := json.Unmarshal(j.Payload, &p); err != nil {
		return job.Permanent(fmt.Errorf("http: bad payload: %w", err))
	}
	u, err := url.Parse(p.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return job.Permanent(errors.New("http: url must be a full http or https address"))
	}
	// u.Host never includes a user name or password.
	where := u.Scheme + "://" + u.Host
	method := strings.ToUpper(p.Method)
	if method == "" {
		method = http.MethodPost
	}
	// A job's own timeout, applied by the worker, replaces the default.
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultTimeout)
		defer cancel()
	}

	var body io.Reader
	if len(p.Body) > 0 && string(p.Body) != "null" {
		body = bytes.NewReader(p.Body)
	}
	req, err := http.NewRequestWithContext(ctx, method, p.URL, body)
	if err != nil {
		return job.Permanent(fmt.Errorf("http: %w", err))
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for name, value := range p.Headers {
		req.Header.Set(name, value)
	}
	req.Header.Set("X-Job-Id", j.ID)
	req.Header.Set("X-Job-Attempt", strconv.Itoa(j.Attempts))

	resp, err := httpClient.Do(req)
	if err != nil {
		// Go's error repeats the whole URL, so keep only the cause.
		var uerr *url.Error
		if errors.As(err, &uerr) {
			err = uerr.Err
		}
		return fmt.Errorf("http: %s %s: %w", method, where, err)
	}
	defer resp.Body.Close()
	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, maxReplySnippet))

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	err = fmt.Errorf("http: %s %s replied %s", method, where, resp.Status)
	if text := strings.TrimSpace(string(snippet)); text != "" {
		err = fmt.Errorf("%w: %s", err, text)
	}
	if resp.StatusCode >= 500 || resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusTooManyRequests {
		return err
	}
	return job.Permanent(err)
}
