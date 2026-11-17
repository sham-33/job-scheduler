package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sham-33/job-scheduler/internal/job"
)

func httpJob(t *testing.T, p HTTPPayload) *job.Job {
	t.Helper()
	payload, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return &job.Job{ID: "job-42", Attempts: 3, Payload: payload}
}

func TestHTTPSendsRequest(t *testing.T) {
	type seen struct {
		method, path, body string
		header             http.Header
	}
	got := make(chan seen, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		got <- seen{r.Method, r.URL.Path, string(body), r.Header}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	err := HTTP(context.Background(), httpJob(t, HTTPPayload{
		URL:     srv.URL + "/hooks/send-receipt",
		Headers: map[string]string{"Authorization": "Bearer test-token"},
		Body:    json.RawMessage(`{"order":1042}`),
	}))
	if err != nil {
		t.Fatalf("HTTP: %v", err)
	}
	r := <-got
	if r.method != http.MethodPost || r.path != "/hooks/send-receipt" || r.body != `{"order":1042}` {
		t.Errorf("request = %s %s with body %s, want POST /hooks/send-receipt with the payload's body", r.method, r.path, r.body)
	}
	for name, want := range map[string]string{
		"Content-Type":  "application/json",
		"Authorization": "Bearer test-token",
		"X-Job-Id":      "job-42",
		"X-Job-Attempt": "3",
	} {
		if got := r.header.Get(name); got != want {
			t.Errorf("header %s = %q, want %q", name, got, want)
		}
	}
}

func TestHTTPUsesPayloadMethod(t *testing.T) {
	got := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got <- r.Method + " " + r.Header.Get("Content-Type")
	}))
	defer srv.Close()

	if err := HTTP(context.Background(), httpJob(t, HTTPPayload{URL: srv.URL, Method: "delete"})); err != nil {
		t.Fatalf("HTTP: %v", err)
	}
	// No body, so no content type either.
	if r := <-got; r != "DELETE " {
		t.Errorf("request = %q, want a DELETE with no Content-Type", r)
	}
}

func TestHTTPReplyDecidesOutcome(t *testing.T) {
	tests := []struct {
		status    int
		ok, retry bool
	}{
		{200, true, false},
		{201, true, false},
		{204, true, false},
		{500, false, true},
		{502, false, true},
		{503, false, true},
		{408, false, true},
		{429, false, true},
		{400, false, false},
		{401, false, false},
		{404, false, false},
		{422, false, false},
	}
	for _, tt := range tests {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(tt.status)
			_, _ = io.WriteString(w, "reason: "+strings.Repeat("x", 1000))
		}))
		err := HTTP(context.Background(), httpJob(t, HTTPPayload{URL: srv.URL}))
		srv.Close()

		switch {
		case tt.ok && err != nil:
			t.Errorf("status %d: HTTP = %v, want success", tt.status, err)
		case !tt.ok && err == nil:
			t.Errorf("status %d: HTTP succeeded, want an error", tt.status)
		case !tt.ok && job.IsPermanent(err) == tt.retry:
			t.Errorf("status %d: permanent = %v, want retryable = %v", tt.status, job.IsPermanent(err), tt.retry)
		case !tt.ok && (!strings.Contains(err.Error(), "reason: x") || len(err.Error()) > 500):
			t.Errorf("status %d: error should carry a short piece of the reply, got %d bytes: %.80s", tt.status, len(err.Error()), err)
		}
	}
}

func TestHTTPNetworkErrorIsRetryable(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close() // nothing is listening there any more

	err := HTTP(context.Background(), httpJob(t, HTTPPayload{URL: url}))
	if err == nil || job.IsPermanent(err) {
		t.Errorf("HTTP to a closed port = %v, want a retryable error", err)
	}
}

func TestHTTPStopsWhenCancelled(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))
	defer srv.Close()
	defer close(release)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := HTTP(ctx, httpJob(t, HTTPPayload{URL: srv.URL}))
	if !errors.Is(err, context.DeadlineExceeded) || job.IsPermanent(err) {
		t.Errorf("HTTP = %v, want a retryable deadline error", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Errorf("HTTP took %s to stop, want about 50ms", took)
	}
}

func TestHTTPBadPayload(t *testing.T) {
	for name, payload := range map[string]string{
		"empty":         ``,
		"not an object": `"https://example.com"`,
		"no url":        `{}`,
		"no scheme":     `{"url": "example.com/hook"}`,
		"wrong scheme":  `{"url": "ftp://example.com/hook"}`,
		"no host":       `{"url": "http:///hook"}`,
		"bad method":    `{"url": "http://example.com", "method": "NOT A METHOD"}`,
	} {
		j := &job.Job{ID: "j1", Payload: json.RawMessage(payload)}
		if err := HTTP(context.Background(), j); !job.IsPermanent(err) {
			t.Errorf("%s: HTTP = %v, want a permanent error", name, err)
		}
	}
}

func TestHTTPJobTimeoutReplacesDefaultLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
	}))
	defer srv.Close()
	const limit = 100 * time.Millisecond // stands in for the 30s default

	// A job with a longer timeout of its own gets all of it.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := callHTTP(ctx, httpJob(t, HTTPPayload{URL: srv.URL}), limit); err != nil {
		t.Errorf("job with a 2s timeout: %v, want success", err)
	}

	// A job without one is cut off at the default, and retried.
	err := callHTTP(context.Background(), httpJob(t, HTTPPayload{URL: srv.URL}), limit)
	if !errors.Is(err, context.DeadlineExceeded) || job.IsPermanent(err) {
		t.Errorf("job with no timeout = %v, want a retryable deadline error", err)
	}
}

func TestHTTPErrorsHideSecretsInURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	closed := httptest.NewServer(http.NotFoundHandler())
	closed.Close()

	for name, base := range map[string]string{"error reply": srv.URL, "no server": closed.URL} {
		host := strings.TrimPrefix(base, "http://")
		// The secret is in the user info, the path and the query, as webhook URLs have it.
		u := "http://user:pass-s3cr3t@" + host + "/hooks/T0/path-s3cr3t?token=query-s3cr3t"
		err := HTTP(context.Background(), httpJob(t, HTTPPayload{URL: u}))
		if err == nil {
			t.Fatalf("%s: HTTP succeeded, want an error", name)
		}
		if strings.Contains(err.Error(), "s3cr3t") {
			t.Errorf("%s: error shows a secret: %v", name, err)
		}
		if !strings.Contains(err.Error(), "POST http://"+host) {
			t.Errorf("%s: error %q should still name the method and host", name, err)
		}
	}

	bad := HTTP(context.Background(), httpJob(t, HTTPPayload{URL: "ftp://x/key-s3cr3t"}))
	if bad == nil || strings.Contains(bad.Error(), "s3cr3t") {
		t.Errorf("bad url: error = %v, want one without the secret", bad)
	}
}
