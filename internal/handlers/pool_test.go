package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sham-33/job-scheduler/internal/job"
	"github.com/sham-33/job-scheduler/internal/queue"
	"github.com/sham-33/job-scheduler/internal/scheduler"
	"github.com/sham-33/job-scheduler/internal/store/storetest"
	"github.com/sham-33/job-scheduler/internal/worker"
)

// These tests run the real handlers in a real worker pool against Redis, to
// show each kind of failure ends where it should.

func startPool(t *testing.T) *queue.Queue {
	t.Helper()
	rdb := storetest.Client(t)
	q := queue.New(rdb, storetest.Keys(t, rdb), 2*time.Second)
	logger := slog.New(slog.DiscardHandler)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	opts := worker.Options{ID: "test", Concurrency: 4, HeartbeatEvery: 500 * time.Millisecond, ShutdownGrace: time.Second}
	wg.Go(func() { worker.NewPool(opts, q, Default(), logger).Run(ctx) })
	wg.Go(func() { scheduler.RunPromoter(ctx, q, logger) })
	wg.Go(func() { scheduler.RunReaper(ctx, q, 20*time.Millisecond, logger) })
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
	return q
}

func enqueue(t *testing.T, q *queue.Queue, s queue.Spec) string {
	t.Helper()
	id, _, err := q.Enqueue(context.Background(), s)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	return id
}

// waitDone waits for the job to succeed, die or be cancelled.
func waitDone(t *testing.T, q *queue.Queue, id string, timeout time.Duration) *job.Job {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		j, err := q.Get(context.Background(), id)
		if err == nil && (j.Status == job.StatusSucceeded || j.Status == job.StatusDead || j.Status == job.StatusCancelled) {
			return j
		}
		if time.Now().After(deadline) {
			t.Fatalf("job %s did not finish within %s: %+v (err %v)", id, timeout, j, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func httpSpec(url string) queue.Spec {
	payload, _ := json.Marshal(HTTPPayload{URL: url, Body: json.RawMessage(`{"order":1042}`)})
	return queue.Spec{Type: "http", Payload: payload, Backoff: queue.Backoff{Base: 20 * time.Millisecond, Max: 100 * time.Millisecond}}
}

func TestFlakyJobSucceedsAfterRetries(t *testing.T) {
	q := startPool(t)
	// At p=0.5, 30 attempts all fail about once in a billion runs.
	const jobs = 20
	ids := make([]string, jobs)
	for i := range ids {
		ids[i] = enqueue(t, q, queue.Spec{
			Type: "flaky", Payload: json.RawMessage(`{"p": 0.5}`), MaxAttempts: 30,
			Backoff: queue.Backoff{Base: 5 * time.Millisecond, Max: 40 * time.Millisecond},
		})
	}
	attempts, most := 0, 0
	for _, id := range ids {
		j := waitDone(t, q, id, 10*time.Second)
		if j.Status != job.StatusSucceeded {
			t.Fatalf("job = %+v, want succeeded", j)
		}
		attempts += j.Attempts
		most = max(most, j.Attempts)
	}
	t.Logf("%d flaky jobs (p=0.5) all succeeded: %d attempts in total, %d for the unluckiest", jobs, attempts, most)
	if attempts == jobs {
		t.Error("no job needed a retry, which p=0.5 makes all but impossible")
	}
}

func TestHTTPJobSucceeds(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer srv.Close()
	q := startPool(t)

	j := waitDone(t, q, enqueue(t, q, httpSpec(srv.URL)), 5*time.Second)
	if j.Status != job.StatusSucceeded || j.Attempts != 1 || calls.Load() != 1 {
		t.Errorf("job = %+v after %d calls, want succeeded on the first call", j, calls.Load())
	}
}

func TestHTTPJobRetriesUntilServerRecovers(t *testing.T) {
	var mu sync.Mutex
	var attempts []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		attempts = append(attempts, r.Header.Get("X-Job-Attempt"))
		n := len(attempts)
		mu.Unlock()
		if n <= 2 {
			http.Error(w, "database is down", http.StatusInternalServerError)
		}
	}))
	defer srv.Close()
	q := startPool(t)

	j := waitDone(t, q, enqueue(t, q, httpSpec(srv.URL)), 5*time.Second)
	if j.Status != job.StatusSucceeded || j.Attempts != 3 {
		t.Errorf("job = %+v, want succeeded on attempt 3", j)
	}
	if !strings.Contains(j.LastError, "500 Internal Server Error: database is down") {
		t.Errorf("LastError = %q, want the server's reply", j.LastError)
	}
	// The receiver could tell the calls apart.
	if got := fmt.Sprint(attempts); got != "[1 2 3]" {
		t.Errorf("X-Job-Attempt headers = %s, want [1 2 3]", got)
	}
}

func TestHTTPJobTimesOutAndRetries(t *testing.T) {
	var calls atomic.Int32
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		if calls.Add(1) == 1 {
			<-release // the first call never gets an answer
		}
	}))
	defer srv.Close()
	defer close(release)
	q := startPool(t)

	spec := httpSpec(srv.URL)
	spec.Timeout = 100 * time.Millisecond
	j := waitDone(t, q, enqueue(t, q, spec), 5*time.Second)
	if j.Status != job.StatusSucceeded || j.Attempts != 2 || j.LastError != "timed out after 100ms" {
		t.Errorf("job = %+v, want succeeded on attempt 2 after a timeout", j)
	}
}

func TestHTTPJobRejectedGoesStraightToDeadLetterQueue(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.Error(w, "order must be a string", http.StatusBadRequest)
	}))
	defer srv.Close()
	q := startPool(t)

	j := waitDone(t, q, enqueue(t, q, httpSpec(srv.URL)), 5*time.Second)
	time.Sleep(300 * time.Millisecond) // time for a wrong retry to show up
	if j.Status != job.StatusDead || j.Attempts != 1 || calls.Load() != 1 {
		t.Errorf("job = %+v after %d calls, want dead after a single call", j, calls.Load())
	}
	if !strings.Contains(j.LastError, "400 Bad Request: order must be a string") {
		t.Errorf("LastError = %q, want the server's reply", j.LastError)
	}
}

func TestHTTPJobDiesWhenServerNeverRecovers(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	q := startPool(t)

	j := waitDone(t, q, enqueue(t, q, httpSpec(srv.URL)), 10*time.Second)
	if j.Status != job.StatusDead || j.Attempts != queue.DefaultMaxAttempts || int(calls.Load()) != queue.DefaultMaxAttempts {
		t.Errorf("job = %+v after %d calls, want dead after %d", j, calls.Load(), queue.DefaultMaxAttempts)
	}
}
