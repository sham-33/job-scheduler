package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sham-33/job-scheduler/internal/api"
	"github.com/sham-33/job-scheduler/internal/job"
	"github.com/sham-33/job-scheduler/internal/monitor"
	"github.com/sham-33/job-scheduler/internal/queue"
	"github.com/sham-33/job-scheduler/internal/store/storetest"
)

// jobctl runs one command against url and returns what it printed.
func jobctl(t *testing.T, url, stdin string, args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	root := newRoot(&cli{out: &out, in: strings.NewReader(stdin)})
	root.SetErr(&out)
	root.SetArgs(append([]string{"--api", url, "--token", "t0k"}, args...))
	err := root.ExecuteContext(context.Background())
	return out.String(), err
}

func mustRun(t *testing.T, url string, args ...string) string {
	t.Helper()
	out, err := jobctl(t, url, "", args...)
	if err != nil {
		t.Fatalf("jobctl %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

func TestJobctl(t *testing.T) {
	rdb := storetest.Client(t)
	keys := storetest.Keys(t, rdb)
	q := queue.New(rdb, keys, time.Second)
	srv := httptest.NewServer(api.New(q, monitor.New(rdb, keys), api.Options{Token: "t0k"}, slog.New(slog.DiscardHandler)).Handler())
	t.Cleanup(srv.Close)
	url := srv.URL

	// submit prints the ID; get shows the job with every flag applied.
	id := strings.TrimSpace(mustRun(t, url, "submit", "echo", `{"n":1}`, "--delay", "1h",
		"--max-attempts", "3", "--timeout", "10s", "--backoff-base", "2s", "--backoff-max", "1m", "--key", "k1"))
	var j api.Job
	if err := json.Unmarshal([]byte(mustRun(t, url, "get", id)), &j); err != nil {
		t.Fatal(err)
	}
	var payload bytes.Buffer
	if err := json.Compact(&payload, j.Payload); err != nil {
		t.Fatal(err)
	}
	if j.ID != id || j.Status != "scheduled" || payload.String() != `{"n":1}` || j.MaxAttempts != 3 ||
		j.Timeout != api.Duration(10*time.Second) || j.Backoff.Base != api.Duration(2*time.Second) || j.IdempotencyKey != "k1" {
		t.Errorf("job = %+v", j)
	}
	// The same key prints the same ID, with a note.
	out, err := jobctl(t, url, "", "submit", "echo", "--key", "k1")
	if err != nil || !strings.Contains(out, "already used") || !strings.HasSuffix(out, id+"\n") {
		t.Errorf("resubmit = %q, %v", out, err)
	}
	// The payload can come from stdin.
	out, err = jobctl(t, url, `{"from":"stdin"}`, "submit", "echo", "-", "--json")
	if err != nil || !strings.Contains(out, `"from": "stdin"`) {
		t.Errorf("submit from stdin = %q, %v", out, err)
	}
	if _, err := jobctl(t, url, "", "submit", "echo", "{not json"); err == nil || !strings.Contains(err.Error(), "not valid JSON") {
		t.Errorf("submit with a bad payload: %v", err)
	}

	if out := mustRun(t, url, "cancel", id); out != id+"  cancelled\n" {
		t.Errorf("cancel = %q", out)
	}
	if _, err := jobctl(t, url, "", "cancel", id); err == nil || !strings.Contains(err.Error(), "HTTP 409") {
		t.Errorf("cancel twice: %v", err)
	}
	if _, err := jobctl(t, url, "", "get", "nope"); err == nil || !strings.Contains(err.Error(), "job not found (HTTP 404)") {
		t.Errorf("get unknown: %v", err)
	}

	// Dead-letter queue: the one ready job (from stdin) fails for good.
	cj, err := q.Claim(context.Background(), "w/0")
	if err != nil || cj == nil {
		t.Fatalf("Claim = %v, %v", cj, err)
	}
	deadID := cj.ID
	if _, dead, err := q.Fail(context.Background(), cj, job.Permanent(errors.New("it broke"))); err != nil || !dead {
		t.Fatalf("Fail = %v, %v", dead, err)
	}
	out = mustRun(t, url, "dlq", "list")
	if !strings.Contains(out, deadID) || !strings.Contains(out, "1/5") || !strings.Contains(out, "it broke") || !strings.Contains(out, "1 of 1 dead jobs") {
		t.Errorf("dlq list =\n%s", out)
	}
	if out := mustRun(t, url, "dlq", "replay", deadID); out != deadID+"  ready\n" {
		t.Errorf("dlq replay = %q", out)
	}
	// Kill it again, so there is something to purge.
	cj, err = q.Claim(context.Background(), "w/0")
	if err != nil || cj == nil || cj.ID != deadID {
		t.Fatalf("Claim = %v, %v; want the replayed job", cj, err)
	}
	if _, _, err := q.Fail(context.Background(), cj, job.Permanent(errors.New("it broke"))); err != nil {
		t.Fatal(err)
	}
	if _, err := jobctl(t, url, "", "dlq", "purge"); err == nil || !strings.Contains(err.Error(), "--yes") {
		t.Errorf("purge without --yes: %v", err)
	}
	if out := mustRun(t, url, "dlq", "purge", "--yes"); out != "deleted 1 dead jobs\n" {
		t.Errorf("dlq purge = %q", out)
	}

	// Crons.
	out = mustRun(t, url, "cron", "add", "nightly", "0 2 * * *", "http", `{"url":"http://example.com"}`, "--max-attempts", "2")
	if !strings.HasPrefix(out, "cron nightly (0 2 * * *): next run ") {
		t.Errorf("cron add = %q", out)
	}
	out = mustRun(t, url, "cron", "list")
	if !strings.Contains(out, "nightly") || !strings.Contains(out, "http") || !strings.Contains(out, "true") {
		t.Errorf("cron list =\n%s", out)
	}
	if out := mustRun(t, url, "cron", "disable", "nightly"); out != "cron nightly (0 2 * * *): disabled\n" {
		t.Errorf("cron disable = %q", out)
	}
	if out := mustRun(t, url, "cron", "enable", "nightly"); !strings.Contains(out, "next run") {
		t.Errorf("cron enable = %q", out)
	}
	if _, err := jobctl(t, url, "", "cron", "add", "bad", "61 * * * *", "echo"); err == nil || !strings.Contains(err.Error(), "HTTP 400") {
		t.Errorf("cron add with a bad schedule: %v", err)
	}
	if out := mustRun(t, url, "cron", "rm", "nightly"); out != "deleted cron nightly\n" {
		t.Errorf("cron rm = %q", out)
	}

	// Stats and events.
	out = mustRun(t, url, "stats")
	for _, want := range []string{"leader   - (term 0)", "queues   scheduled 0 · ready 0 · running 0 · dead 0",
		"submitted 2", "dead 2", "cancelled 1", "replayed 1", "purged 1"} {
		if !strings.Contains(out, want) {
			t.Errorf("stats is missing %q:\n%s", want, out)
		}
	}
	out = mustRun(t, url, "events", "-n", "3")
	if lines := strings.Split(strings.TrimSpace(out), "\n"); len(lines) != 3 || !strings.Contains(lines[2], "purged") ||
		!strings.Contains(lines[1], "dead") || !strings.Contains(lines[1], "it broke") {
		t.Errorf("events -n 3 =\n%s", out)
	}

	// A wrong token is refused.
	var buf bytes.Buffer
	root := newRoot(&cli{out: &buf, in: strings.NewReader("")})
	root.SetArgs([]string{"--api", url, "--token", "wrong", "stats"})
	if err := root.ExecuteContext(context.Background()); err == nil || !strings.Contains(err.Error(), "HTTP 401") {
		t.Errorf("wrong token: %v", err)
	}
}
