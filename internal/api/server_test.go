package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/sham-33/job-scheduler/internal/api"
	"github.com/sham-33/job-scheduler/internal/job"
	"github.com/sham-33/job-scheduler/internal/monitor"
	"github.com/sham-33/job-scheduler/internal/queue"
	"github.com/sham-33/job-scheduler/internal/store/storetest"
)

type env struct {
	q   *queue.Queue
	c   *api.Client
	url string
}

func newEnv(t *testing.T, opts api.Options) env {
	t.Helper()
	rdb := storetest.Client(t)
	keys := storetest.Keys(t, rdb)
	q := queue.New(rdb, keys, 2*time.Second)
	srv := httptest.NewServer(api.New(q, monitor.New(rdb, keys), opts, slog.New(slog.DiscardHandler)).Handler())
	t.Cleanup(srv.Close)
	return env{q: q, c: api.NewClient(srv.URL, opts.Token), url: srv.URL}
}

// wantStatus checks err is an API error with the given status, and that its
// message contains msg.
func wantStatus(t *testing.T, what string, err error, status int, msg string) {
	t.Helper()
	var e *api.Error
	if !errors.As(err, &e) || e.Status != status || !strings.Contains(e.Message, msg) {
		t.Errorf("%s: error = %v, want HTTP %d containing %q", what, err, status, msg)
	}
}

// raw sends a request without the client, for what the client can't send.
func raw(t *testing.T, method, url, body string, header ...string) (int, http.Header, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	var m map[string]any
	if len(data) > 0 {
		if err := json.Unmarshal(data, &m); err != nil {
			t.Fatalf("%s %s replied with non-JSON %q", method, url, data)
		}
	}
	return resp.StatusCode, resp.Header, m
}

func TestSubmitAndGetJob(t *testing.T) {
	e := newEnv(t, api.Options{})
	ctx := context.Background()

	req := api.JobRequest{
		Type: "http", Payload: json.RawMessage(`{"url":"http://example.com"}`),
		Delay: api.Duration(time.Hour), MaxAttempts: 3, Timeout: api.Duration(10 * time.Second),
		Backoff: api.Backoff{Base: api.Duration(2 * time.Second), Max: api.Duration(time.Minute)},
	}
	j, created, err := e.c.SubmitJob(ctx, req, "order-1")
	if err != nil || !created {
		t.Fatalf("SubmitJob = %v, %v", created, err)
	}
	if j.Type != "http" || string(j.Payload) != `{"url":"http://example.com"}` || j.Status != job.StatusScheduled ||
		j.MaxAttempts != 3 || j.Timeout != api.Duration(10*time.Second) || j.IdempotencyKey != "order-1" ||
		j.Backoff != (api.Backoff{Base: api.Duration(2 * time.Second), Max: api.Duration(time.Minute)}) {
		t.Errorf("submitted job = %+v", j)
	}
	if d := j.RunAt.Sub(j.CreatedAt); d != time.Hour {
		t.Errorf("run_at is %s after created_at, want 1h", d)
	}

	got, err := e.c.GetJob(ctx, j.ID)
	if err != nil || got.ID != j.ID || got.Status != job.StatusScheduled {
		t.Errorf("GetJob = %+v, %v", got, err)
	}

	// The same key returns the first job, with 200 instead of 201.
	again, created, err := e.c.SubmitJob(ctx, api.JobRequest{Type: "echo"}, "order-1")
	if err != nil || created || again.ID != j.ID {
		t.Errorf("resubmit with the same key = %s, created %v, %v; want %s", again.ID, created, err, j.ID)
	}

	// Defaults are filled in and shown.
	d, _, err := e.c.SubmitJob(ctx, api.JobRequest{Type: "echo"}, "")
	if err != nil || d.Status != job.StatusReady || d.MaxAttempts != queue.DefaultMaxAttempts ||
		d.Backoff.Base != api.Duration(queue.DefaultBackoffBase) || d.Timeout != 0 {
		t.Errorf("job with defaults = %+v, %v", d, err)
	}

	code, h, body := raw(t, "POST", e.url+"/jobs", `{"type":"echo"}`)
	if code != http.StatusCreated || h.Get("Location") != "/jobs/"+body["id"].(string) {
		t.Errorf("POST /jobs = %d, Location %q, body %v", code, h.Get("Location"), body)
	}

	_, err = e.c.GetJob(ctx, "no-such-job")
	wantStatus(t, "get unknown job", err, http.StatusNotFound, "job not found")
	if !api.IsNotFound(err) {
		t.Error("IsNotFound = false for a 404")
	}
}

func TestSubmitRejectsBadRequests(t *testing.T) {
	e := newEnv(t, api.Options{})
	ctx := context.Background()

	_, _, err := e.c.SubmitJob(ctx, api.JobRequest{}, "")
	wantStatus(t, "no type", err, http.StatusBadRequest, "job type is required")
	_, _, err = e.c.SubmitJob(ctx, api.JobRequest{Type: "echo", MaxAttempts: -1}, "")
	wantStatus(t, "negative attempts", err, http.StatusBadRequest, "max attempts")
	_, _, err = e.c.SubmitJob(ctx, api.JobRequest{Type: "echo"}, "cron:mine")
	wantStatus(t, "reserved key", err, http.StatusBadRequest, "reserved")
	_, _, err = e.c.SubmitJob(ctx, api.JobRequest{Type: "echo"}, strings.Repeat("k", 201))
	wantStatus(t, "long key", err, http.StatusBadRequest, "longer than the limit")

	for name, tc := range map[string]struct {
		body string
		code int
		msg  string
	}{
		"empty body":    {"", 400, "body is empty"},
		"not JSON":      {"{type", 400, "invalid JSON"},
		"unknown field": {`{"type":"echo","max_attempt":3}`, 400, `unknown field "max_attempt"`},
		"number delay":  {`{"type":"echo","delay":5000}`, 400, "duration must be a string"},
		"bad delay":     {`{"type":"echo","delay":"5 minutes"}`, 400, "not a duration"},
		"two objects":   {`{"type":"echo"}{"type":"echo"}`, 400, "single JSON object"},
		"too big":       {`{"type":"echo","payload":"` + strings.Repeat("x", 1<<20) + `"}`, 413, "larger than"},
	} {
		code, _, body := raw(t, "POST", e.url+"/jobs", tc.body)
		if msg, _ := body["error"].(string); code != tc.code || !strings.Contains(msg, tc.msg) {
			t.Errorf("%s: %d %v, want %d containing %q", name, code, body, tc.code, tc.msg)
		}
	}
	if s, err := e.c.Stats(ctx); err != nil || s.Counters["submitted"] != 0 {
		t.Errorf("after only rejected requests, submitted = %d (%v), want 0", s.Counters["submitted"], err)
	}
}

func TestCancelJob(t *testing.T) {
	e := newEnv(t, api.Options{})
	ctx := context.Background()
	j, _, err := e.c.SubmitJob(ctx, api.JobRequest{Type: "echo"}, "")
	if err != nil {
		t.Fatal(err)
	}
	got, err := e.c.CancelJob(ctx, j.ID)
	if err != nil || got.Status != job.StatusCancelled || got.FinishedAt.IsZero() {
		t.Errorf("CancelJob = %+v, %v", got, err)
	}
	_, err = e.c.CancelJob(ctx, j.ID)
	wantStatus(t, "cancel twice", err, http.StatusConflict, "already finished")
	_, err = e.c.CancelJob(ctx, "no-such-job")
	wantStatus(t, "cancel unknown", err, http.StatusNotFound, "not found")
}

func TestDeadLetterQueue(t *testing.T) {
	e := newEnv(t, api.Options{})
	ctx := context.Background()
	var dead []string
	for i := range 3 {
		id, _, err := e.q.Enqueue(ctx, queue.Spec{Type: "work"})
		if err != nil {
			t.Fatal(err)
		}
		j, err := e.q.Claim(ctx, "w/0")
		if err != nil || j == nil || j.ID != id {
			t.Fatalf("Claim = %v, %v", j, err)
		}
		if _, _, err := e.q.Fail(ctx, j, job.Permanent(errors.New("bad input"))); err != nil {
			t.Fatal(err)
		}
		dead = append(dead, id)
		if i < 2 {
			time.Sleep(2 * time.Millisecond)
		}
	}

	page, err := e.c.ListDLQ(ctx, 0, 0)
	if err != nil || page.Total != 3 || len(page.Jobs) != 3 || page.Jobs[0].ID != dead[2] || page.Jobs[0].LastError != "bad input" {
		t.Fatalf("ListDLQ = %+v, %v; want 3 jobs, newest first", page, err)
	}
	page, err = e.c.ListDLQ(ctx, 2, 5)
	if err != nil || page.Total != 3 || len(page.Jobs) != 1 || page.Jobs[0].ID != dead[0] {
		t.Errorf("ListDLQ(2, 5) = %+v, %v; want the oldest", page, err)
	}
	for _, q := range []string{"limit=0", "limit=1001", "offset=-1", "limit=ten"} {
		if code, _, _ := raw(t, "GET", e.url+"/dlq?"+q, ""); code != http.StatusBadRequest {
			t.Errorf("GET /dlq?%s = %d, want 400", q, code)
		}
	}

	j, err := e.c.ReplayJob(ctx, dead[0])
	if err != nil || j.Status != job.StatusReady || j.Attempts != 0 {
		t.Errorf("ReplayJob = %+v, %v; want ready with attempts reset", j, err)
	}
	_, err = e.c.ReplayJob(ctx, dead[0])
	wantStatus(t, "replay a job that isn't dead", err, http.StatusConflict, "not in the dead-letter queue")

	n, err := e.c.PurgeDLQ(ctx)
	if err != nil || n != 2 {
		t.Errorf("PurgeDLQ = %d, %v; want 2", n, err)
	}
	if page, err = e.c.ListDLQ(ctx, 0, 0); err != nil || page.Total != 0 || len(page.Jobs) != 0 {
		t.Errorf("ListDLQ after purge = %+v, %v; want empty", page, err)
	}
	_, err = e.c.GetJob(ctx, dead[1])
	wantStatus(t, "get a purged job", err, http.StatusNotFound, "not found")
}

func TestCrons(t *testing.T) {
	e := newEnv(t, api.Options{})
	ctx := context.Background()

	req := api.CronRequest{Name: "nightly", Schedule: "0 2 * * *", Job: api.JobRequest{
		Type: "http", Payload: json.RawMessage(`{"url":"http://example.com"}`), MaxAttempts: 2, Timeout: api.Duration(time.Minute),
	}}
	c, err := e.c.PutCron(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if c.Name != "nightly" || !c.Enabled || c.NextRun.IsZero() || c.NextRun.UTC().Hour() != 2 ||
		c.Job.Type != "http" || c.Job.MaxAttempts != 2 || c.Job.Timeout != api.Duration(time.Minute) {
		t.Errorf("PutCron = %+v", c)
	}
	if _, err := e.c.PutCron(ctx, api.CronRequest{Name: "hourly", Schedule: "@hourly", Job: api.JobRequest{Type: "echo"}}); err != nil {
		t.Fatal(err)
	}

	crons, err := e.c.ListCrons(ctx)
	if err != nil || len(crons) != 2 || crons[0].Name != "hourly" {
		t.Errorf("ListCrons = %+v, %v; want hourly first (it fires sooner)", crons, err)
	}

	c, err = e.c.SetCronEnabled(ctx, "nightly", false)
	if err != nil || c.Enabled || !c.NextRun.IsZero() {
		t.Errorf("disable = %+v, %v", c, err)
	}
	if c, err = e.c.GetCron(ctx, "nightly"); err != nil || c.Enabled {
		t.Errorf("GetCron after disable = %+v, %v", c, err)
	}
	if c, err = e.c.SetCronEnabled(ctx, "nightly", true); err != nil || !c.Enabled || c.NextRun.IsZero() {
		t.Errorf("enable = %+v, %v", c, err)
	}
	if code, _, body := raw(t, "PATCH", e.url+"/crons/nightly", `{}`); code != http.StatusBadRequest {
		t.Errorf("PATCH without enabled = %d %v, want 400", code, body)
	}

	_, err = e.c.PutCron(ctx, api.CronRequest{Name: "bad", Schedule: "61 * * * *", Job: api.JobRequest{Type: "echo"}})
	wantStatus(t, "bad schedule", err, http.StatusBadRequest, "schedule")
	_, err = e.c.PutCron(ctx, api.CronRequest{Name: "late", Schedule: "@hourly", Job: api.JobRequest{Type: "echo", Delay: api.Duration(time.Second)}})
	wantStatus(t, "template with a delay", err, http.StatusBadRequest, "delay")

	if err := e.c.DeleteCron(ctx, "nightly"); err != nil {
		t.Fatal(err)
	}
	_, err = e.c.GetCron(ctx, "nightly")
	wantStatus(t, "get deleted cron", err, http.StatusNotFound, "cron not found")
	wantStatus(t, "delete it again", e.c.DeleteCron(ctx, "nightly"), http.StatusNotFound, "cron not found")
	_, err = e.c.SetCronEnabled(ctx, "nightly", true)
	wantStatus(t, "enable deleted cron", err, http.StatusNotFound, "cron not found")
}

func TestStatsAndEvents(t *testing.T) {
	e := newEnv(t, api.Options{})
	ctx := context.Background()
	var ids []string
	for range 5 {
		j, _, err := e.c.SubmitJob(ctx, api.JobRequest{Type: "echo"}, "")
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, j.ID)
	}

	s, err := e.c.Stats(ctx)
	if err != nil || s.Counters["submitted"] != 5 || s.Queues.Ready != 5 || s.Leader != "" || len(s.Workers) != 0 {
		t.Errorf("Stats = %+v, %v; want 5 submitted and ready", s, err)
	}

	// Without after: the latest events, oldest first.
	page, err := e.c.Events(ctx, "", 2)
	if err != nil || len(page.Events) != 2 || page.Events[0].JobID != ids[3] || page.Events[1].JobID != ids[4] ||
		page.LastID != page.Events[1].ID {
		t.Errorf("Events(latest 2) = %+v, %v; want jobs 4 and 5", page, err)
	}
	// Paging from the start sees each event once, in order.
	first, err := e.c.Events(ctx, "0-0", 3)
	if err != nil || len(first.Events) != 3 || first.Events[0].JobID != ids[0] {
		t.Fatalf("Events(after 0-0) = %+v, %v", first, err)
	}
	rest, err := e.c.Events(ctx, first.LastID, 100)
	if err != nil || len(rest.Events) != 2 || rest.Events[1].JobID != ids[4] {
		t.Errorf("Events(after first page) = %+v, %v", rest, err)
	}
	// Nothing new keeps the position.
	if none, err := e.c.Events(ctx, rest.LastID, 100); err != nil || len(none.Events) != 0 || none.LastID != rest.LastID {
		t.Errorf("Events(at the end) = %+v, %v; want none and the same last_id", none, err)
	}
	_, err = e.c.Events(ctx, "not-an-id", 10)
	wantStatus(t, "malformed event ID", err, http.StatusBadRequest, "event ID")
	if code, _, _ := raw(t, "GET", e.url+"/events?limit=5000", ""); code != http.StatusBadRequest {
		t.Errorf("GET /events?limit=5000 = %d, want 400", code)
	}
}

func TestUnknownRoutesAnswerInJSON(t *testing.T) {
	e := newEnv(t, api.Options{})
	code, _, body := raw(t, "GET", e.url+"/nope", "")
	if code != http.StatusNotFound || body["error"] != "Not Found" {
		t.Errorf("GET /nope = %d %v, want a JSON 404", code, body)
	}
	code, h, body := raw(t, "PUT", e.url+"/jobs", "")
	if code != http.StatusMethodNotAllowed || body["error"] != "Method Not Allowed" || !strings.Contains(h.Get("Allow"), "POST") {
		t.Errorf("PUT /jobs = %d %v Allow %q, want a JSON 405 allowing POST", code, body, h.Get("Allow"))
	}
}

func TestToken(t *testing.T) {
	e := newEnv(t, api.Options{Token: "s3cret"})
	ctx := context.Background()
	if _, err := e.c.Stats(ctx); err != nil {
		t.Errorf("with the token: %v", err)
	}
	_, err := api.NewClient(e.url, "").Stats(ctx)
	wantStatus(t, "no token", err, http.StatusUnauthorized, "API token")
	_, err = api.NewClient(e.url, "wrong").Stats(ctx)
	wantStatus(t, "wrong token", err, http.StatusUnauthorized, "API token")
	code, h, _ := raw(t, "POST", e.url+"/jobs", `{"type":"echo"}`, "Authorization", "s3cret")
	if code != http.StatusUnauthorized || !strings.HasPrefix(h.Get("WWW-Authenticate"), "Bearer") {
		t.Errorf("token without Bearer = %d, want 401 with a Bearer challenge", code)
	}
	// The health check needs no token.
	if err := api.NewClient(e.url, "").Health(ctx); err != nil {
		t.Errorf("health check without a token: %v", err)
	}
}

func TestRedisDown(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:1", MaxRetries: -1})
	t.Cleanup(func() { _ = rdb.Close() })
	keys := storetest.Keys(t, storetest.Client(t))
	srv := httptest.NewServer(api.New(queue.New(rdb, keys, time.Second), monitor.New(rdb, keys),
		api.Options{}, slog.New(slog.DiscardHandler)).Handler())
	t.Cleanup(srv.Close)
	c := api.NewClient(srv.URL, "")
	ctx := context.Background()

	wantStatus(t, "health", c.Health(ctx), http.StatusServiceUnavailable, "Redis is unreachable")
	// Redis being down is temporary, not a bug: 503, with a hint to retry,
	// and no details such as its address.
	_, err := c.GetJob(ctx, "x")
	wantStatus(t, "get job", err, http.StatusServiceUnavailable, "Redis is unavailable; try again shortly")
	if strings.Contains(err.Error(), "localhost:1") {
		t.Errorf("error %q reveals the Redis address", err)
	}
	if _, h, _ := raw(t, "GET", srv.URL+"/stats", ""); h.Get("Retry-After") != "1" {
		t.Errorf("Retry-After = %q, want 1", h.Get("Retry-After"))
	}
}

func TestInternalErrorsHideDetails(t *testing.T) {
	rdb := storetest.Client(t)
	keys := storetest.Keys(t, rdb)
	srv := httptest.NewServer(api.New(queue.New(rdb, keys, time.Second), monitor.New(rdb, keys),
		api.Options{}, slog.New(slog.DiscardHandler)).Handler())
	t.Cleanup(srv.Close)
	// A job hash that can't be read, as if edited by hand.
	if err := rdb.HSet(context.Background(), keys.Job("broken"), "id", "broken", "attempts", "many").Err(); err != nil {
		t.Fatal(err)
	}
	_, err := api.NewClient(srv.URL, "").GetJob(context.Background(), "broken")
	wantStatus(t, "unreadable job", err, http.StatusInternalServerError, "internal error")
	if strings.Contains(err.Error(), "many") {
		t.Errorf("error %q reveals internal details", err)
	}
}

func TestDashboard(t *testing.T) {
	e := newEnv(t, api.Options{Token: "s3cret"})

	// The page and its files need no token: they hold no data.
	for path, wantType := range map[string]string{
		"/":                 "text/html",
		"/assets/app.js":    "text/javascript",
		"/assets/style.css": "text/css",
		"/assets/icon.svg":  "image/svg+xml",
	} {
		resp, err := http.Get(e.url + path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || !strings.HasPrefix(resp.Header.Get("Content-Type"), wantType) || len(body) == 0 {
			t.Errorf("GET %s = %d %q (%d bytes), want 200 %s", path, resp.StatusCode, resp.Header.Get("Content-Type"), len(body), wantType)
		}
		if csp := resp.Header.Get("Content-Security-Policy"); !strings.Contains(csp, "script-src 'self'") || strings.Contains(csp, "unsafe") {
			t.Errorf("GET %s: Content-Security-Policy = %q", path, csp)
		}
		if resp.Header.Get("X-Content-Type-Options") != "nosniff" {
			t.Errorf("GET %s: no X-Content-Type-Options: nosniff", path)
		}
	}
	resp, err := http.Get(e.url + "/")
	if err != nil {
		t.Fatal(err)
	}
	page, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(page), `<script src="/assets/app.js"`) {
		t.Error("the page does not load app.js")
	}

	// No directory listings, and anything not public still needs the token.
	for path, want := range map[string]int{"/assets/": 404, "/assets/nope.js": 404, "/stats": 401, "/index.html": 401} {
		if code, _, _ := raw(t, "GET", e.url+path, ""); code != want {
			t.Errorf("GET %s = %d, want %d", path, code, want)
		}
	}
}

func TestCrossSiteRequestsAreRefused(t *testing.T) {
	e := newEnv(t, api.Options{}) // no token: the case this matters most for
	body := `{"type":"echo"}`

	// A page on another site posting to the API is refused, whether the
	// browser says so with Sec-Fetch-Site or only with Origin.
	if code, _, b := raw(t, "POST", e.url+"/jobs", body, "Sec-Fetch-Site", "cross-site"); code != http.StatusForbidden || b["error"] == nil {
		t.Errorf("cross-site POST = %d %v, want a JSON 403", code, b)
	}
	if code, _, _ := raw(t, "DELETE", e.url+"/dlq", "", "Origin", "https://evil.example"); code != http.StatusForbidden {
		t.Errorf("cross-origin DELETE = %d, want 403", code)
	}
	// The dashboard itself (same origin) and non-browser clients pass.
	if code, _, _ := raw(t, "POST", e.url+"/jobs", body, "Sec-Fetch-Site", "same-origin"); code != http.StatusCreated {
		t.Errorf("same-origin POST = %d, want 201", code)
	}
	if code, _, _ := raw(t, "POST", e.url+"/jobs", body); code != http.StatusCreated {
		t.Errorf("POST with no browser headers = %d, want 201", code)
	}
	// Reads are allowed from anywhere; without CORS headers the other site
	// can't read the reply anyway.
	if code, _, _ := raw(t, "GET", e.url+"/stats", "", "Sec-Fetch-Site", "cross-site"); code != http.StatusOK {
		t.Errorf("cross-site GET = %d, want 200", code)
	}
}
