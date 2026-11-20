// Package api is the HTTP API for submitting and managing jobs and crons and
// for watching the cluster, plus a Go client for it (see Client). Every
// request and reply body is JSON, errors included.
package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/sham-33/job-scheduler/internal/dashboard"
	"github.com/sham-33/job-scheduler/internal/monitor"
	"github.com/sham-33/job-scheduler/internal/queue"
)

const (
	// maxBodyBytes caps a request body, payload included.
	maxBodyBytes = 1 << 20
	// defaultPage and maxPage bound list replies.
	defaultPage = 100
	maxPage     = 1000
	// pingTimeout bounds the Redis check in /healthz.
	pingTimeout = 2 * time.Second
)

// Options configures a Server.
type Options struct {
	// Token, if set, must be sent as "Authorization: Bearer <token>" with
	// every request except GET /healthz.
	Token string
}

// Server serves the API.
type Server struct {
	q      *queue.Queue
	m      *monitor.Monitor
	opts   Options
	logger *slog.Logger
	mux    *http.ServeMux
}

// New returns a Server.
func New(q *queue.Queue, m *monitor.Monitor, opts Options, logger *slog.Logger) *Server {
	s := &Server{q: q, m: m, opts: opts, logger: logger, mux: http.NewServeMux()}
	routes := []struct {
		pattern string
		handle  func(http.ResponseWriter, *http.Request) error
	}{
		{"POST /jobs", s.submitJob},
		{"GET /jobs/{id}", s.getJob},
		{"DELETE /jobs/{id}", s.cancelJob},
		{"GET /dlq", s.listDLQ},
		{"POST /dlq/{id}/replay", s.replayJob},
		{"DELETE /dlq", s.purgeDLQ},
		{"POST /crons", s.putCron},
		{"GET /crons", s.listCrons},
		{"GET /crons/{name}", s.getCron},
		{"PATCH /crons/{name}", s.patchCron},
		{"DELETE /crons/{name}", s.deleteCron},
		{"GET /stats", s.stats},
		{"GET /events", s.events},
		{"GET /healthz", s.healthz},
	}
	for _, rt := range routes {
		s.mux.HandleFunc(rt.pattern, func(w http.ResponseWriter, r *http.Request) {
			if err := rt.handle(w, r); err != nil {
				s.fail(w, r, err)
			}
		})
	}
	page := dashboard.Handler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusNotFound, http.StatusText(http.StatusNotFound))
	}))
	s.mux.Handle("GET /{$}", page)
	s.mux.Handle("GET /assets/", page)
	return s
}

// contentSecurityPolicy lets the dashboard load only its own files and call
// only this API. Even if a job's error message or a cron name held HTML, the
// browser would refuse to run any script it smuggled in.
const contentSecurityPolicy = "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self'; " +
	"connect-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'"

// isPublic reports whether a path needs no token: the health check, and the
// dashboard's files, which hold no data.
func isPublic(path string) bool {
	return path == "/healthz" || path == "/" || strings.HasPrefix(path, "/assets/")
}

// Handler returns the API and dashboard with their middleware: panic
// recovery, request logging, security headers, cross-site request blocking,
// the token check, and JSON replies for unknown routes.
func (s *Server) Handler() http.Handler {
	return s.recover(s.log(securityHeaders(s.crossOrigin(s.auth(http.HandlerFunc(s.route))))))
}

// crossOrigin rejects state-changing requests that a browser sends from
// another site. Without it, a web page open in the same browser could submit
// jobs to an API on localhost, or purge its dead-letter queue, whenever no
// API_TOKEN is set. Requests from jobctl and other non-browser clients carry
// no Origin or Sec-Fetch-Site header and pass.
func (s *Server) crossOrigin(next http.Handler) http.Handler {
	cop := http.NewCrossOriginProtection()
	cop.SetDenyHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusForbidden, "cross-origin request refused")
	}))
	return cop.Handler(next)
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", contentSecurityPolicy)
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "no-referrer")
		next.ServeHTTP(w, r)
	})
}

// route sends a request to its handler. ServeMux answers an unknown path or
// method in plain text, so those replies are rewritten as JSON.
func (s *Server) route(w http.ResponseWriter, r *http.Request) {
	if _, pattern := s.mux.Handler(r); pattern != "" {
		s.mux.ServeHTTP(w, r)
		return
	}
	rec := &discardWriter{header: http.Header{}, code: http.StatusNotFound}
	s.mux.ServeHTTP(rec, r)
	if allow := rec.header.Get("Allow"); allow != "" {
		w.Header().Set("Allow", allow)
	}
	writeError(w, rec.code, http.StatusText(rec.code))
}

func (s *Server) submitJob(w http.ResponseWriter, r *http.Request) error {
	var req JobRequest
	if err := decode(w, r, &req); err != nil {
		return err
	}
	id, created, err := s.q.Enqueue(r.Context(), req.spec(r.Header.Get("Idempotency-Key")))
	if err != nil {
		return err
	}
	j, err := s.q.Get(r.Context(), id)
	if err != nil {
		return err
	}
	w.Header().Set("Location", "/jobs/"+id)
	code := http.StatusOK // an earlier request with the same key made it
	if created {
		code = http.StatusCreated
	}
	writeJSON(w, code, jobOf(j))
	return nil
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) error {
	return s.replyJob(w, r, r.PathValue("id"))
}

// cancelJob cancels a job and returns it. A running job stops at its
// worker's next heartbeat, so it can still show as running briefly.
func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request) error {
	id := r.PathValue("id")
	if err := s.q.Cancel(r.Context(), id); err != nil {
		return err
	}
	return s.replyJob(w, r, id)
}

func (s *Server) replayJob(w http.ResponseWriter, r *http.Request) error {
	id := r.PathValue("id")
	if err := s.q.Replay(r.Context(), id); err != nil {
		return err
	}
	return s.replyJob(w, r, id)
}

func (s *Server) replyJob(w http.ResponseWriter, r *http.Request, id string) error {
	j, err := s.q.Get(r.Context(), id)
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, jobOf(j))
	return nil
}

func (s *Server) listDLQ(w http.ResponseWriter, r *http.Request) error {
	limit, err := intParam(r, "limit", defaultPage, 1, maxPage)
	if err != nil {
		return err
	}
	offset, err := intParam(r, "offset", 0, 0, 1<<31-1)
	if err != nil {
		return err
	}
	jobs, total, err := s.q.ListDLQ(r.Context(), offset, limit)
	if err != nil {
		return err
	}
	page := DLQPage{Jobs: make([]Job, len(jobs)), Total: total}
	for i, j := range jobs {
		page.Jobs[i] = jobOf(j)
	}
	writeJSON(w, http.StatusOK, page)
	return nil
}

func (s *Server) purgeDLQ(w http.ResponseWriter, r *http.Request) error {
	n, err := s.q.PurgeDLQ(r.Context())
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, PurgeResult{Purged: n})
	return nil
}

func (s *Server) putCron(w http.ResponseWriter, r *http.Request) error {
	var req CronRequest
	if err := decode(w, r, &req); err != nil {
		return err
	}
	c, err := s.q.PutCron(r.Context(), queue.CronSpec{Name: req.Name, Schedule: req.Schedule, Job: req.Job.spec("")})
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, cronOf(c))
	return nil
}

func (s *Server) listCrons(w http.ResponseWriter, r *http.Request) error {
	crons, err := s.q.ListCrons(r.Context())
	if err != nil {
		return err
	}
	list := CronList{Crons: make([]Cron, len(crons))}
	for i, c := range crons {
		list.Crons[i] = cronOf(c)
	}
	writeJSON(w, http.StatusOK, list)
	return nil
}

func (s *Server) getCron(w http.ResponseWriter, r *http.Request) error {
	c, err := s.q.GetCron(r.Context(), r.PathValue("name"))
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, cronOf(c))
	return nil
}

func (s *Server) patchCron(w http.ResponseWriter, r *http.Request) error {
	var patch CronPatch
	if err := decode(w, r, &patch); err != nil {
		return err
	}
	if patch.Enabled == nil {
		return badRequest(errors.New(`the body must set "enabled" to true or false`))
	}
	c, err := s.q.SetCronEnabled(r.Context(), r.PathValue("name"), *patch.Enabled)
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, cronOf(c))
	return nil
}

func (s *Server) deleteCron(w http.ResponseWriter, r *http.Request) error {
	if err := s.q.DeleteCron(r.Context(), r.PathValue("name")); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *Server) stats(w http.ResponseWriter, r *http.Request) error {
	snap, err := s.m.Snapshot(r.Context())
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, snap)
	return nil
}

// events returns events oldest first: the latest ones, or with ?after= the
// ones that came after that event.
func (s *Server) events(w http.ResponseWriter, r *http.Request) error {
	limit, err := intParam(r, "limit", defaultPage, 1, maxPage)
	if err != nil {
		return err
	}
	after := r.URL.Query().Get("after")
	if after != "" && !eventIDPattern.MatchString(after) {
		return badRequest(fmt.Errorf("after must be an event ID such as 1700000000000-0, got %q", after))
	}
	var events []monitor.Event
	if after == "" {
		events, err = s.m.RecentEvents(r.Context(), limit)
		slices.Reverse(events)
	} else {
		events, err = s.m.EventsAfter(r.Context(), after, limit)
	}
	if err != nil {
		return err
	}
	page := EventsPage{Events: events, LastID: after}
	if len(events) > 0 {
		page.LastID = events[len(events)-1].ID
	}
	writeJSON(w, http.StatusOK, page)
	return nil
}

func (s *Server) healthz(w http.ResponseWriter, r *http.Request) error {
	ctx, cancel := context.WithTimeout(r.Context(), pingTimeout)
	defer cancel()
	if err := s.m.Ping(ctx); err != nil {
		s.logger.Warn("health check: Redis unreachable", "err", err)
		writeError(w, http.StatusServiceUnavailable, "Redis is unreachable")
		return nil
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	return nil
}

// eventIDPattern matches a stream entry ID: milliseconds, then a sequence
// number.
var eventIDPattern = regexp.MustCompile(`^[0-9]{1,20}-[0-9]{1,20}$`)

// httpError is an error with the status code to answer it with.
type httpError struct {
	code int
	err  error
}

func (e *httpError) Error() string { return e.err.Error() }
func (e *httpError) Unwrap() error { return e.err }

func badRequest(err error) error { return &httpError{http.StatusBadRequest, err} }

// fail answers a request whose handler returned err. Errors the caller can
// fix get their message back; anything else is logged and answered with a
// plain 500, since its message may say more about the system than a client
// should see.
func (s *Server) fail(w http.ResponseWriter, r *http.Request, err error) {
	var he *httpError
	switch {
	case errors.As(err, &he):
		writeError(w, he.code, he.Error())
	case errors.Is(err, queue.ErrInvalid):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, queue.ErrNotFound), errors.Is(err, queue.ErrCronNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, queue.ErrFinished), errors.Is(err, queue.ErrNotDead), errors.Is(err, queue.ErrCronChanged):
		writeError(w, http.StatusConflict, err.Error())
	case r.Context().Err() != nil:
		// The client went away; nobody is listening for a reply.
	default:
		s.logger.Error("request failed", "method", r.Method, "path", r.URL.Path, "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

// decode reads a JSON body into v. Unknown fields are rejected, so a typo
// such as "max_attempt" is an error instead of being silently ignored.
func decode(w http.ResponseWriter, r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	err := dec.Decode(v)
	if err == nil && dec.Decode(&struct{}{}) != io.EOF {
		err = errors.New("the body must hold a single JSON object")
	}
	var tooBig *http.MaxBytesError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &tooBig):
		return &httpError{http.StatusRequestEntityTooLarge, fmt.Errorf("the body is larger than %d bytes", tooBig.Limit)}
	case errors.Is(err, io.EOF):
		return badRequest(errors.New("the body is empty; it must be a JSON object"))
	}
	return badRequest(fmt.Errorf("invalid JSON body: %w", err))
}

// intParam reads an optional integer query parameter between lo and hi.
func intParam(r *http.Request, name string, def, lo, hi int) (int, error) {
	v := r.URL.Query().Get(name)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < lo || n > hi {
		return 0, badRequest(fmt.Errorf("%s must be a whole number from %d to %d, got %q", name, lo, hi, v))
	}
	return n, nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	// Compact, not indented: indenting would also reformat job payloads,
	// which should come back exactly as they were submitted.
	_ = json.NewEncoder(w).Encode(v) // a write error means the client went away
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, ErrorBody{Error: msg})
}

// auth rejects requests without the token, if one is set. The health check
// stays open so load balancers and orchestrators can call it, and so do the
// dashboard's files, which ask for the token themselves.
func (s *Server) auth(next http.Handler) http.Handler {
	if s.opts.Token == "" {
		return next
	}
	want := []byte("Bearer " + s.opts.Token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := []byte(r.Header.Get("Authorization"))
		if !isPublic(r.URL.Path) && subtle.ConstantTimeCompare(got, want) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="job-scheduler"`)
			writeError(w, http.StatusUnauthorized, "missing or wrong API token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// log writes one line per request. Successful reads are logged at debug
// level: the dashboard polls several times a second, and health checks come
// every few seconds. Changes and errors are logged at info or above.
func (s *Server) log(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(sw, r)
		level := slog.LevelInfo
		switch {
		case sw.code >= 500:
			level = slog.LevelError
		case r.Method == http.MethodGet && sw.code < 400:
			level = slog.LevelDebug
		}
		s.logger.Log(r.Context(), level, "request", "method", r.Method, "path", r.URL.Path,
			"route", r.Pattern, "status", sw.code, "took", time.Since(start), "remote", r.RemoteAddr)
	})
}

// recover turns a panic in a handler into a 500, so one bad request can't
// take the server down.
func (s *Server) recover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				if v == http.ErrAbortHandler {
					panic(v)
				}
				s.logger.Error("handler panicked", "method", r.Method, "path", r.URL.Path,
					"panic", v, "stack", string(debug.Stack()))
				writeError(w, http.StatusInternalServerError, "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// statusWriter remembers the status code written through it.
type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(code int) {
	w.code = code
	w.ResponseWriter.WriteHeader(code)
}

// discardWriter captures the status and headers of a reply and drops its body.
type discardWriter struct {
	header http.Header
	code   int
}

func (w *discardWriter) Header() http.Header         { return w.header }
func (w *discardWriter) Write(b []byte) (int, error) { return len(b), nil }
func (w *discardWriter) WriteHeader(code int)        { w.code = code }
