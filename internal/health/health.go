// Package health answers the question Docker asks a container every few
// seconds: is this process still doing its job?
//
// A worker or scheduler has no web server to ask, and a process that merely
// exists may be stuck, or cut off from Redis. So each one touches a file
// every time it gets through to Redis (see File), and `<binary> healthcheck`
// fails once that file is older than a few seconds (see CheckFile). The API
// is asked over HTTP instead (see CheckURL).
package health

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"
)

// File is a file whose modification time records the last time the process
// got through to Redis. The zero File records nothing.
type File struct {
	path string
}

// NewFile returns a File at path. An empty path records nothing.
func NewFile(path string) File {
	return File{path: path}
}

// Touch records that the process got through to Redis just now. Errors are
// ignored: a health file that can't be written makes the check fail, which
// is the right outcome.
func (f File) Touch() {
	if f.path == "" {
		return
	}
	now := time.Now()
	if err := os.Chtimes(f.path, now, now); err != nil {
		_ = os.WriteFile(f.path, nil, 0o644)
	}
}

// CheckFile returns an error unless the file at path was touched within
// maxAge.
func CheckFile(path string, maxAge time.Duration) error {
	if path == "" {
		return fmt.Errorf("HEALTH_FILE is not set, so there is nothing to check")
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("not yet in touch with Redis: %w", err)
	}
	if age := time.Since(info.ModTime()); age > maxAge {
		return fmt.Errorf("last in touch with Redis %s ago, more than %s", age.Round(time.Millisecond), maxAge)
	}
	return nil
}

// CheckURL returns an error unless a GET of url answers 200 within timeout.
func CheckURL(url string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s answered %s", url, resp.Status)
	}
	return nil
}

// LocalURL turns a listen address such as ":8080" or "0.0.0.0:8080" into a
// URL that reaches it from the same machine.
func LocalURL(listenAddr, path string) (string, error) {
	host, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return "", fmt.Errorf("listen address %q: %w", listenAddr, err)
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port) + path, nil
}

// Run is what a binary's `healthcheck` subcommand does: it runs check and
// exits 0 if it passes, or prints why and exits 1.
func Run(check func() error) {
	if err := check(); err != nil {
		fmt.Fprintln(os.Stderr, "unhealthy:", err)
		os.Exit(1)
	}
	os.Exit(0)
}
