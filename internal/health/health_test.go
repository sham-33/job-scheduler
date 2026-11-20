package health

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "healthy")

	if err := CheckFile(path, time.Second); err == nil || !strings.Contains(err.Error(), "not yet in touch") {
		t.Errorf("before any touch: %v, want not yet in touch", err)
	}
	f := NewFile(path)
	f.Touch()
	if err := CheckFile(path, time.Second); err != nil {
		t.Errorf("just touched: %v", err)
	}

	// Stale: last touched 10 s ago.
	old := time.Now().Add(-10 * time.Second)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	if err := CheckFile(path, 5*time.Second); err == nil || !strings.Contains(err.Error(), "more than 5s") {
		t.Errorf("touched 10 s ago: %v, want too old", err)
	}
	// Touching again makes it fresh, whether or not the file exists.
	f.Touch()
	if err := CheckFile(path, 5*time.Second); err != nil {
		t.Errorf("touched again: %v", err)
	}

	if err := CheckFile("", time.Second); err == nil {
		t.Error("no path: no error")
	}
	NewFile("").Touch() // records nothing, and doesn't panic
}

func TestCheckURL(t *testing.T) {
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	defer ok.Close()
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer down.Close()
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}))
	defer slow.Close()

	if err := CheckURL(ok.URL, time.Second); err != nil {
		t.Errorf("200: %v", err)
	}
	if err := CheckURL(down.URL, time.Second); err == nil || !strings.Contains(err.Error(), "503") {
		t.Errorf("503: %v, want an error naming it", err)
	}
	if err := CheckURL(slow.URL, 100*time.Millisecond); err == nil {
		t.Error("no answer within the timeout: no error")
	}
	closed := httptest.NewServer(http.NotFoundHandler())
	closed.Close()
	if err := CheckURL(closed.URL, time.Second); err == nil {
		t.Error("nothing listening: no error")
	}
}

func TestLocalURL(t *testing.T) {
	for addr, want := range map[string]string{
		":8080":          "http://127.0.0.1:8080/healthz",
		"0.0.0.0:9000":   "http://127.0.0.1:9000/healthz",
		"[::]:9000":      "http://127.0.0.1:9000/healthz",
		"localhost:8080": "http://localhost:8080/healthz",
		"10.0.0.5:80":    "http://10.0.0.5:80/healthz",
	} {
		if got, err := LocalURL(addr, "/healthz"); err != nil || got != want {
			t.Errorf("LocalURL(%q) = %q, %v; want %q", addr, got, err, want)
		}
	}
	if _, err := LocalURL("8080", "/healthz"); err == nil {
		t.Error(`LocalURL("8080"): no error`)
	}
}
