package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/sham-33/job-scheduler/internal/job"
)

func TestSleep(t *testing.T) {
	j := &job.Job{ID: "j1", Payload: json.RawMessage(`{"ms": 30}`)}
	start := time.Now()
	if err := Sleep(context.Background(), j); err != nil {
		t.Fatalf("Sleep: %v", err)
	}
	if took := time.Since(start); took < 30*time.Millisecond {
		t.Errorf("Sleep returned after %s, want at least 30ms", took)
	}
}

func TestSleepStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	j := &job.Job{ID: "j1", Payload: json.RawMessage(`{"ms": 10000}`)}

	start := time.Now()
	err := Sleep(ctx, j)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Sleep = %v, want context.DeadlineExceeded", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Errorf("Sleep took %s after cancel, want it to stop promptly", took)
	}
}

func TestSleepBadPayload(t *testing.T) {
	for _, payload := range []string{`{"ms": "soon"}`, `{"ms": -1}`} {
		j := &job.Job{ID: "j1", Payload: json.RawMessage(payload)}
		if err := Sleep(context.Background(), j); err == nil {
			t.Errorf("Sleep(%s) succeeded, want error", payload)
		}
	}
}

func TestDefaultRegistersEveryType(t *testing.T) {
	h := Default()
	for _, name := range []string{"echo", "sleep"} {
		if h[name] == nil {
			t.Errorf("Default() has no %q handler", name)
		}
	}
}
