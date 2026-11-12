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
		// Retrying a malformed payload can't help.
		if err := Sleep(context.Background(), j); !job.IsPermanent(err) {
			t.Errorf("Sleep(%s) = %v, want a permanent error", payload, err)
		}
	}
}

func TestDefaultRegistersEveryType(t *testing.T) {
	h := Default()
	for _, name := range []string{"echo", "sleep", "flaky", "http"} {
		if h[name] == nil {
			t.Errorf("Default() has no %q handler", name)
		}
	}
}

func TestFlaky(t *testing.T) {
	never := &job.Job{ID: "j1", Payload: json.RawMessage(`{"p": 0}`)}
	always := &job.Job{ID: "j2", Payload: json.RawMessage(`{"p": 1}`)}
	half := &job.Job{ID: "j3", Payload: json.RawMessage(`{"p": 0.5}`)}

	failures := 0
	for range 200 {
		if err := Flaky(context.Background(), never); err != nil {
			t.Fatalf("Flaky(p=0) = %v, want success", err)
		}
		// These failures are worth retrying, so they must not be permanent.
		if err := Flaky(context.Background(), always); err == nil || job.IsPermanent(err) {
			t.Fatalf("Flaky(p=1) = %v, want an ordinary error", err)
		}
		if Flaky(context.Background(), half) != nil {
			failures++
		}
	}
	// 200 fair coin flips land outside 50..150 about once in 10^12 runs.
	if failures < 50 || failures > 150 {
		t.Errorf("Flaky(p=0.5) failed %d of 200 times, want about 100", failures)
	}
	if err := Flaky(context.Background(), &job.Job{ID: "j4"}); err != nil {
		t.Errorf("Flaky with no payload = %v, want success", err)
	}
}

func TestFlakyBadPayload(t *testing.T) {
	for _, payload := range []string{`{"p": "often"}`, `{"p": -0.1}`, `{"p": 1.5}`} {
		j := &job.Job{ID: "j1", Payload: json.RawMessage(payload)}
		if err := Flaky(context.Background(), j); !job.IsPermanent(err) {
			t.Errorf("Flaky(%s) = %v, want a permanent error", payload, err)
		}
	}
}
