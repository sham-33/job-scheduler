package job

import (
	"strings"
	"testing"
	"time"
)

func TestFromHash(t *testing.T) {
	j, err := FromHash(map[string]string{
		"id":          "j1",
		"type":        "echo",
		"payload":     `{"msg":"hi"}`,
		"status":      "running",
		"attempts":    "2",
		"idem_key":    "order-42",
		"lease_owner": "worker-1",
		"lease_token": "17",
		"run_at":      "1700000000000",
		"created_at":  "1699999999000",
		"updated_at":  "1700000000500",
		"started_at":  "1700000000500",
	})
	if err != nil {
		t.Fatalf("FromHash: %v", err)
	}
	if j.ID != "j1" || j.Type != "echo" || j.Status != StatusRunning || j.Attempts != 2 ||
		j.IdempotencyKey != "order-42" || j.LeaseOwner != "worker-1" || j.LeaseToken != 17 {
		t.Errorf("FromHash fields = %+v", j)
	}
	if string(j.Payload) != `{"msg":"hi"}` {
		t.Errorf("Payload = %s", j.Payload)
	}
	if want := time.UnixMilli(1700000000000); !j.RunAt.Equal(want) {
		t.Errorf("RunAt = %v, want %v", j.RunAt, want)
	}
	if want := time.UnixMilli(1700000000500); !j.StartedAt.Equal(want) {
		t.Errorf("StartedAt = %v, want %v", j.StartedAt, want)
	}
	if !j.FinishedAt.IsZero() {
		t.Errorf("FinishedAt = %v, want zero for a missing field", j.FinishedAt)
	}
}

func TestFromHashErrors(t *testing.T) {
	tests := []struct {
		name    string
		hash    map[string]string
		wantErr string
	}{
		{"missing id", map[string]string{"type": "echo"}, "no id"},
		{"bad attempts", map[string]string{"id": "j1", "attempts": "two"}, "attempts"},
		{"bad time", map[string]string{"id": "j1", "run_at": "soon"}, "run_at"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := FromHash(tt.hash)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("FromHash error = %v, want it to mention %q", err, tt.wantErr)
			}
		})
	}
}
