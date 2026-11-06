package store

import "testing"

func TestKeys(t *testing.T) {
	for _, prefix := range []string{"", "staging:"} {
		k := NewKeys(prefix)
		tests := map[string]string{
			k.Job("abc"):         "job:abc",
			k.JobPrefix():        "job:",
			k.Scheduled():        "jobs:scheduled",
			k.Ready():            "jobs:ready",
			k.Leases():           "jobs:leases",
			k.LeaseToken():       "lease:token",
			k.Idempotency("k-1"): "idem:k-1",
			k.Stats():            "stats",
		}
		for got, want := range tests {
			if got != prefix+want {
				t.Errorf("prefix %q: got %q, want %q", prefix, got, prefix+want)
			}
		}
	}
}
