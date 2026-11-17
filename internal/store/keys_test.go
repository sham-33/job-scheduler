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
			k.DLQ():              "jobs:dlq",
			k.LeaseToken():       "lease:token",
			k.Idempotency("k-1"): "idem:k-1",
			k.Stats():            "stats",
			k.Leader():           "scheduler:leader",
			k.Epoch():            "scheduler:epoch",
			k.Cron("nightly"):    "cron:nightly",
			k.CronPrefix():       "cron:",
			k.Crons():            "crons",
			k.Events():           "events",
			k.Workers():          "workers",
			k.Worker("w-1"):      "worker:w-1",
			k.WorkerPrefix():     "worker:",
		}
		for got, want := range tests {
			if got != prefix+want {
				t.Errorf("prefix %q: got %q, want %q", prefix, got, prefix+want)
			}
		}
	}
}
