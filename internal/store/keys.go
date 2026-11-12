package store

// Keys builds the name of every Redis key. All names share one prefix, so
// separate deployments (or parallel tests) can share a Redis server without
// seeing each other's jobs. The zero value uses no prefix.
type Keys struct {
	prefix string
}

func NewKeys(prefix string) Keys {
	return Keys{prefix: prefix}
}

// Job is the hash holding one job's fields.
func (k Keys) Job(id string) string { return k.JobPrefix() + id }

// JobPrefix is passed to Lua scripts that build job keys from the IDs they
// pop off a queue, since those keys can't be known in advance.
func (k Keys) JobPrefix() string { return k.prefix + "job:" }

// Scheduled is a sorted set of delayed job IDs, scored by run_at (ms).
func (k Keys) Scheduled() string { return k.prefix + "jobs:scheduled" }

// Ready is a sorted set of runnable job IDs, scored by run_at (ms), so the
// job that has been due longest is claimed first.
func (k Keys) Ready() string { return k.prefix + "jobs:ready" }

// Leases is a sorted set of running job IDs, scored by lease expiry (ms).
func (k Keys) Leases() string { return k.prefix + "jobs:leases" }

// DLQ is the dead-letter queue: a sorted set of IDs of jobs that ran out of
// attempts or failed permanently, scored by when they died (ms).
func (k Keys) DLQ() string { return k.prefix + "jobs:dlq" }

// LeaseToken is a counter that issues a new fencing token for every claim.
func (k Keys) LeaseToken() string { return k.prefix + "lease:token" }

// Idempotency maps a client-supplied idempotency key to the job it created.
func (k Keys) Idempotency(key string) string { return k.prefix + "idem:" + key }

// Stats is a hash of counters such as submitted, claimed and succeeded.
func (k Keys) Stats() string { return k.prefix + "stats" }
