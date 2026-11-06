# Distributed Job Scheduler

A fault-tolerant job scheduler in Go and Redis, with leader election, distributed leases, idempotent claims, retries and a dead-letter queue. [PLAN.md](PLAN.md) has the design and the build phases.

**Status:** Phase 1 is done. A worker runs immediate and delayed jobs one at a time, and submitting with an idempotency key never creates duplicates. Next is Phase 2: 25 concurrent workers.

## Prerequisites

- Go 1.27+
- Redis 7, either in Docker (`make redis-up`) or a local `redis-server`

## Quick start

```bash
make redis-up     # Redis in Docker, AOF on
make run-worker   # claims and runs jobs; Ctrl-C to stop
make test         # Redis tests skip if Redis isn't running
```

Run `make help` for every target. There's no submit command until Phase 8; to watch an immediate and a delayed job run end to end, run:

```bash
go test -v -run TestWorkerRunsImmediateAndDelayedJobs ./internal/worker/
```

## How a job flows

Each step is one Lua script in [internal/queue/scripts](internal/queue/scripts), so Redis runs it atomically.

1. **enqueue.lua** creates the `job:{id}` hash. A due job goes into `jobs:ready`; a delayed one goes into `jobs:scheduled`, scored by `run_at`. An idempotency key that was already used returns the existing job instead.
2. **promote.lua** (every 100ms) moves due jobs from `jobs:scheduled` to `jobs:ready`.
3. **claim.lua** pops the longest-waiting ready job, marks it `running`, issues a new fencing token, and records a lease in `jobs:leases`.
4. **complete.lua** marks the job `succeeded`, but only if the caller still holds the current fencing token.

## Configuration

Every binary reads the same environment variables. An empty variable counts as unset.

| Variable            | Default              | Meaning                                                                      |
| ------------------- | -------------------- | ---------------------------------------------------------------------------- |
| `NODE_ID`         | `<hostname>-<pid>` | Identifies this process in leases, logs and the worker registry              |
| `LOG_LEVEL`       | `info`             | `debug`, `info`, `warn` or `error`                                   |
| `REDIS_ADDR`      | `localhost:6379`   | Redis host and port                                                          |
| `REDIS_PASSWORD`  | (none)               | Redis password                                                               |
| `REDIS_DB`        | `0`                | Redis database number. Tests use DB 15                                       |
| `HTTP_ADDR`       | `:8080`            | API and dashboard listen address                                             |
| `CONCURRENCY`     | `25`               | Jobs one worker process runs at once                                         |
| `LEASE_TTL`       | `1500ms`           | How long a claimed job stays owned without a heartbeat                       |
| `HEARTBEAT_EVERY` | `500ms`            | How often a worker extends its leases. Must be at most half of `LEASE_TTL` |
| `REAP_EVERY`      | `200ms`            | How often the scheduler reclaims expired leases                              |

Tests read `REDIS_TEST_ADDR` (default `localhost:6379`).
