# Distributed Job Scheduler

A fault-tolerant job scheduler in Go and Redis, with leader election, distributed leases, idempotent claims, retries and a dead-letter queue. [docs/PLAN.md](docs/PLAN.md) has the design and the build phases.

**Status:** Phase 3 is done. If a worker crashes or freezes, another worker takes over its job in under 2 seconds, and the old worker can't record a result. Next is Phase 4: retries and the dead-letter queue.

## Prerequisites

- Go 1.27+
- Redis 7, either in Docker (`make redis-up`) or a local `redis-server`

## Quick start

```bash
make redis-up     # Redis in Docker, AOF on
make run-worker   # claims and runs jobs; Ctrl-C to stop
make test         # Redis tests skip if Redis isn't running
```

Run `make help` for every target. There's no submit command until Phase 8. To watch jobs run end to end, run these tests, which print their timings:

```bash
go test -v -run TestPoolRunsImmediateAndDelayedJobs ./internal/worker/
```

```bash
go test -v -run TestPoolRunsEveryJobExactlyOnce ./internal/worker/
```

```bash
go test -v -run TestPoolPicksUpJobFromCrashedWorker ./internal/worker/
```

## How a job flows

Each step is one Lua script in [internal/queue/scripts](internal/queue/scripts), so Redis runs it atomically.

1. **enqueue.lua** creates the `job:{id}` hash. A due job goes into `jobs:ready`; a delayed one goes into `jobs:scheduled`, scored by `run_at`. An idempotency key that was already used returns the existing job instead.
2. **promote.lua** (every 100ms) moves due jobs from `jobs:scheduled` to `jobs:ready`.
3. **claim.lua** pops the longest-waiting ready job, marks it `running`, issues a new fencing token, and records a lease in `jobs:leases`.
4. **heartbeat.lua** renews the lease every 500ms while the job runs. A worker told it has lost the lease cancels the job.
5. **complete.lua** marks the job `succeeded`, but only if the caller still holds the current fencing token.
6. **reap.lua** (every 200ms) finds leases nobody renewed and puts those jobs back in `jobs:ready`, ahead of newer work, so another worker can take over.

Delivery is **at-least-once**: a job whose worker dies runs again, so handlers should be safe to repeat. Fencing guarantees only one attempt's result is ever recorded.

## Configuration

Every binary reads the same environment variables. An empty variable counts as unset.

| Variable          | Default            | Meaning                                                                    |
| ----------------- | ------------------ | -------------------------------------------------------------------------- |
| `NODE_ID`         | `<hostname>-<pid>` | Identifies this process in leases, logs and the worker registry            |
| `LOG_LEVEL`       | `info`             | `debug`, `info`, `warn` or `error`                                         |
| `REDIS_ADDR`      | `localhost:6379`   | Redis host and port                                                        |
| `REDIS_PASSWORD`  | (none)             | Redis password                                                             |
| `REDIS_DB`        | `0`                | Redis database number. Tests use DB 15                                     |
| `HTTP_ADDR`       | `:8080`            | API and dashboard listen address                                           |
| `CONCURRENCY`     | `25`               | Jobs one worker process runs at once                                       |
| `LEASE_TTL`       | `1500ms`           | How long a claimed job stays owned without a heartbeat                     |
| `HEARTBEAT_EVERY` | `500ms`            | How often a worker extends its leases. Must be at most half of `LEASE_TTL` |
| `REAP_EVERY`      | `200ms`            | How often the scheduler reclaims expired leases                            |
| `SHUTDOWN_GRACE`  | `5s`               | How long running jobs get to finish after a worker is told to stop         |

Tests read `REDIS_TEST_ADDR` (default `localhost:6379`).
