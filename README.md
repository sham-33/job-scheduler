# Distributed Job Scheduler

A fault-tolerant job scheduler in Go and Redis, with leader election, distributed leases, idempotent claims, retries and a dead-letter queue. [docs/PLAN.md](docs/PLAN.md) has the design and the build phases.

**Status:** Phase 4 is done. A failed job is retried with growing waits, and after 5 attempts it moves to a dead-letter queue, from where it can be replayed. Jobs can have a timeout and can be cancelled. The `http` job type calls any web address, so new kinds of work need no change to the worker. Next is Phase 5: the scheduler service and leader election.

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

```bash
go test -v -run TestPoolRetriesFailedJobWithGrowingDelays ./internal/worker/
```

## How a job flows

Each step is one Lua script in [internal/queue/scripts](internal/queue/scripts), so Redis runs it atomically.

1. **enqueue.lua** creates the `job:{id}` hash. A due job goes into `jobs:ready`; a delayed one goes into `jobs:scheduled`, scored by `run_at`. An idempotency key that was already used returns the existing job instead.
2. **promote.lua** (every 100ms) moves due jobs from `jobs:scheduled` to `jobs:ready`.
3. **claim.lua** pops the longest-waiting ready job, marks it `running`, issues a new fencing token, and records a lease in `jobs:leases`.
4. **heartbeat.lua** renews the lease every 500ms while the job runs. A worker told it has lost the lease, or that the job was cancelled, stops the job.
5. **complete.lua** marks the job `succeeded`, but only if the caller still holds the current fencing token.
6. **fail.lua** records a failed attempt, with the same token check. The job goes back to `jobs:scheduled` to wait for its retry. If it is out of attempts, or the failure is permanent, it goes to `jobs:dlq`, the dead-letter queue.
7. **reap.lua** (every 200ms) finds leases nobody renewed and puts those jobs back in `jobs:ready`, ahead of newer work and with no wait, so another worker can take over. A job that has used up its attempts goes to the dead-letter queue instead.

Two more scripts act on a job from outside. **cancel.lua** stops a job that hasn't finished, and a worker running it finds out at its next heartbeat. **replay.lua** moves a dead job back to `jobs:ready` with its attempts reset.

Delivery is **at-least-once**: a job that fails, or whose worker dies, runs again, so handlers should be safe to repeat. Fencing guarantees only one attempt's result is ever recorded.

## Job options

Each job carries its own retry settings, set when it is submitted.

| Option         | Default | Meaning                                                                   |
| -------------- | ------- | ------------------------------------------------------------------------- |
| `MaxAttempts`  | `5`     | Times the job may start before it moves to the dead-letter queue          |
| `Timeout`      | none    | Longest a single attempt may run. A timed-out attempt counts as a failure |
| `Backoff.Base` | `1s`    | Wait before the first retry. It doubles on each retry, with random jitter |
| `Backoff.Max`  | `1m`    | Longest wait between retries                                              |

## Job types

A job's `type` picks the code that runs it, from [internal/handlers](internal/handlers). A handler that returns an error is retried. One that returns `job.Permanent(err)` goes straight to the dead-letter queue.

| Type    | Payload                        | What it does                                             |
| ------- | ------------------------------ | -------------------------------------------------------- |
| `echo`  | anything                       | Logs the payload                                         |
| `sleep` | `{"ms": 250}`                  | Waits, to stand in for slow work                         |
| `flaky` | `{"p": 0.5}`                   | Fails with probability `p`, to exercise retries          |
| `http`  | `{url, method, headers, body}` | Calls a web address, so the work can live in any service |

The `http` type sends `body` as JSON (`POST` unless `method` says otherwise) with `X-Job-Id` and `X-Job-Attempt` headers, so the receiver can spot a repeat. The reply decides the outcome:

| Reply                                       | Outcome                           |
| ------------------------------------------- | --------------------------------- |
| 2xx                                         | Succeeded                         |
| 5xx, 408, 429, a network error or a timeout | Retried with backoff              |
| Anything else                               | Straight to the dead-letter queue |

It does not sign requests or block internal addresses, which a hosted service would need.

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
