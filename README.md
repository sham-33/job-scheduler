# Distributed Job Scheduler

A fault-tolerant job scheduler in Go and Redis, with leader election, distributed leases, idempotent claims, retries and a dead-letter queue. [docs/PLAN.md](docs/PLAN.md) has the design and the build phases.

**Status:** Phase 8 is done. An HTTP API and the `jobctl` command-line tool submit, inspect and cancel jobs, manage the dead-letter queue and crons, and show stats and live events. Every job state change is now recorded in an `events` stream, each worker process checks in every second so dead ones show up, and one call reads counters, queue sizes, the leader and the workers together. Crons create jobs on a schedule: the leading scheduler makes exactly one job for each scheduled time, even when leadership changes hands, and a cron that fell behind fires once instead of catching up. Schedulers elect a leader through Redis, and only the leader releases delayed jobs, takes jobs back from dead workers and fires crons. If the leader dies, another scheduler takes over within about 3 seconds. Failed jobs are retried with growing waits and end up in a dead-letter queue, from where they can be replayed. The `http` job type calls any web address. Next is Phase 9: a web dashboard.

## Prerequisites

- Go 1.27+
- Redis 7, either in Docker (`make redis-up`) or a local `redis-server`

## Quick start

```bash
make redis-up        # Redis in Docker, AOF on
make run-scheduler   # releases delayed jobs, reclaims jobs from dead workers, fires crons
make run-worker      # claims and runs jobs; Ctrl-C to stop
make run-api         # the HTTP API on :8080
make build           # builds ./bin/jobctl, the command-line client, with the other binaries
make test            # Redis tests skip if Redis isn't running
```

Each `run-` target runs until Ctrl-C, so give each its own terminal. Workers need a scheduler running: without one, delayed jobs never fall due, a dead worker's jobs are never taken back, and crons never fire. Start a second scheduler for failover.

Then, in another terminal:

```bash
./bin/jobctl submit echo '{"hello":"world"}'        # prints the new job's ID
./bin/jobctl get <id>                               # the job, with its status
./bin/jobctl submit http '{"url":"https://example.com/hook","body":{"n":1}}' --delay 30s
./bin/jobctl cron add ping '@every 1m' echo '{"from":"cron"}'
./bin/jobctl stats                                  # leader, workers, queues and counters
./bin/jobctl events -f                              # follow job events; Ctrl-C to stop
```

Run `make help` for every target and `./bin/jobctl --help` for every command. These tests print timings as jobs run end to end:

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

```bash
go test -v -run TestFailoverFiresEachCronSlotOnce ./internal/scheduler/
```

## How a job flows

Each step is one Lua script in [internal/queue/scripts](internal/queue/scripts), so Redis runs it atomically.

1. **enqueue.lua** creates the `job:{id}` hash. A due job goes into `jobs:ready`; a delayed one goes into `jobs:scheduled`, scored by `run_at`. An idempotency key that was already used returns the existing job instead.
2. **promote.lua** (every 100ms, run by the leading scheduler) moves due jobs from `jobs:scheduled` to `jobs:ready`.
3. **claim.lua** pops the longest-waiting ready job, marks it `running`, issues a new fencing token, and records a lease in `jobs:leases`.
4. **heartbeat.lua** renews the lease every 500ms while the job runs. A worker told it has lost the lease, or that the job was cancelled, stops the job.
5. **complete.lua** marks the job `succeeded`, but only if the caller still holds the current fencing token.
6. **fail.lua** records a failed attempt, with the same token check. The job goes back to `jobs:scheduled` to wait for its retry. If it is out of attempts, or the failure is permanent, it goes to `jobs:dlq`, the dead-letter queue.
7. **reap.lua** (every 200ms, run by the leading scheduler) finds leases nobody renewed and puts those jobs back in `jobs:ready`, ahead of newer work and with no wait, so another worker can take over. A job that has used up its attempts goes to the dead-letter queue instead.

A worker that is told to stop gets `SHUTDOWN_GRACE` to finish its jobs. It then cancels what is left, and **release.lua** puts each of those jobs straight back in `jobs:ready`. Being interrupted isn't the job's fault, so that attempt doesn't count against it.

Two more scripts act on a job from outside. **cancel.lua** stops a job that hasn't finished, and a worker running it finds out at its next heartbeat. **replay.lua** moves a dead job back to `jobs:ready` with its attempts reset.

## Leader election

Any number of schedulers can run, but only one leads at a time. Leadership is the Redis key `scheduler:leader`, which holds the leader's ID and expires 3 seconds after its last renewal. Each step is a small Lua script in [internal/election/scripts](internal/election/scripts):

- **acquire.lua** takes the seat if it is free and starts a new term, numbered by `scheduler:epoch`.
- **renew.lua** extends the term every second, but only for the current leader. A renewal that fails, or finds someone else in the seat, ends the term at once.
- **release.lua** gives the seat up on shutdown, so another scheduler takes over in about 250ms instead of waiting for the term to expire.

If the leader dies, its key expires and another scheduler takes over within about 3 seconds. A leader that was only paused can wake up still believing it leads, and run one more step before its renewal fails. That is harmless: every step is an atomic Lua script that checks each job's state, so running it twice changes nothing.

## Cron schedules

A cron is a job template plus a schedule. The leading scheduler checks every 100ms for crons that are due, and makes exactly one job for each scheduled time, called a slot.

- **Schedule:** a standard five-field expression such as `*/5 * * * *`, or a shorthand such as `@hourly` or `@every 30s`. `@every` takes a whole number of seconds, at least `1s`. It is read in UTC unless it starts with a zone, as in `CRON_TZ=Asia/Kolkata 0 9 * * *`.
- **Job:** the same options as any other job: type, payload, `MaxAttempts`, `Timeout` and `Backoff`. Each job's `run_at` is its slot, and its idempotency key is `cron:{name}:{slot in ms}`.
- **One job per slot:** **cron_fire.lua** creates the slot's job and moves the cron on to its next slot in one atomic step. It does this only if the cron is still waiting for that slot. So two schedulers racing for a slot, or a paused leader that wakes up after its successor fired one, can't make a second job.
- **Missed slots:** a cron that fell behind, because no scheduler was running, fires once for its oldest missed slot and then skips to its first slot after now. It doesn't make a job for every slot it missed.
- **Changes:** saving a cron under a name that exists replaces it and restarts its schedule from now. A disabled cron never fires. When turned back on, it restarts from now too, so the slots it missed while off never run.

Crons are stored in a `cron:{name}` hash each, with every name in the `crons` sorted set, scored by its next slot. Phase 8 puts an API and a CLI in front of `queue.PutCron`, `ListCrons`, `SetCronEnabled` and `DeleteCron`.

## API and `jobctl`

`cmd/api` serves a JSON API, and `jobctl` is its command-line client. Every reply is JSON, errors included as `{"error": "..."}`.

| Request                     | `jobctl`                              | Does                                                    |
| --------------------------- | ------------------------------------- | ------------------------------------------------------- |
| `POST /jobs`                | `submit TYPE [PAYLOAD]`               | Submits a job: `201`, or `200` for a repeated key       |
| `GET /jobs/{id}`            | `get ID`                              | Shows a job                                             |
| `DELETE /jobs/{id}`         | `cancel ID`                           | Cancels a job that hasn't finished                      |
| `GET /dlq`                  | `dlq list`                            | Lists dead jobs, the most recent first                  |
| `POST /dlq/{id}/replay`     | `dlq replay ID`                       | Runs a dead job again, with its attempts reset          |
| `DELETE /dlq`               | `dlq purge --yes`                     | Deletes every dead job                                  |
| `POST /crons`               | `cron add NAME SCHEDULE TYPE [PAYLOAD]` | Saves a cron, replacing one with the same name        |
| `GET /crons`, `/crons/{name}` | `cron list`, `cron get NAME`        | Shows crons                                             |
| `PATCH /crons/{name}`       | `cron enable NAME`, `cron disable NAME` | Turns a cron on or off, with `{"enabled": true}`      |
| `DELETE /crons/{name}`      | `cron rm NAME`                        | Deletes a cron                                          |
| `GET /stats`                | `stats`                               | Leader, workers, queue sizes and counters               |
| `GET /events`               | `events [-f]`                         | The latest events, or with `?after=ID` the next ones    |
| `GET /healthz`              | none                                  | `200` if Redis answers, `503` if not                    |

A job request looks like this. Only `type` is required, and durations are strings such as `"500ms"` or `"5m"`:

```json
{
  "type": "http",
  "payload": { "url": "https://example.com/hook", "body": { "n": 1 } },
  "delay": "30s",
  "max_attempts": 3,
  "timeout": "10s",
  "backoff": { "base": "1s", "max": "1m" }
}
```

- **Idempotency:** send an `Idempotency-Key` header (`jobctl submit --key`) to make retries safe. A repeated key returns the first job with `200` instead of making a second one, for 24 hours.
- **Errors:**
  - A mistake in the request gets `400` with a message saying what is wrong. Unknown fields count as mistakes, so a typo such as `max_attempt` isn't silently ignored.
  - A missing job or cron gets `404`.
  - Cancelling a finished job, or replaying one that isn't dead, gets `409`.
  - Anything else gets `500` with no details, and is logged.
- **Auth:** set `API_TOKEN` and every request except `/healthz` must send `Authorization: Bearer <token>`. `jobctl` reads the same variable. Without a token, anyone who can reach the API can submit `http` jobs that call any address your workers can reach, so the API logs a warning at start-up.

`jobctl` talks to `$JOBCTL_API` (default `http://localhost:8080`). Add `--json` to any command for the API's raw reply.

## Watching the cluster

[internal/monitor](internal/monitor) reads three things the rest of the system writes. Phase 8's API and Phase 9's dashboard are built on it.

- **Counters:** the `stats` hash counts what has happened: `submitted`, `claimed`, `succeeded`, `retried`, `dead`, `reclaimed`, `released`, `fenced`, `cancelled`, `replayed`, `purged` and `cron_fired`.
- **Events:** every script that changes a job's status also appends to the `events` stream, in the same atomic step. Each entry records the job, its type, the status it moved from and to, and the attempt number. It also records the worker for changes into or out of `running`, and the error if there is one. The stream keeps about the latest 10,000 events.

  | Event       | Change                                       |
  | ----------- | -------------------------------------------- |
  | `submitted` | new → `ready` or `scheduled`                 |
  | `promoted`  | `scheduled` → `ready`                        |
  | `claimed`   | `ready` → `running`                          |
  | `succeeded` | `running` → `succeeded`                      |
  | `failed`    | `running` → `scheduled`, to be retried       |
  | `dead`      | `running` → `dead`                           |
  | `reclaimed` | `running` → `ready`: its worker went silent  |
  | `released`  | `running` → `ready`: its worker shut down    |
  | `cancelled` | any unfinished status → `cancelled`          |
  | `replayed`  | `dead` → `ready`                             |
  | `purged`    | `dead` → deleted                             |

- **Workers:** each worker process checks in every second, recording its slots, how many jobs it is running, and whether it is shutting down. A worker that hasn't checked in for 5 seconds is listed as dead, and is dropped after an hour. A worker that shuts down cleanly removes itself once its jobs have stopped.

`Monitor.Snapshot` returns counters, queue sizes, the leader and the workers in one call. `RecentEvents` returns the latest events, and `EventsAfter` pages through the stream in order.

## Delivery

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

A call is limited by the job's `Timeout`, or by 30 seconds if the job has none. Error messages name only the address's scheme and host, because webhook URLs often carry a secret in the path or query string.

It does not sign requests or block internal addresses, which a hosted service would need.

## Configuration

Every binary reads the same environment variables. An empty variable counts as unset.

| Variable             | Default            | Meaning                                                                    |
| -------------------- | ------------------ | -------------------------------------------------------------------------- |
| `NODE_ID`            | `<hostname>-<pid>` | Identifies this process in leases and logs                                 |
| `LOG_LEVEL`          | `info`             | `debug`, `info`, `warn` or `error`                                         |
| `REDIS_ADDR`         | `localhost:6379`   | Redis host and port                                                        |
| `REDIS_PASSWORD`     | (none)             | Redis password                                                             |
| `REDIS_DB`           | `0`                | Redis database number. Tests use DB 15                                     |
| `HTTP_ADDR`          | `:8080`            | API and dashboard listen address                                           |
| `API_TOKEN`          | (none)             | If set, API requests must send `Authorization: Bearer <token>`             |
| `CONCURRENCY`        | `25`               | Jobs one worker process runs at once                                       |
| `LEASE_TTL`          | `1500ms`           | How long a claimed job stays owned without a heartbeat                     |
| `HEARTBEAT_EVERY`    | `500ms`            | How often a worker extends its leases. Must be at most half of `LEASE_TTL` |
| `REAP_EVERY`         | `200ms`            | How often the leading scheduler reclaims expired leases                    |
| `SHUTDOWN_GRACE`     | `5s`               | How long running jobs get to finish after a worker is told to stop         |
| `LEADER_TTL`         | `3s`               | How long a scheduler stays leader without renewing. Bounds failover time   |
| `LEADER_RENEW_EVERY` | `1s`               | How often the leader renews. Must be at most half of `LEADER_TTL`          |

Tests read `REDIS_TEST_ADDR` (default `localhost:6379`).
