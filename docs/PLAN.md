# Distributed Job Scheduler (Go + Redis): build plan

## Context

The goal is the project in this resume bullet:

> Fault-tolerant distributed job scheduler in Go + Redis. 25 concurrent workers, 100K+ scheduled jobs under load. Leader election, distributed leases, idempotent job claims, retries, dead-letter queues. 99%+ completion reliability, <2s worker-failure recovery in fault-injection tests.

**Change of approach:** Claude now implements the whole project (code, tests, running it, verifying it). You handle **all git work**: branches, commits, pushes and merges. Extras in scope: **HTTP API + CLI**, **cron schedules**, **web dashboard**, **`http` job type**.

**Current state**

- Phase 0 is done and merged to `main` at `github.com/sham-33/job-scheduler` (merge commit `2998a28`). Go 1.27.3 is installed.
- **The README in that merge commit still has its conflict markers.** The `<<<<<<< HEAD / ======= / >>>>>>>` lines are committed and pushed. GitHub's "Initial commit" added a one-line `# job-scheduler` README that clashed with the Phase 0 one.
- **The Go module path is wrong.** It's `github.com/shambutn/job-scheduler`, but your GitHub username is `sham-33`. It's used in 6 places: `go.mod` and the imports in `cmd/worker/main.go`, `internal/store/store.go` and `internal/store/store_test.go`.

**The rule that makes this work:** every state change to a job happens in a single atomic **Redis Lua script**. Leader election only makes the system efficient. The atomic scripts plus fencing tokens make it correct.

---

## How we work, for each phase

1. **Claude implements** the phase: code, unit tests, and integration tests against real Redis.
2. **Claude verifies:** `gofmt`, `go vet`, `go test -race ./...`, then actually runs the phase's **Done when** demo.
3. **Claude updates the docs:** the status line in `README.md`, plus a ✅ on the phase in `PLAN.md`.
4. **Claude stops and reports:**
   - What changed and what was verified, including anything that couldn't be checked.
   - A short explanation of the phase's key concept, for interview prep.
   - A **branch name and commit message** ready to copy-paste, in the `/create-pr` format.
5. **You** review, then branch, commit, push and merge. Say "next" to start the following phase.

**Git rule:** Claude only runs read-only git commands (`status`, `diff`, `log`, `show`). It never runs `add`, `commit`, `push`, `merge`, `checkout`/`switch`, `branch`, `stash`, `reset` or `rebase`.

**Redis while developing:**

- **Phases 1–9:** Claude starts a temporary Homebrew `redis-server` in its scratchpad folder for tests and demos, then stops it. Docker isn't needed.
- **Phases 10–12:** these need Docker Desktop running. Claude will ask you to start it.

---

## Architecture

```
            ┌──────────┐   HTTP/JSON   ┌──────────────────────┐
 jobctl ───▶│   api    │──────────────▶│                      │
 browser ──▶│ +dashbd  │               │        Redis         │
            └──────────┘               │  job:{id}   (HASH)   │
            ┌──────────┐ leader lease  │  jobs:scheduled ZSET │
            │scheduler │◀────────────▶│  jobs:ready     ZSET │
            │  ×2 (1   │ promote/reap/ │  jobs:leases    ZSET │
            │  leader) │ cron          │  jobs:dlq       ZSET │
            └──────────┘               │  stats HASH, events  │
            ┌──────────┐ claim/heart-  │  STREAM, workers ZSET│
            │ worker×5 │ beat/complete │                      │
            │ (5 gor.  │◀────────────▶│                      │
            │  each=25)│               └──────────────────────┘
            └──────────┘
```

**Job lifecycle:**
`scheduled → ready → running → succeeded`
`running → (fail or lease expired) → scheduled (retry with backoff) … → dead (DLQ)`
`any non-terminal state → cancelled`

**Redis keys**

| Key                                    | Type                       | Purpose                                                                                                                                   |
| -------------------------------------- | -------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `job:{id}`                             | HASH                       | id, type, payload, status, attempts, max_attempts, run_at, timeout_ms, idem_key, lease_owner, lease_token, last_error, created/updated_at |
| `jobs:scheduled`                       | ZSET score=run_at ms       | delayed jobs and retries waiting for backoff                                                                                              |
| `jobs:ready`                           | ZSET score=ready_at ms     | runnable now (FIFO by score)                                                                                                              |
| `jobs:leases`                          | ZSET score=lease_expiry ms | running jobs. The reaper scans for expired leases                                                                                         |
| `jobs:dlq`                             | ZSET score=died_at         | dead-letter queue                                                                                                                         |
| `idem:{key}`                           | STRING (TTL 24h)           | maps an idempotency key to a job id                                                                                                       |
| `lease:token`                          | STRING (INCR)              | monotonically increasing fencing token                                                                                                    |
| `scheduler:leader` / `scheduler:epoch` | STRING PX / INCR           | leader lease and leadership epoch                                                                                                         |
| `cron:{name}` / `crons`                | HASH / SET                 | cron definitions                                                                                                                          |
| `cron:fired:{name}:{ts}`               | STRING NX TTL              | dedupe for each cron fire                                                                                                                 |
| `workers`                              | ZSET score=last_seen       | worker registry                                                                                                                           |
| `stats`                                | HASH                       | counters: submitted, succeeded, retried, dead, reclaimed…                                                                                 |
| `events`                               | STREAM (MAXLEN ~10k)       | lifecycle events, used by the dashboard and to measure recovery time                                                                      |

All timestamps inside scripts come from `redis.call('TIME')`, so every node agrees on the clock.

**Layout**

```
cmd/{api,scheduler,worker,jobctl,loadgen,chaos}/
internal/{config,logging,store,job,queue,election,scheduler,worker,api,dashboard,events}/
internal/queue/scripts/*.lua   (embedded with go:embed, next to the Go code that calls them)
internal/handlers/             (job types: sleep, echo, flaky, http)
internal/store/storetest/      (test helper: real Redis on DB 15 plus a key prefix per test)
docs/{PLAN.md,design.md,results.md,interview.md}
```

Every key name comes from `store.Keys`, which adds an optional prefix. Production uses no prefix. Each test gets a random prefix, so test packages can run in parallel against one Redis.

**Reuse from Phase 0:**

- `config.Load()` ([internal/config/config.go](../internal/config/config.go)) already has `Concurrency`, `LeaseTTL`, `HeartbeatEvery`, `ReapEvery` and `HTTPAddr`.
- `store.Connect()` ([internal/store/store.go](../internal/store/store.go)).
- `logging.New()` ([internal/logging/logging.go](../internal/logging/logging.go)).
- The `signal.NotifyContext` shutdown pattern in [cmd/worker/main.go](../cmd/worker/main.go).
- `storetest.Client(t)` and `storetest.Keys(t, rdb)` ([internal/store/storetest](../internal/store/storetest/storetest.go)) for every Redis integration test.

**Dependencies to add when needed:** `github.com/robfig/cron/v3` (Phase 6), `github.com/spf13/cobra` (Phase 8). `github.com/google/uuid` was added in Phase 1.

---

## Phase 0: Setup ✅ done

Covers the module, config, store, logging, a worker skeleton, docker-compose, the Makefile and the README. It's merged to `main`.

---

## Phase 1: Housekeeping, the job model, enqueue, and a single worker ✅ done

**As built, differences from the steps below:**

- The Lua scripts live in `internal/queue/scripts/`, not `internal/store/`.
- `scripts/lib.lua` provides a shared `now_ms()` and is prepended to every script.
- The handlers live in `internal/handlers/`.
- A failed job stays `running` until Phases 3 and 4 add the reaper and retries.

**Housekeeping first:**

1. Fix `README.md`. Drop the conflict markers and GitHub's one-line `# job-scheduler` heading, and keep the full Phase 0 README.
2. Change the module path to `github.com/sham-33/job-scheduler`: run `go mod edit -module` and update the 3 Go files that import it.
3. Replace `PLAN.md` in the repo with this plan.

**Then build:** 4. `internal/job`: the `Job` struct, `Status` constants, and conversion to and from a Redis hash. 5. `internal/store`: key-name helpers, plus embedded Lua scripts loaded with `redis.NewScript`. 6. `enqueue.lua`, all in one atomic step:

- **Idempotency check:** if the `idem:{key}` mapping exists, return the existing job id.
- Otherwise `HSET job:{id}`, then ZADD to `jobs:ready` (if due) or `jobs:scheduled` (if delayed).
- Bump `stats`.
- Doing all of this in one script means a crash can never leave an idempotency key pointing at a job that doesn't exist.

7. `claim.lua`:
   - `ZPOPMIN jobs:ready`, skipping any job whose status isn't `ready`.
   - `INCR lease:token` for the fencing token.
   - Set `status=running`, the owner and the token, and `attempts++`.
   - `ZADD jobs:leases`.
8. `complete.lua`: if the token matches, set `succeeded` and remove the lease. Otherwise return `ErrLeaseLost`.
9. `internal/queue`: Go wrappers `Enqueue`, `Claim`, `Complete`, `Get`.
10. `internal/worker`: a handler registry plus a **single-goroutine** loop: claim, run, complete. Handlers `echo` and `sleep` go in `handlers/`.
11. A temporary promoter goroutine inside the worker moves due jobs from `scheduled` to `ready` (`promote.lua`). Phase 5 moves it to the scheduler.

**Done when:**

- An immediate job and a job delayed by 5 s both run, at the right times.
- Submitting twice with the same idempotency key returns the same id.
- Integration tests cover each script.

---

## Phase 2: Concurrency, 25 workers ✅ done

**As built:**

- `worker.Pool` replaces the single `Worker`. Each goroutine is a slot, and `lease_owner` is `<node>/<slot>`.
- The go-redis pool size is `2 × CONCURRENCY + 4`: a claim or completion plus a heartbeat per slot, and a few background loops.

**Measured:**

- 10,000 jobs on 25 slots: 0 duplicates, about 15,000 jobs/s with no-op handlers under `-race`.
- With a naive multi-command claim swapped in, the same test fails: 9,219 of 10,000 jobs ran more than once.
- 100 jobs × 50 ms took 5,153 ms at `CONCURRENCY=1` and 206 ms at `CONCURRENCY=25`.

1. `worker.Pool` runs `CONCURRENCY` goroutines. When the queue is empty, each backs off with jitter (10 ms up to 200 ms).
2. Panics inside handlers are recovered and turned into errors.
3. The go-redis pool size scales with concurrency.

**Done when:** an integration test with 25 goroutines and 10K jobs proves **every job ran exactly once**, under `-race`.
**Key concept:** why a naive `ZRANGE` + `ZREM` claim races, and how Lua makes the claim atomic.

---

## Phase 3: Leases, heartbeats, fencing, and the reaper ✅ done

**As built:**

- A reclaimed job goes straight back to `jobs:ready` with its original `run_at` as the score, so it runs ahead of newer work. There is no backoff, which is what keeps recovery under 2 s.
- `stats.reclaimed` and `stats.fenced` count takeovers and rejected calls from stale workers. The `events` stream arrives in Phase 7.
- A worker that can't reach Redis for a whole lease cancels its job, because it has to assume the job was handed to someone else.
- `ContextTimeoutEnabled` is on for go-redis. Without it, a call to a hung Redis blocks for 3 s, longer than a lease.
- New setting: `SHUTDOWN_GRACE` (default 5 s).

**Measured with real processes, default settings:**

- `kill -9` mid-job, 30 trials on an idle cluster: min 1,116 ms, p50 1,341 ms, p95 1,526 ms, max 1,568 ms. All 30 were under 2 s.
- Frozen worker woken after takeover: its completion was rejected, with 1 success recorded and `fenced=1`.
- 9 deliberate code breaks in a throwaway copy were all caught by the tests.

1. `heartbeat.lua`: if the token matches, run `ZADD jobs:leases XX now+TTL`. If it doesn't, return "lost". Each running job gets a heartbeat goroutine (`HeartbeatEvery`). Losing the lease **cancels the job's context**.
2. `reap.lua`: find expired leases and treat each as a failed attempt with reason `lease_expired`. Emit a `reclaimed` event. For now it runs inside the worker process; Phase 5 moves it to the scheduler.
3. Graceful shutdown: stop claiming, wait for a grace period, then exit. Any leases left behind expire.

**Done when:**

- `kill -9` on a worker mid-job leads to the job being re-claimed in **under 2 s**. Claude measures and reports the time.
- **Zombie test:** a worker paused with `kill -STOP` past its lease, then resumed, has its completion **rejected**.

**Key concept:** leases versus locks, and fencing tokens.

---

## Phase 4: Retries, backoff, timeouts, DLQ, cancel, and the `http` job type ✅ done

**As built:**

- Retry settings live on each job (`max_attempts`, `timeout_ms`, `backoff_base_ms`, `backoff_max_ms`), so one queue can hold jobs with different rules. Defaults: 5 attempts, 1 s first wait, 1 min longest wait, no timeout.
- The wait doubles per failure and is picked at random from the upper half of that value.
- A job interrupted by worker shutdown is not counted as a failure. Its lease expires and the reaper requeues it at once, as in Phase 3.
- A running job that is cancelled stops at its worker's next heartbeat. Cancelled jobs are not counted in `stats.fenced`.
- Replay resets `attempts` to 0 and keeps `last_error`.
- A job whose worker keeps dying is also capped: the reaper moves it to the DLQ once its attempts are used up.
- Bad payloads in `sleep`, `flaky` and `http` are permanent failures.
- The `http` handler has a 30 s cap per call for jobs with no timeout of their own.
- New counters: `stats.retried`, `dead`, `cancelled`, `replayed`.
- Listing the DLQ arrives with the API in Phase 8.

**Measured with real processes (4 workers, 200 ms first wait):**

- An always-failing job waited 158 ms, 324 ms, 772 ms and 1,241 ms between attempts, then reached the DLQ after attempt 5.
- An `http` job got 500, 500, then 200, and succeeded on attempt 3. One that got 400 was dead after a single call.
- `kill -9` recovery, 3 trials: 1,375 ms, 1,308 ms and 1,365 ms, so still under 2 s.
- 15 deliberate code breaks in a throwaway copy were all caught by the tests. One was missed at first, and the cancel test was tightened to catch it.

1. Spec fields: `MaxAttempts` (default 5), `TimeoutMs`, `Backoff{Base, Max}`.
2. Backoff is computed in Go (`min(max, base*2^attempt) + jitter`) and passed into Lua. It has unit tests.
3. `fail.lua` (token-checked): either a retry (back into `jobs:scheduled`) or dead (into `jobs:dlq`). `reap.lua` gets the same attempts cap, but keeps requeueing immediately with no backoff, so worker-failure recovery stays under 2 s.
4. Per-job timeouts with `context.WithTimeout`.
5. `job.Permanent(err)` skips retries and goes straight to the DLQ.
6. `cancel.lua` and `replay.lua`.
7. A `flaky` handler with failure probability `p`.
8. An `http` job type, so people can schedule their own work without writing Go or changing the worker. The payload is `{url, method, headers, body}`, and `method` defaults to `POST`. The worker calls the address and adds `X-Job-Id` and `X-Job-Attempt` headers, so the receiver can spot a repeat:
   - A 2xx reply means the job succeeded.
   - A 5xx, 408 or 429 reply, a network error, or a timeout means retry with backoff.
   - Any other reply is `job.Permanent`, straight to the DLQ.

**Done when:**

- `flaky p=0.5` succeeds after retries with growing delays, and an always-failing job reaches the DLQ after 5 attempts and can be replayed.
- `http` jobs against a fake test server end correctly for each reply: 200 succeeds, 500 retries and succeeds once the server recovers, a server that never answers times out and retries, and 400 goes straight to the DLQ.

**Key concept:** why a scheduler calls the user's code instead of running it.

---

## Phase 5: Scheduler service and leader election

1. `internal/election`:
   - Acquire with `SET scheduler:leader <node> NX PX 3000`, plus `INCR scheduler:epoch`.
   - Renew with a compare-and-PEXPIRE Lua script every 1 s.
   - Release with compare-and-DEL.
   - `OnElected` / `OnDemoted` callbacks. If a renew fails, demote immediately.
2. `cmd/scheduler`: only the leader runs the promoter (every 100 ms, up to 1,000 jobs), the reaper, and the cron loop (Phase 6). Remove the temporary loops from the worker.
3. Add a `run-scheduler` Makefile target.

**Done when:** with 2 schedulers running, `kill -9` the leader and the other takes over within about 3 s. No job is lost or double-promoted.
**Key concept:** why a brief two-leader overlap is harmless here, and the leader epoch.

---

## Phase 6: Cron schedules

1. A `cron:{name}` hash holds spec, job template, `next_run` and `enabled`. Names go in the `crons` set.
2. The leader runs `SET cron:fired:{name}:{ts} NX`, and only if that succeeds enqueues a job with idempotency key `cron:{name}:{ts}`.
3. Missed-fire policy: fire once, skip the rest.
4. Use `robfig/cron/v3`'s parser for `Next()`. It has unit tests.

**Done when:** `*/1 * * * *` fires exactly once per minute, including across a forced leader failover.

---

## Phase 7: Stats, events, and the worker registry

1. Each script XADDs an `events` entry (`MAXLEN ~10000`) with `job_id, type, from, to, worker, ts`.
2. Workers run `ZADD workers now <id>` every 1 s. A worker unseen for more than 5 s counts as dead.
3. `internal/events` helpers for stats, recent events, live workers, the leader and queue depths.

**Done when:** a test shows counters and events updating as jobs flow through.

---

## Phase 8: HTTP API and CLI

**API** (`net/http` with Go 1.22+ routing):

- `POST /jobs` (supports the `Idempotency-Key` header), `GET /jobs/{id}`, `DELETE /jobs/{id}`.
- `GET /dlq`, `POST /dlq/{id}/replay`, `DELETE /dlq`.
- `POST|GET /crons`, `DELETE|PATCH /crons/{name}`.
- `GET /stats`, `GET /healthz`.
- JSON errors and logging middleware.

**CLI:** `cmd/jobctl` with cobra: `submit`, `get`, `cancel`, `dlq list|replay|purge`, `cron add|list|rm`, `stats`.

Add a `run-api` Makefile target.

**Done when:** `httptest` handler tests pass, and a `jobctl` smoke test against a running stack exercises every feature, including a delayed `http` job that calls a small local server.

---

## Phase 9: Web dashboard

A static HTML page with vanilla JS, embedded in the API and served at `/`. It polls `/stats` every 1 s.

Panels: leader and epoch, live workers, queue depths, counters and throughput, a recent events feed, a DLQ table with Replay buttons, and a cron list with enable/disable.

**Done when:** Claude opens `http://localhost:8080` in the browser pane, runs `http` jobs against a small local server, kills a worker, and confirms it turns red while a `reclaimed` event appears. Claude shares a screenshot.

---

## Phase 10: Dockerize and scale out (needs Docker Desktop)

1. A multi-stage `Dockerfile` that builds all binaries.
2. Compose services: `redis`, `api`, `scheduler` ×2, and `worker` ×5 with `CONCURRENCY=5` (**25 workers**).
3. Healthchecks and `stop_grace_period`.

**Done when:** `docker compose up` brings everything up, and the dashboard shows 1 leader and 5 workers.

---

## Phase 11: Load test, 100K+ jobs

`cmd/loadgen`:

- Pipelined submission of 100K jobs. The mix is about 70% immediate, 20% delayed (0–60 s), and 10% `flaky p=0.2`, with `sleep` durations of 5–50 ms.
- Reports throughput, latency p50/p95/p99, counts, and the completion rate.
- Writes the results to `docs/results.md`.
- If it's too slow: tune the pool size, the promote batch size, and add a batch claim.

**Done when:** all 100K jobs reach a terminal state, the completion rate is ≥ 99%, and the numbers are recorded.

---

## Phase 12: Fault injection, proving 99%+ and <2s

`cmd/chaos` runs during loadgen:

- `docker kill` a random worker every 5–15 s, then restart it.
- Kill the leader every 30 s.
- `docker pause` / `docker unpause` a worker (the zombie case).
- Optionally restart Redis.

**Measured from `events`:**

- Recovery time p50/p99/max (target under 2 s).
- Completion reliability, `succeeded / (submitted − intentional permanent failures)`, with the raw counts shown alongside.
- Duplicate completions, which must be 0.

**Done when:** `docs/results.md` contains a table from at least 3 chaos runs.

---

## Phase 13: Docs, CI, and interview prep

- `README.md`: architecture diagram, quick start, and a dashboard screenshot.
- `docs/design.md`:
  - Data model and state machine.
  - Each Lua script.
  - Delivery semantics: delivery is at-least-once, and you get effectively-once results through fencing plus idempotent handlers.
  - Tradeoffs: Redis is a single point of failure (mitigations: Sentinel or Cluster, plus `{jobs}` hash tags) and the choice of lease TTL.
  - What the `http` job type leaves out that a hosted product would need: signed requests, so receivers can trust them, and blocking calls to internal addresses (SSRF).
- `.github/workflows/ci.yml`: `go vet` and `go test -race` with a Redis service container. You push it.
- `docs/interview.md`: written answers to:
  - Why Lua instead of `MULTI`/`WATCH`?
  - What if a worker pauses for 10 s?
  - What if two leaders exist for a moment?
  - How would you make Redis highly available?
  - Why ZSET over LIST or Streams?
  - How would other teams use this without changing your code?

---

## Verification (whole project)

1. `make test` passes: unit tests plus Redis integration tests, all with `-race`.
2. `docker compose up --scale worker=5`, then a `jobctl` smoke test of every feature.
3. Loadgen with 100K jobs reaches ≥ 99% completion.
4. Chaos plus loadgen: recovery p99 under 2 s, 0 duplicate completions, leader failover under about 3 s.
5. The dashboard shows all of this live at `http://localhost:8080`.
