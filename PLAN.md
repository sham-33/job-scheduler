* [ ] 

# Distributed Job Scheduler (Go + Redis): step-by-step build plan

## Context

You want to build, from scratch, the project in this resume bullet:

> Fault-tolerant distributed job scheduler in Go + Redis. 25 concurrent workers, 100K+ scheduled jobs under load. Leader election, distributed leases, idempotent job claims, retries, dead-letter queues. 99%+ completion reliability, <2s worker-failure recovery in fault-injection tests.

**How we'll work:** you write the code and this plan is your guide. Each phase lists the concepts, files, Redis data structures and scripts, and a **Done when** checkpoint. After each phase, ask me to review it or help you debug. Extras you picked: **HTTP API + CLI**, **cron schedules**, **web dashboard**.

**Current state:** `/Users/shambutn/personal/Job-scheduler` is empty. Redis and Docker are installed. **Go is not installed.** The folder sits inside the parent `personal` git repo (branch `sham`). Give this project its own repo so you can link it on GitHub.

**The rule that makes this work:** every state change to a job happens in a single atomic **Redis Lua script**. Leader election only makes the system efficient. The atomic scripts plus fencing tokens make it correct. This is your main interview talking point.

---

## Architecture (what you're building)

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

**Job lifecycle (state machine):**
`scheduled → ready → running → succeeded`
`running → (fail or lease expired) → scheduled (retry with backoff) … → dead (DLQ)`
`any non-terminal state → cancelled`

**Redis keys**

| Key                                        | Type                       | Purpose                                                                                                                                   |
| ------------------------------------------ | -------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `job:{id}`                               | HASH                       | id, type, payload, status, attempts, max_attempts, run_at, timeout_ms, idem_key, lease_owner, lease_token, last_error, created/updated_at |
| `jobs:scheduled`                         | ZSET score=run_at ms       | delayed jobs and retries waiting for backoff                                                                                              |
| `jobs:ready`                             | ZSET score=ready_at ms     | runnable now (FIFO by score)                                                                                                              |
| `jobs:leases`                            | ZSET score=lease_expiry ms | running jobs. The reaper scans for expired leases                                                                                         |
| `jobs:dlq`                               | ZSET score=died_at         | dead-letter queue                                                                                                                         |
| `idem:{key}`                             | STRING (TTL 24h)           | maps an idempotency key to a job id                                                                                                       |
| `lease:token`                            | STRING (INCR)              | monotonically increasing fencing token                                                                                                    |
| `scheduler:leader` / `scheduler:epoch` | STRING PX / INCR           | leader lease and leadership epoch                                                                                                         |
| `cron:{name}` / `crons`                | HASH / SET                 | cron definitions                                                                                                                          |
| `cron:fired:{name}:{ts}`                 | STRING NX TTL              | dedupe for each cron fire                                                                                                                 |
| `workers`                                | ZSET score=last_seen       | worker registry for the dashboard                                                                                                         |
| `stats`                                  | HASH                       | counters: submitted, succeeded, retried, dead, reclaimed…                                                                                |
| `events`                                 | STREAM (MAXLEN ~10k)       | lifecycle events used by the dashboard and for measuring recovery time                                                                    |

All timestamps inside scripts come from `redis.call('TIME')`, so every node agrees on the clock and client clock skew can't cause bugs.

**Project layout**

```
Job-scheduler/
  go.mod  Makefile  README.md  Dockerfile  docker-compose.yml
  cmd/api/  cmd/scheduler/  cmd/worker/  cmd/jobctl/  cmd/loadgen/  cmd/chaos/
  internal/
    config/      env-based config (REDIS_ADDR, LEASE_TTL, CONCURRENCY…)
    job/         Job struct, Status consts, state-transition helpers
    store/       go-redis client, key names, embedded Lua scripts (scripts/*.lua via embed.FS)
    queue/       Enqueue, Claim, Heartbeat, Complete, Fail, Cancel, Replay (Go wrappers over Lua)
    election/    leader lease acquire/renew/release
    scheduler/   promoter, reaper, cron loop
    worker/      pool, handler registry, heartbeat loop
    api/         net/http handlers (Go 1.22+ method routing, no framework)
    dashboard/   embedded static HTML/JS
    events/      stats counters + event stream helpers
  handlers/      example job types: sleep, echo, flaky, http
  docs/          design.md, results.md
```

**Dependencies:** `github.com/redis/go-redis/v9`, `github.com/robfig/cron/v3` (only to parse cron specs), `github.com/google/uuid`, `github.com/spf13/cobra` (CLI). Standard library for everything else: `log/slog`, `net/http`, `context`, `embed`.

---

## Phase 0: Setup (½ day)

1. `brew install go` (Go 1.23+). Check with `go version`.
2. In `Job-scheduler/`: `git init`, `go mod init github.com/<you>/job-scheduler`, then create the folder layout above.
3. `docker-compose.yml` with only `redis:7` for now (enable AOF: `--appendonly yes`).
4. `Makefile` targets: `redis-up`, `test`, `build`, `run-worker`, `run-scheduler`, `run-api`.
5. `internal/config`: read env vars with defaults.
6. `internal/store`: create the Redis client and ping it on startup.

**Learn:** Go modules, `context`, `slog`, project layout.
**Done when:** `go run ./cmd/worker` connects to Redis, logs "connected", and exits cleanly on Ctrl-C (use `signal.NotifyContext`).

---

## Phase 1: Job model and a naive single-node queue (1–2 days)

1. `internal/job`: `Job` struct, `Status` type, and JSON plus Redis-hash (de)serialization.
2. `queue.Enqueue(ctx, spec)`: write `job:{id}` (HSET), then ZADD to `jobs:ready` if `run_at <= now`, else to `jobs:scheduled`.
3. **Idempotent submission:** if the spec has an `IdempotencyKey`, `SET idem:{key} {id} NX EX 86400`. If the key already exists, return the existing job id instead of creating a duplicate.
4. `worker`: a handler registry (`map[string]HandlerFunc`, where `HandlerFunc func(ctx, *Job) error`). Add handlers `echo` and `sleep`.
5. Write the claim the **naive way on purpose**: `ZRANGE jobs:ready 0 0`, then `ZREM`, then `HSET status=running` as separate commands.
6. A temporary loop inside the worker that moves due jobs from `scheduled` to `ready`.

**Learn:** Redis HASH and ZSET commands, why a ZSET works as a time-ordered queue.
**Done when:** you enqueue an immediate job and a job delayed by 5 s, and one worker runs both at the right times.
**Experiment:** start 2 workers and enqueue 1,000 jobs while counting executions per job id. You'll see **duplicate executions**. That race is the motivation for Phase 2.

---

## Phase 2: Atomic claims with Lua, and 25 concurrent workers (2 days)

1. Write `store/scripts/claim.lua`. In one atomic step it:
   - `ZPOPMIN jobs:ready`. If the job's status isn't `ready` (for example it was cancelled), skip it and pop again, up to N tries.
   - `INCR lease:token` to get the fencing token.
   - `HSET status=running lease_owner=<worker> lease_token=<t>`, `HINCRBY attempts 1`.
   - `ZADD jobs:leases now+leaseTTL id`, `HINCRBY stats ...`.
   - It returns the job fields and the token.
2. Write `complete.lua`: **only if `lease_token` matches**, set `status=succeeded`, `ZREM jobs:leases`, and store the result. If the token doesn't match, return `ErrLeaseLost`.
3. Load scripts once with `redis.NewScript(...)` (it uses EVALSHA and falls back to EVAL).
4. `worker.Pool`: N goroutines (`CONCURRENCY=25`). Each loops claim → run → complete. When the queue is empty, back off with jitter (10 ms up to 200 ms).
5. Recover from panics inside handlers so a crashing handler never kills its goroutine.

**Learn:** Redis runs Lua scripts single-threaded and atomically, plus goroutines, `sync.WaitGroup`, and `errgroup`.
**Done when:** you have an integration test (real Redis on DB 15, flushed for each test) with 25 goroutines and 10K jobs that asserts **every job ran exactly once**. Run it with `go test -race`.

---

## Phase 3: Distributed leases, heartbeats, fencing, and the reaper (2 days)

1. Defaults: `LEASE_TTL=1500ms`, `HEARTBEAT_EVERY=500ms` (3 heartbeats fit in each TTL), `REAP_EVERY=200ms`.
2. `heartbeat.lua`: if the token matches, `ZADD jobs:leases XX now+TTL id`. If it doesn't, return "lost". A heartbeat goroutine runs for each running job. When the lease is lost, it **cancels the job's context** so the handler stops.
3. `reap.lua`: `ZRANGEBYSCORE jobs:leases -inf now LIMIT 0 100`. For each expired lease, treat it as a failed attempt with reason `lease_expired`: requeue with backoff, or move to the DLQ (Phase 4 logic). Emit a `reclaimed` event. For now, run it in a standalone goroutine; Phase 5 moves it into the scheduler.
4. Graceful shutdown: on SIGTERM, stop claiming, wait up to a grace period for in-flight jobs, then exit. Any leases left behind simply expire and get reaped.

**Learn:** leases versus locks, why you need fencing tokens (a GC pause or network partition can leave a "zombie" worker that later tries to complete a job someone else now owns), and how lease TTL trades recovery speed against false expiries.
**Done when:**

- You `kill -9` a worker mid-job and the job is re-claimed by another worker in **under 2 s** (worst case is TTL plus the reap interval, about 1.7 s).
- **Zombie test:** pause a worker with `kill -STOP` past its lease, let another worker finish the job, then resume the first one with `kill -CONT`. Its completion is **rejected** because its token is stale.

---

## Phase 4: Retries, backoff, timeouts, DLQ, and cancel (1–2 days)

1. Spec fields: `MaxAttempts` (default 5), `TimeoutMs`, `Backoff{Base, Max}`.
2. Backoff is computed in Go and passed to Lua as an argument: `min(max, base * 2^attempt) + jitter`. Unit-test it.
3. `fail.lua` (token-checked): if `attempts < max_attempts`, set `status=scheduled` and `ZADD jobs:scheduled now+delay`. Otherwise set `status=dead` and ZADD to `jobs:dlq`. Always `ZREM jobs:leases`, set `last_error`, and bump stats. `reap.lua` reuses this logic.
4. Per-job timeouts with `context.WithTimeout`. A timeout counts as a failure.
5. Let handlers return a **permanent** error (`job.Permanent(err)`) that skips the remaining retries and goes straight to the DLQ.
6. `cancel.lua`: set `status=cancelled` and ZREM from scheduled and ready. A running job finishes, but `complete.lua` keeps the cancelled status.
7. `replay.lua`: move a job from the DLQ to ready and reset attempts.
8. Add a `flaky` handler that fails with probability `p`, which the later fault tests use.

**Done when:** a `flaky p=0.5` job eventually succeeds after retries with growing delays, and an always-failing job ends up in the DLQ after 5 attempts and can be replayed.

---

## Phase 5: Scheduler service and leader election (2 days)

1. `election.Elector`:
   - **Acquire:** `SET scheduler:leader <nodeID> NX PX 3000`. On success, `INCR scheduler:epoch`.
   - **Renew** every 1 s with a compare-and-PEXPIRE Lua script (renew only if the value is still `nodeID`).
   - **Release** on shutdown with compare-and-DEL.
   - Expose `IsLeader()` plus `OnElected` / `OnDemoted` callbacks. If a renew fails, demote yourself immediately.
2. `cmd/scheduler`: every node runs the elector. **Only the leader** runs these loops:
   - **Promoter** (every 100 ms): `promote.lua` moves up to 1,000 due jobs from `scheduled` to `ready` and sets status in the same script.
   - **Reaper**: moved here from Phase 3.
   - **Cron loop**: Phase 6.
3. Remove the temporary promoter loop from the worker.

**Learn:** lease-based leader election, why a brief two-leader overlap is **harmless here** (promote and reap are atomic and idempotent), and the epoch as a fencing token for leaders.
**Done when:** with 2 schedulers running, `kill -9` the leader and the other takes over within about 3 s. No job is lost or double-promoted, which you check by counting events.

---

## Phase 6: Cron schedules (1–2 days)

1. A cron definition (`cron:{name}` HASH) holds `spec`, `job_template`, `next_run`, `enabled`. Names are kept in the `crons` SET.
2. Each tick, the leader computes the fire time `ts` for every due cron and runs `SET cron:fired:{name}:{ts} NX EX 3600`. **Only if that succeeds**, it enqueues a job with idempotency key `cron:{name}:{ts}` and advances `next_run`. This double dedupe makes firing idempotent even during a leader failover.
3. Missed-fire policy: if the scheduler was down, fire **once** and skip the rest. Document this choice.
4. Use `robfig/cron/v3`'s parser only for `Next()` calculations. Unit-test it.

**Done when:** a `*/1 * * * *` cron fires exactly once per minute, including across a forced leader failover.

---

## Phase 7: Stats, events, and the worker registry (1 day)

1. Every Lua script already bumps `stats` counters. Also XADD to `events` with `MAXLEN ~ 10000`, using fields `job_id, type, from, to, worker, ts`.
2. Workers `ZADD workers now <workerID>` every second. The API treats any worker unseen for more than 5 s as dead.
3. `events` helpers: read stats, read recent events, list live workers, read the current leader, and get queue depths (ZCARD of each ZSET).

**Done when:** a small Go test or `redis-cli` shows counters and events updating as jobs flow through the system.

---

## Phase 8: HTTP API and CLI (2 days)

**API** (`cmd/api`, `net/http` with `mux.HandleFunc("POST /jobs", …)`):

- `POST /jobs` submits a job and supports the `Idempotency-Key` header. `GET /jobs/{id}`. `DELETE /jobs/{id}` cancels.
- `GET /dlq`, `POST /dlq/{id}/replay`, `DELETE /dlq` (purge).
- `POST /crons`, `GET /crons`, `DELETE /crons/{name}`, `PATCH /crons/{name}` (enable/disable).
- `GET /stats` returns leader, live workers, queue depths, counters, and recent events.
- `GET /healthz`.
- Request validation, JSON errors, and request logging middleware.

**CLI** (`cmd/jobctl`, cobra): `submit --type sleep --payload '{"ms":100}' --delay 5s --max-attempts 3 --idem-key x`, `get <id>`, `cancel <id>`, `dlq list|replay|purge`, `cron add|list|rm`, `stats`.

**Done when:** you can drive every feature from `jobctl`, and `httptest` handler tests pass.

---

## Phase 9: Web dashboard (1–2 days)

- `internal/dashboard`: a static `index.html` plus vanilla JS, served by the API through `embed.FS` at `/`.
- It polls `GET /stats` every 1 s. A later upgrade is Server-Sent Events from the events stream.
- Panels: **current leader and epoch**, **live workers** (green or red by last-seen), **queue depths** (scheduled/ready/running/DLQ), **counters** (succeeded, retried, dead, reclaimed, throughput per second), a **recent events** feed, a **DLQ table with Replay buttons**, and a **cron list** with enable/disable.

**Done when:** you open `http://localhost:8080`, kill a worker, and watch it turn red while its job shows a `reclaimed` event.

---

## Phase 10: Dockerize and scale out (1 day)

1. A multi-stage `Dockerfile` builds all binaries into one small image. Each compose service sets its own `command`.
2. `docker-compose.yml`: `redis`, `api`, `scheduler` ×2, and `worker` ×5 with `CONCURRENCY=5`, which gives **25 concurrent workers**. Scale with `docker compose up --scale worker=5`.
3. Add healthchecks, `stop_grace_period`, and environment-based tuning.

**Done when:** `docker compose up` brings up the whole cluster and the dashboard shows 1 leader and 5 workers.

---

## Phase 11: Load test, 100K+ jobs (1–2 days)

`cmd/loadgen`:

- Submits 100K jobs over **pipelined** batches of 500. The mix is about 70% immediate, 20% delayed (0–60 s), and 10% `flaky p=0.2`, with `sleep` durations of 5–50 ms.
- Waits until every job is terminal. It reports throughput (jobs/s), latency p50/p95/p99 from `run_at` to start and to finish, the succeeded/dead/retried counts, and the **completion rate**.
- Writes the results to `docs/results.md`.

**Tuning if it's slow:** a bigger go-redis pool, promote batch size, a batch-claim variant of `claim.lua`, fewer `HSET` fields, and an events `MAXLEN`.

**Done when:** 100K jobs reach a terminal state, the completion rate is ≥ 99%, and the numbers are recorded.

---

## Phase 12: Fault injection, proving 99%+ and <2s (1–2 days)

`cmd/chaos` runs while loadgen runs:

- Every 5–15 s, `docker kill` a random worker container, then restart it.
- Every 30 s, kill the **leader** scheduler.
- `docker pause` a worker past its lease, then unpause it (the zombie case, so fencing must reject it).
- Optional: restart Redis with AOF on, to show that data survives.

**Measure from the `events` stream:**

- **Worker-failure recovery time:** the time from a worker's last heartbeat to its job's `reclaimed` and then `running` event on another worker. Report p50, p99, and max. The target is under 2 s.
- **Completion reliability:** `succeeded / (submitted − intentionally-permanent-failures)`. Report the raw numbers too, and be honest about how you define the metric.
- **Duplicate completions:** must be 0, thanks to fencing.

**Done when:** `docs/results.md` contains a table of these numbers from at least 3 chaos runs.

---

## Phase 13: Docs, polish, and interview prep (1 day)

- `README.md`: an architecture diagram, a quick start (`docker compose up`, `jobctl submit`), and a dashboard screenshot.
- `docs/design.md`:
  - Data model and state machine.
  - Each Lua script and why it's atomic.
  - **Delivery semantics:** delivery is at-least-once. You get effectively-once results through fencing plus idempotent handlers.
  - Leader election tradeoffs.
  - **Known limits:** Redis is a single point of failure (mitigations: Sentinel or Cluster, plus `{jobs}` hash tags so multi-key scripts stay on one slot), clock handling, and false lease expiry when TTLs are too low.
- CI with GitHub Actions running `go vet`, `go test -race`, and a Redis service container.
- An interview Q&A list. Practise answering each question in two minutes:
  - Why Lua instead of `MULTI`/`WATCH`?
  - What happens if a worker pauses for 10 s?
  - What if two leaders exist for a moment?
  - How would you make Redis highly available?
  - Why ZSET over LIST or Streams?

---

## Verification (end to end)

1. `make test` runs unit tests (backoff, cron next-run, state transitions) and integration tests against a real Redis on DB 15 (one per Lua script, plus the exactly-once test with 25 goroutines), all with `-race`.
2. `docker compose up --scale worker=5`, then the `jobctl` smoke test: submit, get, cancel, DLQ replay, add a cron.
3. `go run ./cmd/loadgen -n 100000` gives completion ≥ 99%.
4. Run `go run ./cmd/chaos` together with loadgen: recovery p99 under 2 s, 0 duplicate completions, leader failover under about 3 s.
5. Watch all of it live on the dashboard at `http://localhost:8080`.

**Rough total:** 3–4 weeks part-time. Phases 2, 3 and 5 are the core distributed-systems learning, so spend the most time there.
