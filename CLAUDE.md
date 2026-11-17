# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

A distributed job scheduler in Go and Redis (module `github.com/sham-33/job-scheduler`), built phase by phase from [docs/PLAN.md](docs/PLAN.md). The plan holds the architecture, the Redis key table, the remaining phases, and, for each finished phase, "As built" and "Measured" notes. Read the relevant phase before starting work.

## Working agreement

- **Git:** the user does all git work. Run only read-only git commands (`status`, `diff`, `log`, `show`, `fetch`). Never run `add`, `commit`, `push`, `merge`, `checkout`/`switch`, `branch`, `stash`, `reset` or `rebase` unless explicitly asked. When a phase or fix is done, suggest a branch name (`feat/phase-N-...`, `fix/...`) and a one-line conventional commit message.
- **One phase at a time:** implement it, verify it, then stop and report. Wait for the user to say "next".
- **Each phase updates the docs:** the **Status** line in README.md, and a ✅ plus "As built" and "Measured" notes on the phase in docs/PLAN.md.
- **Verify with real Redis and real processes:** run the phase's "Done when" demo (for example `kill -9` a worker or the leader) and report measured numbers. Mocks don't count as verification.

## Commands

```bash
make build                # go build -o bin/ ./cmd/...  (bin/worker, bin/scheduler)
make test                 # go test -race ./...
make vet                  # go vet ./...
gofmt -l .                # must print nothing
make run-scheduler        # go run ./cmd/scheduler
make run-worker           # go run ./cmd/worker
make redis-up             # Redis 7 in Docker; a local Homebrew `redis-server` works too

go test -race -count=1 -run TestName ./internal/queue/    # one test
REDIS_TEST_ADDR=localhost:6391 go test ./...              # tests against another Redis
```

- **Redis tests skip, they don't fail,** when Redis isn't reachable. Check for `SKIP` before calling a run green.
- **Test isolation:** tests use DB 15 with a random key prefix per test (`storetest.Client` / `storetest.Keys`), so packages run in parallel against one Redis and never touch DB 0.
- **Use the binaries for process demos:** use `./bin/worker` and `./bin/scheduler` for `kill -9` tests, not `go run`. Killing `go run` leaves the child process alive.

## Architecture

### Every job state change is one atomic Lua script

The scripts live in `internal/queue/scripts/*.lua`, are embedded with `go:embed`, and are called through `queue.Queue`.

- **Shared helpers:** `lib.lua` is prepended to every queue script. It provides:
  - `now_ms()` — Redis `TIME`, so all nodes share one clock.
  - `lease_state()` — the fencing check, which also counts `stats.fenced`.
  - `out_of_attempts()` and `bury()`.
  - `create_job()`, shared by `enqueue.lua` and `cron_fire.lua`.
- **Header comment:** each script starts with one that documents `KEYS`, `ARGV` and the return codes. Keep that format.
- **Return codes:** scripts that act for a lease holder (`complete`, `fail`, `heartbeat`, `release`) return `1` for ok, `0` when the lease was lost, and `-1` when the job was cancelled. `leaseErr` maps these to `ErrLeaseLost` / `ErrCancelled`.

Correctness comes from these scripts, never from coordination between processes. A script can run twice, or from two processes at once, and stay correct: each one checks the job's `status` and fencing token before changing anything.

### Redis data model

Every key name comes from `store.Keys`, which adds an optional prefix. Production uses no prefix.

| Key                | Type       | Holds                                      |
| ------------------ | ---------- | ------------------------------------------ |
| `job:{id}`         | hash       | The job                                    |
| `jobs:scheduled`   | sorted set | Delayed jobs, scored by `run_at` in ms     |
| `jobs:ready`       | sorted set | Due jobs, scored by `run_at` in ms         |
| `jobs:leases`      | sorted set | Running jobs, scored by lease expiry in ms |
| `jobs:dlq`         | sorted set | Dead jobs, scored by time of death in ms   |
| `lease:token`      | counter    | Fencing tokens                             |
| `idem:{key}`       | string     | Maps an idempotency key to its job         |
| `stats`            | hash       | Counters                                   |
| `scheduler:leader` | string     | The leading scheduler's ID                 |
| `scheduler:epoch`  | counter    | Leadership term number                     |
| `cron:{name}`      | hash       | A cron: schedule, job template, `next_run` |
| `crons`            | sorted set | Cron names, scored by next slot in ms      |

Redis stores durations as whole milliseconds (`*_ms` fields), so Go code that reports a duration Redis will use truncates it to milliseconds.

### Who runs which script

- **Clients** (the Phase 8 API/CLI, tests for now): `enqueue`, `cancel`, `replay`, and `cron_put`, `cron_enable`, `cron_delete`.
- **Workers** (`internal/worker`, `cmd/worker`):
  - `claim` and `heartbeat`.
  - Then one of `complete`, `fail` or `release`.
- **The elected scheduler leader only** (`internal/scheduler`, `cmd/scheduler`): `promote` (scheduled → ready), `reap` (expired lease → ready, or the dead-letter queue when out of attempts), and `cron_due` + `cron_fire`. A worker alone never promotes, reaps or fires crons, so delayed jobs, crash recovery and crons need a scheduler running.

### Lifecycle and retries

- **Path:** `scheduled → ready → running → succeeded`.
- **On failure:** `fail.lua` computes no waits itself. The Go code computes the backoff (`queue/backoff.go`: doubling, random within the upper half of the wait) and passes it in.
- **Permanent failures:** a handler returns `job.Permanent(err)` to skip retries.
- **Reaper:** requeues immediately, with no backoff and with the original `run_at` as the score. That keeps worker-failure recovery under 2 s. It still enforces `max_attempts`.
- **Release:** a worker that is shut down mid-job calls `release.lua`. It puts the job back in `ready` and gives back the attempt.

### Crons (`internal/queue/cron.go`, `schedule.go`)

- **One job per slot:** `cron_fire.lua` compares the cron's `next_run` (plus `enabled` and `schedule`) with what the caller read. Only if they match does it move `next_run` on and create the job, with idempotency key `cron:{name}:{slot ms}`. There is no separate `cron:fired:` key. `Enqueue` rejects client keys starting with `cron:`.
- **Next slot:** Go computes it with robfig's standard parser, always from Redis time passed in as UTC. `CRON_TZ=` picks another zone. A cron that fell behind fires once for its oldest missed slot, then skips to its first slot after now (`nextFire`).
- **Disabled:** `next_run` is `0` and the score in `crons` is `+inf`. Saving or re-enabling a cron restarts its schedule from now.

### Worker pool (`internal/worker/worker.go`)

- **Slots:** each slot is a goroutine that runs claim → `process`.
- **Two contexts:**
  - Claiming stops on the `Run` context.
  - Jobs run on `jobsCtx`, which outlives `Run`'s context so `ShutdownGrace` can let them finish.
  - Results are recorded on `context.WithoutCancel`.
- **Heartbeat:** a goroutine per job renews the lease. It cancels the job's context, with the _cause_, on `ErrLeaseLost` / `ErrCancelled`, or after a full `LeaseTTL` without a successful renewal.
- **Outcome order:** the `switch` in `process` is order-sensitive: lease lost → cancelled → success → shutdown (release) → timeout → failure.

### Leader election (`internal/election`)

- **Scripts:** its own Lua scripts (`acquire`, `renew`, `release`) with no `lib.lua`.
- **API:** `Elector.Run(ctx, lead)` calls `lead(termCtx, epoch)` for each term won and cancels `termCtx` when the term ends. Leader-only work goes inside `lead` (today `scheduler.Lead`).
- **Renewal:** a failed renewal ends the term at once.
- **Correctness:** the election only prevents duplicate work. Correctness still rests on the atomic scripts, so a brief overlap of two leaders is harmless.

### Handlers (`internal/handlers`)

`echo`, `sleep` and `flaky` are demo types.

`http` is the "call your service" type. It maps replies to outcomes:

| Reply                                   | Outcome           |
| --------------------------------------- | ----------------- |
| 2xx                                     | Success           |
| 5xx, 408, 429, network error or timeout | Retry             |
| Anything else                           | Permanent failure |

- Its 30 s limit applies only when the job has no `Timeout` of its own.
- Errors name only the URL's scheme and host, so secrets in webhook URLs stay out of logs.

### Config

- **Source:** every binary reads `internal/config` from environment variables. An empty variable counts as unset.
- **Rules:** `Load` validates everything and reports all errors together. It requires `HEARTBEAT_EVERY ≤ LEASE_TTL/2` and `LEADER_RENEW_EVERY ≤ LEADER_TTL/2`.
- **Lease timeouts:** `store.Connect` sets `ContextTimeoutEnabled` so per-call context deadlines work, which lease timing depends on.
