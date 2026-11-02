# Distributed Job Scheduler

A fault-tolerant job scheduler in Go and Redis, with leader election, distributed leases, idempotent claims, retries and a dead-letter queue. [PLAN.md](PLAN.md) has the design and the build phases.

**Status:** Phase 0 (project setup) is done.

## Prerequisites

- Go 1.27+
- Redis 7, either in Docker (`make redis-up`) or a local `redis-server`

## Quick start

```bash
make redis-up     # Redis in Docker, AOF on
make run-worker   # logs "connected"; Ctrl-C to stop
make test         # Redis tests skip if Redis isn't running
```

Run `make help` for every target.

## Configuration

Every binary reads the same environment variables. An empty variable counts as unset.

| Variable | Default | Meaning |
|---|---|---|
| `NODE_ID` | `<hostname>-<pid>` | Identifies this process in leases, logs and the worker registry |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error` |
| `REDIS_ADDR` | `localhost:6379` | Redis host and port |
| `REDIS_PASSWORD` | (none) | Redis password |
| `REDIS_DB` | `0` | Redis database number. Tests use DB 15 |
| `HTTP_ADDR` | `:8080` | API and dashboard listen address |
| `CONCURRENCY` | `25` | Jobs one worker process runs at once |
| `LEASE_TTL` | `1500ms` | How long a claimed job stays owned without a heartbeat |
| `HEARTBEAT_EVERY` | `500ms` | How often a worker extends its leases. Must be at most half of `LEASE_TTL` |
| `REAP_EVERY` | `200ms` | How often the scheduler reclaims expired leases |

Tests read `REDIS_TEST_ADDR` (default `localhost:6379`).
