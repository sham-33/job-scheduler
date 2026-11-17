-- Shared helpers, prepended to every script in this folder.

-- Current time in Unix milliseconds from Redis's own clock, so every worker
-- and scheduler agrees on "now" no matter how far their clocks drift.
local function now_ms()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

-- Says whether the caller still owns a job: 'ok' if the job is running under
-- the caller's fencing token, 'cancelled' if someone cancelled it, and 'lost'
-- otherwise (the lease was taken over, or the job already finished). A 'lost'
-- call is counted in stats.fenced.
local function lease_state(job_key, token, stats_key)
  local status, current = unpack(redis.call('HMGET', job_key, 'status', 'lease_token'))
  if status == 'running' and current == token then
    return 'ok'
  end
  if status == 'cancelled' then
    return 'cancelled'
  end
  redis.call('HINCRBY', stats_key, 'fenced', 1)
  return 'lost'
end

-- Reports whether a job has used up its attempts. A job with no max_attempts
-- field has no limit.
local function out_of_attempts(job_key)
  local attempts, max = unpack(redis.call('HMGET', job_key, 'attempts', 'max_attempts'))
  max = tonumber(max) or 0
  return max > 0 and tonumber(attempts) >= max
end

-- The events stream keeps about this many of the most recent entries.
-- 'MAXLEN ~' lets Redis trim in whole blocks, which is far cheaper than an
-- exact cap, so the stream can hold slightly more for a moment.
local EVENTS_MAX = 10000

-- Records that a job moved from one status to another: counts it in the
-- stats hash under the event's name and appends it to the events stream.
-- Doing this in the same script as the change itself means the counters and
-- the stream can never show a change that didn't happen, or miss one that
-- did. worker is the lease owner involved, or '' for none.
local function record(stats_key, events_key, event, id, job_type, from, to, worker, now)
  redis.call('HINCRBY', stats_key, event, 1)
  redis.call('XADD', events_key, 'MAXLEN', '~', EVENTS_MAX, '*',
    'event', event, 'job_id', id, 'type', job_type or '', 'from', from,
    'to', to, 'worker', worker or '', 'ts', now)
end

-- Moves a running job to the dead-letter queue. worker is the lease owner it
-- was taken from.
local function bury(job_key, dlq_key, stats_key, events_key, id, job_type, worker, err, now)
  redis.call('HSET', job_key, 'status', 'dead', 'last_error', err,
    'finished_at', now, 'updated_at', now)
  redis.call('ZADD', dlq_key, now, id)
  record(stats_key, events_key, 'dead', id, job_type, 'running', 'dead', worker, now)
end

-- Creates a job and queues it: in the ready set if run_at has passed, or in
-- the scheduled set until it does. f holds the job's settings: type,
-- payload, idem_key, max_attempts, timeout_ms, backoff_base_ms and
-- backoff_max_ms.
local function create_job(job_key, ready_key, scheduled_key, stats_key, events_key, id, run_at, now, f)
  local status, queue_key = 'ready', ready_key
  if run_at > now then
    status, queue_key = 'scheduled', scheduled_key
  end
  redis.call('HSET', job_key,
    'id', id, 'type', f.type, 'payload', f.payload, 'status', status,
    'attempts', 0, 'idem_key', f.idem_key, 'run_at', run_at,
    'max_attempts', f.max_attempts, 'timeout_ms', f.timeout_ms,
    'backoff_base_ms', f.backoff_base_ms, 'backoff_max_ms', f.backoff_max_ms,
    'created_at', now, 'updated_at', now)
  redis.call('ZADD', queue_key, run_at, id)
  record(stats_key, events_key, 'submitted', id, f.type, '', status, '', now)
end
