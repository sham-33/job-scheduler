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

-- Moves a job to the dead-letter queue.
local function bury(job_key, dlq_key, stats_key, id, err, now)
  redis.call('HSET', job_key, 'status', 'dead', 'last_error', err,
    'finished_at', now, 'updated_at', now)
  redis.call('ZADD', dlq_key, now, id)
  redis.call('HINCRBY', stats_key, 'dead', 1)
end
