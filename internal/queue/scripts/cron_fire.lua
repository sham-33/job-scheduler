-- Fires a cron for one scheduled time (a "slot"): creates the slot's job and
-- moves the cron on to its next slot, in one atomic step. It fires only if
-- the cron is still as the caller read it: enabled, on the same schedule,
-- and waiting for this slot. So when two schedulers race to fire a slot, or
-- a paused leader wakes up and tries to fire one its successor already
-- fired, exactly one job is created.
--
-- KEYS[1] cron hash   KEYS[2] crons zset   KEYS[3] new job hash
-- KEYS[4] ready zset   KEYS[5] scheduled zset   KEYS[6] the slot's idempotency key
-- KEYS[7] stats hash
-- ARGV[1] name   ARGV[2] slot, as read from next_run (ms)
-- ARGV[3] schedule, as read   ARGV[4] the slot after this one (ms)
-- ARGV[5] new job id   ARGV[6] the slot's idempotency key
-- ARGV[7] idempotency TTL (s)
--
-- Returns {1, job id} when it created the slot's job, {2, job id} when the
-- slot already had a job (so it made none), or {0} when the cron isn't
-- waiting for this slot: someone else fired it, or the cron was changed,
-- disabled or deleted.

local now = now_ms()
local enabled, next_run, schedule, job_type, payload, max_attempts, timeout_ms,
  backoff_base_ms, backoff_max_ms = unpack(redis.call('HMGET', KEYS[1],
    'enabled', 'next_run', 'schedule', 'type', 'payload', 'max_attempts',
    'timeout_ms', 'backoff_base_ms', 'backoff_max_ms'))
local slot = ARGV[2]
if enabled ~= '1' or next_run ~= slot or schedule ~= ARGV[3] or tonumber(slot) > now then
  return {0}
end

-- Move on first, so whatever happens below, this slot can't fire again.
redis.call('HSET', KEYS[1], 'next_run', ARGV[4], 'last_fired', slot)
redis.call('ZADD', KEYS[2], ARGV[4], ARGV[1])

-- The key outlives the cron. If this slot already fired (the cron was
-- deleted and saved again, say), it still gets only one job.
local existing = redis.call('GET', KEYS[6])
if existing then
  redis.call('HSET', KEYS[1], 'last_job_id', existing)
  return {2, existing}
end

local id = ARGV[5]
-- run_at is the slot, so the job queues ahead of work that came due later,
-- and started_at - run_at shows how late it ran against its schedule.
create_job(KEYS[3], KEYS[4], KEYS[5], KEYS[7], id, tonumber(slot), now, {
  type = job_type, payload = payload, idem_key = ARGV[6], max_attempts = max_attempts,
  timeout_ms = timeout_ms, backoff_base_ms = backoff_base_ms, backoff_max_ms = backoff_max_ms,
})
redis.call('SET', KEYS[6], id, 'EX', ARGV[7])
redis.call('HSET', KEYS[1], 'last_job_id', id)
redis.call('HINCRBY', KEYS[7], 'cron_fired', 1)
return {1, id}
