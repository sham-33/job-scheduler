-- Creates a job, or returns the existing one when the idempotency key was
-- already used. Doing both in one script means a crash can never leave an
-- idempotency key pointing at a job that doesn't exist.
--
-- KEYS[1] job hash          KEYS[2] ready zset   KEYS[3] scheduled zset
-- KEYS[4] idempotency key   KEYS[5] stats hash
-- ARGV[1] job id   ARGV[2] type   ARGV[3] payload   ARGV[4] delay (ms)
-- ARGV[5] idempotency key, or '' for none   ARGV[6] idempotency TTL (s)
-- ARGV[7] max attempts   ARGV[8] timeout per attempt (ms), 0 for none
-- ARGV[9] first retry wait (ms)   ARGV[10] longest retry wait (ms)
--
-- Returns {job id, 1} when created, or {existing job id, 0} for a duplicate.

local id, idem = ARGV[1], ARGV[5]

if idem ~= '' then
  local existing = redis.call('GET', KEYS[4])
  if existing then
    return {existing, 0}
  end
end

local now = now_ms()
local run_at = now + tonumber(ARGV[4])
local status = 'ready'
if run_at > now then
  status = 'scheduled'
end

redis.call('HSET', KEYS[1],
  'id', id, 'type', ARGV[2], 'payload', ARGV[3], 'status', status,
  'attempts', 0, 'idem_key', idem, 'run_at', run_at,
  'max_attempts', ARGV[7], 'timeout_ms', ARGV[8],
  'backoff_base_ms', ARGV[9], 'backoff_max_ms', ARGV[10],
  'created_at', now, 'updated_at', now)

if status == 'ready' then
  redis.call('ZADD', KEYS[2], run_at, id)
else
  redis.call('ZADD', KEYS[3], run_at, id)
end

if idem ~= '' then
  redis.call('SET', KEYS[4], id, 'EX', ARGV[6])
end
redis.call('HINCRBY', KEYS[5], 'submitted', 1)
return {id, 1}
