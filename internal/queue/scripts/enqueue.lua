-- Creates a job, or returns the existing one when the idempotency key was
-- already used. Doing both in one script means a crash can never leave an
-- idempotency key pointing at a job that doesn't exist.
--
-- KEYS[1] job hash          KEYS[2] ready zset   KEYS[3] scheduled zset
-- KEYS[4] idempotency key   KEYS[5] stats hash   KEYS[6] events stream
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
create_job(KEYS[1], KEYS[2], KEYS[3], KEYS[5], KEYS[6], id, now + tonumber(ARGV[4]), now, {
  type = ARGV[2], payload = ARGV[3], idem_key = idem,
  max_attempts = ARGV[7], timeout_ms = ARGV[8],
  backoff_base_ms = ARGV[9], backoff_max_ms = ARGV[10],
})

if idem ~= '' then
  redis.call('SET', KEYS[4], id, 'EX', ARGV[6])
end
return {id, 1}
