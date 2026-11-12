-- Records a failed attempt, but only for the worker holding the current
-- fencing token. The job waits in the scheduled set for its retry, or goes
-- to the dead-letter queue if it is out of attempts or the failure is
-- permanent.
--
-- KEYS[1] job hash   KEYS[2] leases zset   KEYS[3] scheduled zset
-- KEYS[4] dead-letter zset   KEYS[5] stats hash
-- ARGV[1] job id   ARGV[2] fencing token   ARGV[3] error message
-- ARGV[4] wait before the retry (ms)   ARGV[5] '1' if retrying can't help
--
-- Returns 1 if the job will be retried, 2 if it is now dead, 0 if the caller
-- no longer holds the lease, or -1 if the job was cancelled.

local state = lease_state(KEYS[1], ARGV[2], KEYS[5])
if state == 'cancelled' then
  return -1
elseif state ~= 'ok' then
  return 0
end

local now = now_ms()
redis.call('ZREM', KEYS[2], ARGV[1])

if ARGV[5] == '1' or out_of_attempts(KEYS[1]) then
  bury(KEYS[1], KEYS[4], KEYS[5], ARGV[1], ARGV[3], now)
  return 2
end

local run_at = now + tonumber(ARGV[4])
redis.call('HSET', KEYS[1], 'status', 'scheduled', 'run_at', run_at,
  'last_error', ARGV[3], 'updated_at', now)
redis.call('ZADD', KEYS[3], run_at, ARGV[1])
redis.call('HINCRBY', KEYS[5], 'retried', 1)
return 1
