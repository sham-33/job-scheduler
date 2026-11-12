-- Extends a job's lease, but only for the worker holding the current fencing
-- token. A worker that was declared dead and later woke up gets 0 and learns
-- it must stop. This is also how a running worker finds out its job was
-- cancelled.
--
-- KEYS[1] job hash   KEYS[2] leases zset   KEYS[3] stats hash
-- ARGV[1] job id     ARGV[2] fencing token   ARGV[3] lease TTL (ms)
--
-- Returns 1 if the lease was extended, 0 if the caller no longer holds it,
-- or -1 if the job was cancelled.

local state = lease_state(KEYS[1], ARGV[2], KEYS[3])
if state == 'cancelled' then
  return -1
elseif state ~= 'ok' then
  return 0
end

redis.call('ZADD', KEYS[2], 'XX', now_ms() + tonumber(ARGV[3]), ARGV[1])
return 1
