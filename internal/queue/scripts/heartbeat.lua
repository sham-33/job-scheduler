-- Extends a job's lease, but only for the worker holding the current fencing
-- token. A worker that was declared dead and later woke up gets 0 and learns
-- it must stop.
--
-- KEYS[1] job hash   KEYS[2] leases zset   KEYS[3] stats hash
-- ARGV[1] job id     ARGV[2] fencing token   ARGV[3] lease TTL (ms)
--
-- Returns 1 if the lease was extended, or 0 if the caller no longer holds it.

local status, token = unpack(redis.call('HMGET', KEYS[1], 'status', 'lease_token'))
if status ~= 'running' or token ~= ARGV[2] then
  redis.call('HINCRBY', KEYS[3], 'fenced', 1)
  return 0
end

redis.call('ZADD', KEYS[2], 'XX', now_ms() + tonumber(ARGV[3]), ARGV[1])
return 1
