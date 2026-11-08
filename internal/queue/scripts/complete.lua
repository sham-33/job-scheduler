-- Marks a running job as succeeded, but only for the worker holding the
-- current fencing token. A worker whose lease was taken over changes nothing.
--
-- KEYS[1] job hash   KEYS[2] leases zset   KEYS[3] stats hash
-- ARGV[1] job id     ARGV[2] fencing token from the claim
--
-- Returns 1 on success, or 0 if the caller no longer holds the lease.

local status, token = unpack(redis.call('HMGET', KEYS[1], 'status', 'lease_token'))
if status ~= 'running' or token ~= ARGV[2] then
  redis.call('HINCRBY', KEYS[3], 'fenced', 1)
  return 0
end

local now = now_ms()
redis.call('HSET', KEYS[1], 'status', 'succeeded', 'finished_at', now, 'updated_at', now)
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('HINCRBY', KEYS[3], 'succeeded', 1)
return 1
