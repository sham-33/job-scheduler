-- Marks a running job as succeeded, but only for the worker holding the
-- current fencing token. A worker whose lease was taken over changes nothing.
--
-- KEYS[1] job hash   KEYS[2] leases zset   KEYS[3] stats hash
-- ARGV[1] job id     ARGV[2] fencing token from the claim
--
-- Returns 1 on success, 0 if the caller no longer holds the lease, or -1 if
-- the job was cancelled.

local state = lease_state(KEYS[1], ARGV[2], KEYS[3])
if state == 'cancelled' then
  return -1
elseif state ~= 'ok' then
  return 0
end

local now = now_ms()
redis.call('HSET', KEYS[1], 'status', 'succeeded', 'finished_at', now, 'updated_at', now)
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('HINCRBY', KEYS[3], 'succeeded', 1)
return 1
