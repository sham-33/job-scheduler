-- Hands a running job straight back to the ready set, for a worker that is
-- shutting down and can't finish it. Being interrupted isn't the job's fault,
-- so the attempt it was on is given back. Only the worker holding the current
-- fencing token can release a job.
--
-- KEYS[1] job hash   KEYS[2] leases zset   KEYS[3] ready zset   KEYS[4] stats hash
-- ARGV[1] job id     ARGV[2] fencing token from the claim
--
-- Returns 1 if the job was released, 0 if the caller no longer holds the
-- lease, or -1 if the job was cancelled.

local state = lease_state(KEYS[1], ARGV[2], KEYS[4])
if state == 'cancelled' then
  return -1
elseif state ~= 'ok' then
  return 0
end

local now = now_ms()
redis.call('HSET', KEYS[1], 'status', 'ready', 'updated_at', now)
redis.call('HINCRBY', KEYS[1], 'attempts', -1)
redis.call('ZREM', KEYS[2], ARGV[1])
-- Like a reclaimed job, it keeps its original run_at, so it goes ahead of
-- newer work.
redis.call('ZADD', KEYS[3], redis.call('HGET', KEYS[1], 'run_at'), ARGV[1])
redis.call('HINCRBY', KEYS[4], 'released', 1)
return 1
