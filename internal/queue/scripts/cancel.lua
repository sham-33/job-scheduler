-- Cancels a job that hasn't finished. A job that is running right now is not
-- interrupted here: its worker finds out at its next heartbeat and stops.
--
-- KEYS[1] job hash   KEYS[2] scheduled zset   KEYS[3] ready zset
-- KEYS[4] leases zset   KEYS[5] stats hash
-- ARGV[1] job id
--
-- Returns 1 if the job was cancelled, 0 if it had already finished, or -1 if
-- there is no such job.

local status = redis.call('HGET', KEYS[1], 'status')
if not status then
  return -1
end
if status ~= 'scheduled' and status ~= 'ready' and status ~= 'running' then
  return 0
end

local now = now_ms()
redis.call('HSET', KEYS[1], 'status', 'cancelled', 'finished_at', now, 'updated_at', now)
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('ZREM', KEYS[4], ARGV[1])
redis.call('HINCRBY', KEYS[5], 'cancelled', 1)
return 1
