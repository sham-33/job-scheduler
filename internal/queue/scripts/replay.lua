-- Gives a dead job a fresh start: back to the ready set with its attempts
-- reset, typically after whatever made it fail has been fixed.
--
-- KEYS[1] job hash   KEYS[2] dead-letter zset   KEYS[3] ready zset
-- KEYS[4] stats hash
-- ARGV[1] job id
--
-- Returns 1 if the job was replayed, 0 if it isn't dead, or -1 if there is
-- no such job.

local status = redis.call('HGET', KEYS[1], 'status')
if not status then
  return -1
end
if status ~= 'dead' then
  return 0
end

local now = now_ms()
redis.call('HSET', KEYS[1], 'status', 'ready', 'attempts', 0, 'run_at', now, 'updated_at', now)
redis.call('HDEL', KEYS[1], 'finished_at')
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZADD', KEYS[3], now, ARGV[1])
redis.call('HINCRBY', KEYS[4], 'replayed', 1)
return 1
