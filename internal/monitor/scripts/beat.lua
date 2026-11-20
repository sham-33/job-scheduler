-- Records that a worker process is alive, with what it is doing, and forgets
-- workers that have not checked in for a long time.
--
-- KEYS[1] workers zset   KEYS[2] this worker's hash
-- ARGV[1] worker id   ARGV[2] slots   ARGV[3] jobs running
-- ARGV[4] '1' if shutting down   ARGV[5] '1' on the first beat since start
-- ARGV[6] how long to remember a silent worker (ms)
--
-- Returns the time of the beat in ms, from Redis's clock.

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local forget_after = tonumber(ARGV[6])

-- A worker that restarts under the same ID gets a new start time. Every
-- other field is written on every beat.
if ARGV[5] == '1' then
  redis.call('HSET', KEYS[2], 'started_at', now)
end
redis.call('HSET', KEYS[2], 'concurrency', ARGV[2], 'busy', ARGV[3],
  'draining', ARGV[4], 'last_seen', now)
redis.call('PEXPIRE', KEYS[2], forget_after)
redis.call('ZADD', KEYS[1], now, ARGV[1])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', '(' .. (now - forget_after))
return now
