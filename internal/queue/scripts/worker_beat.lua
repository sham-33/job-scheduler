-- Records that a worker process is alive: sets its last-seen time in the
-- workers set and refreshes its hash. Each beat also forgets workers that
-- haven't been seen for a long time (ones that crashed, since a worker that
-- shuts down cleanly removes itself), so the registry can't grow forever.
--
-- KEYS[1] workers zset   KEYS[2] this worker's hash
-- ARGV[1] worker id   ARGV[2] concurrency   ARGV[3] jobs running now
-- ARGV[4] '1' on the process's first beat
-- ARGV[5] forget workers unseen for this long (ms)   ARGV[6] worker key prefix
--
-- Returns the number of workers forgotten.

local now = now_ms()
local gone = redis.call('ZRANGE', KEYS[1], '-inf', '(' .. (now - tonumber(ARGV[5])),
  'BYSCORE', 'LIMIT', 0, 100)
for _, id in ipairs(gone) do
  redis.call('ZREM', KEYS[1], id)
  redis.call('DEL', ARGV[6] .. id)
end

redis.call('ZADD', KEYS[1], now, ARGV[1])
redis.call('HSET', KEYS[2], 'concurrency', ARGV[2], 'running', ARGV[3])
-- A restarted process can reuse its old ID, so its first beat always resets
-- started_at. HSETNX covers a worker that was paused for so long it was
-- forgotten, and comes back with no hash.
if ARGV[4] == '1' then
  redis.call('HSET', KEYS[2], 'started_at', now)
else
  redis.call('HSETNX', KEYS[2], 'started_at', now)
end
return #gone
