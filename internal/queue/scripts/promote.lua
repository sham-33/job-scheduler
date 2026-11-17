-- Moves jobs whose run_at has passed from the scheduled set to the ready set.
--
-- KEYS[1] scheduled zset   KEYS[2] ready zset   KEYS[3] stats hash
-- KEYS[4] events stream
-- ARGV[1] job key prefix   ARGV[2] max jobs to move
--
-- Returns the number of jobs moved.

local now = now_ms()
local due = redis.call('ZRANGE', KEYS[1], '-inf', now, 'BYSCORE',
  'LIMIT', 0, tonumber(ARGV[2]), 'WITHSCORES')

local moved = 0
for i = 1, #due, 2 do
  local id, run_at = due[i], due[i + 1]
  local key = ARGV[1] .. id
  redis.call('ZREM', KEYS[1], id)
  -- A job that already left 'scheduled' some other way is only dropped from
  -- the set, never resurrected.
  local status, job_type = unpack(redis.call('HMGET', key, 'status', 'type'))
  if status == 'scheduled' then
    redis.call('HSET', key, 'status', 'ready', 'updated_at', now)
    redis.call('ZADD', KEYS[2], run_at, id)
    record(KEYS[3], KEYS[4], 'promoted', id, job_type, 'scheduled', 'ready', '', now)
    moved = moved + 1
  end
end
return moved
