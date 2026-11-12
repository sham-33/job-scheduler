-- Takes jobs back from workers that stopped renewing their leases. A job
-- goes straight back to the ready set, or to the dead-letter queue if it has
-- used up its attempts, so a job that keeps crashing its worker can't do so
-- forever. Safe to run from several processes at once: each expired lease is
-- removed in the same step that hands the job back, so only one caller can
-- ever act on it.
--
-- KEYS[1] leases zset   KEYS[2] ready zset   KEYS[3] dead-letter zset
-- KEYS[4] stats hash
-- ARGV[1] job key prefix   ARGV[2] max jobs to take back
--
-- Returns the number of jobs taken back.

local now = now_ms()
local expired = redis.call('ZRANGE', KEYS[1], '-inf', now, 'BYSCORE',
  'LIMIT', 0, tonumber(ARGV[2]))

local taken = 0
for _, id in ipairs(expired) do
  local key = ARGV[1] .. id
  local status, run_at = unpack(redis.call('HMGET', key, 'status', 'run_at'))
  redis.call('ZREM', KEYS[1], id)
  if status == 'running' then
    if out_of_attempts(key) then
      bury(key, KEYS[3], KEYS[4], id, 'lease expired', now)
    else
      redis.call('HSET', key, 'status', 'ready', 'last_error', 'lease expired', 'updated_at', now)
      -- No backoff, and scoring by the original run_at puts the job ahead of
      -- newer work. Both keep recovery from a dead worker fast.
      redis.call('ZADD', KEYS[2], run_at, id)
      redis.call('HINCRBY', KEYS[4], 'reclaimed', 1)
    end
    taken = taken + 1
  end
end
return taken
