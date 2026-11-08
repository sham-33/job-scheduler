-- Takes jobs back from workers that stopped renewing their leases, and makes
-- them claimable again. Safe to run from several processes at once: each
-- expired lease is removed in the same step that hands the job back, so only
-- one caller can ever act on it.
--
-- KEYS[1] leases zset   KEYS[2] ready zset   KEYS[3] stats hash
-- ARGV[1] job key prefix   ARGV[2] max jobs to reclaim
--
-- Returns the number of jobs reclaimed.

local now = now_ms()
local expired = redis.call('ZRANGE', KEYS[1], '-inf', now, 'BYSCORE',
  'LIMIT', 0, tonumber(ARGV[2]))

local reclaimed = 0
for _, id in ipairs(expired) do
  local key = ARGV[1] .. id
  local status, run_at = unpack(redis.call('HMGET', key, 'status', 'run_at'))
  redis.call('ZREM', KEYS[1], id)
  if status == 'running' then
    redis.call('HSET', key, 'status', 'ready', 'last_error', 'lease expired', 'updated_at', now)
    -- Scoring by the original run_at puts the job ahead of newer work, which
    -- is what keeps recovery fast when the queue is busy.
    redis.call('ZADD', KEYS[2], run_at, id)
    redis.call('HINCRBY', KEYS[3], 'reclaimed', 1)
    reclaimed = reclaimed + 1
  end
end
return reclaimed
