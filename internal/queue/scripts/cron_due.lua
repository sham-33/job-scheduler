-- Lists crons whose next fire time has come, the longest-waiting first.
-- Disabled crons score +inf, so they never come due.
--
-- KEYS[1] crons zset
-- ARGV[1] cron key prefix   ARGV[2] most crons to list
--
-- Returns {now (ms), name, schedule, next_run, name, schedule, next_run, …}.

local now = now_ms()
local names = redis.call('ZRANGE', KEYS[1], '-inf', now, 'BYSCORE',
  'LIMIT', 0, tonumber(ARGV[2]))

local out = {now}
for _, name in ipairs(names) do
  local schedule, next_run = unpack(redis.call('HMGET', ARGV[1] .. name, 'schedule', 'next_run'))
  if schedule then
    out[#out + 1] = name
    out[#out + 1] = schedule
    out[#out + 1] = next_run
  end
end
return out
