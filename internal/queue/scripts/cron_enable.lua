-- Turns a cron on or off. A disabled cron never fires. Turning it back on
-- restarts its schedule from ARGV[3], so slots that passed while it was off
-- never fire.
--
-- KEYS[1] cron hash   KEYS[2] crons zset
-- ARGV[1] name   ARGV[2] '1' to enable, '0' to disable
-- ARGV[3] next fire time (ms), when enabling
-- ARGV[4] the schedule ARGV[3] was computed from, when enabling
--
-- Returns 1 when done (or it already was so), 0 when enabling and the
-- schedule changed after the caller read it, or -1 when there is no such cron.

local schedule, enabled = unpack(redis.call('HMGET', KEYS[1], 'schedule', 'enabled'))
if not schedule then
  return -1
end
if enabled == ARGV[2] then
  return 1
end
if ARGV[2] == '1' and schedule ~= ARGV[4] then
  return 0
end

local now = now_ms()
if ARGV[2] == '1' then
  redis.call('HSET', KEYS[1], 'enabled', '1', 'next_run', ARGV[3], 'updated_at', now)
  redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1])
else
  redis.call('HSET', KEYS[1], 'enabled', '0', 'next_run', '0', 'updated_at', now)
  redis.call('ZADD', KEYS[2], '+inf', ARGV[1])
end
return 1
