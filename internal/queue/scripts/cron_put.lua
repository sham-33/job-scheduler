-- Creates a cron, or replaces the definition of one with the same name. A
-- replaced cron keeps its enabled or disabled state, its creation time and
-- its record of the last fire, and its schedule restarts from ARGV[3].
--
-- KEYS[1] cron hash   KEYS[2] crons zset
-- ARGV[1] name   ARGV[2] schedule   ARGV[3] next fire time (ms)
-- ARGV[4] job type   ARGV[5] payload   ARGV[6] max attempts
-- ARGV[7] timeout per attempt (ms), 0 for none   ARGV[8] first retry wait (ms)
-- ARGV[9] longest retry wait (ms)
--
-- Returns 1 when the cron was created, or 0 when it replaced one.

local enabled = redis.call('HGET', KEYS[1], 'enabled')
local created = 0
if not enabled then
  created = 1
end

local next_run, score = ARGV[3], ARGV[3]
if enabled == '0' then
  next_run, score = '0', '+inf'
else
  enabled = '1'
end

local now = now_ms()
redis.call('HSET', KEYS[1],
  'name', ARGV[1], 'schedule', ARGV[2], 'enabled', enabled, 'next_run', next_run,
  'type', ARGV[4], 'payload', ARGV[5], 'max_attempts', ARGV[6],
  'timeout_ms', ARGV[7], 'backoff_base_ms', ARGV[8], 'backoff_max_ms', ARGV[9],
  'updated_at', now)
redis.call('HSETNX', KEYS[1], 'created_at', now)
redis.call('ZADD', KEYS[2], score, ARGV[1])
return created
