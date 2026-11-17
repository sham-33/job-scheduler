-- Deletes a cron. Jobs it already created are left alone.
--
-- KEYS[1] cron hash   KEYS[2] crons zset
-- ARGV[1] name
--
-- Returns 1 when the cron was deleted, or 0 when there was no such cron.

local deleted = redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
return deleted
