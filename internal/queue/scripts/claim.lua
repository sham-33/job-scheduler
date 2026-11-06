-- Hands the longest-waiting ready job to one worker and leases it to them.
-- Redis runs a script start to finish before any other command, so two
-- workers calling this at the same moment can never get the same job.
--
-- KEYS[1] ready zset   KEYS[2] leases zset   KEYS[3] lease token counter
-- KEYS[4] stats hash
-- ARGV[1] job key prefix   ARGV[2] worker id   ARGV[3] lease TTL (ms)
--
-- Returns the job hash as a flat field/value list, or false if none is ready.

-- Entries whose job is no longer 'ready' (cancelled, for example) are
-- dropped. The cap keeps a single call short even if many pile up.
for _ = 1, 100 do
  local popped = redis.call('ZPOPMIN', KEYS[1])
  if #popped == 0 then
    return false
  end

  local id = popped[1]
  local key = ARGV[1] .. id
  if redis.call('HGET', key, 'status') == 'ready' then
    local now = now_ms()
    local token = redis.call('INCR', KEYS[3])
    redis.call('HSET', key, 'status', 'running', 'lease_owner', ARGV[2],
      'lease_token', token, 'started_at', now, 'updated_at', now)
    redis.call('HINCRBY', key, 'attempts', 1)
    redis.call('ZADD', KEYS[2], now + tonumber(ARGV[3]), id)
    redis.call('HINCRBY', KEYS[4], 'claimed', 1)
    return redis.call('HGETALL', key)
  end
end
return false
