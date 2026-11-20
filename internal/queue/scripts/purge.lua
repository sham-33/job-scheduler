-- Deletes jobs from the dead-letter queue for good, oldest first, along with
-- their idempotency keys, so a client that submits with the same key again
-- gets a new job.
--
-- KEYS[1] dead-letter zset   KEYS[2] stats hash   KEYS[3] events stream
-- ARGV[1] job key prefix   ARGV[2] most jobs to look at
-- ARGV[3] idempotency key prefix
--
-- Returns {jobs deleted, entries looked at}. Fewer entries than ARGV[2]
-- means the queue is now empty.

local ids = redis.call('ZRANGE', KEYS[1], 0, tonumber(ARGV[2]) - 1)
local purged = 0
for _, id in ipairs(ids) do
  local key = ARGV[1] .. id
  redis.call('ZREM', KEYS[1], id)
  -- An entry whose job left the dead-letter queue some other way is only
  -- dropped from the set.
  if redis.call('HGET', key, 'status') == 'dead' then
    -- Emitted first, while the job's fields can still be read.
    emit(KEYS[3], key, id, 'purged', 'dead', '')
    local idem = redis.call('HGET', key, 'idem_key')
    if idem and idem ~= '' and redis.call('GET', ARGV[3] .. idem) == id then
      redis.call('DEL', ARGV[3] .. idem)
    end
    redis.call('DEL', key)
    purged = purged + 1
  end
end
if purged > 0 then
  redis.call('HINCRBY', KEYS[2], 'purged', purged)
end
return {purged, #ids}
