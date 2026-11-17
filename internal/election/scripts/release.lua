-- Gives up leadership so another process can take over at once, but only if
-- the caller still leads: a plain DEL could remove a newer leader's key.
--
-- KEYS[1] leader key
-- ARGV[1] candidate id
--
-- Returns 1 if released, or 0 if the caller didn't lead.

if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
