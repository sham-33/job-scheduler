-- Extends the caller's term, but only if the caller still leads.
--
-- KEYS[1] leader key
-- ARGV[1] candidate id   ARGV[2] how long the term lasts unless renewed (ms)
--
-- Returns 1 if renewed, or 0 if the caller no longer leads.

if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
return 0
