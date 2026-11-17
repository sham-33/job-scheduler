-- Makes the caller leader if no one else is, and starts a new term.
--
-- KEYS[1] leader key   KEYS[2] epoch counter
-- ARGV[1] candidate id   ARGV[2] how long the term lasts unless renewed (ms)
--
-- Returns the new term's epoch, or 0 if another process leads.

local holder = redis.call('GET', KEYS[1])
if holder and holder ~= ARGV[1] then
  return 0
end
-- The seat is free, or it still holds the caller's id from a term the caller
-- already gave up (a renewal that failed on the caller's side but reached
-- Redis). Either way, nobody else is leading, so this is a new term.
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
return redis.call('INCR', KEYS[2])
