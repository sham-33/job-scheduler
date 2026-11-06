-- Shared helpers, prepended to every script in this folder.

-- Current time in Unix milliseconds from Redis's own clock, so every worker
-- and scheduler agrees on "now" no matter how far their clocks drift.
local function now_ms()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end
