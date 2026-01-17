-- Redis token bucket Lua script
-- KEYS[1] = key for bucket state
-- ARGV[1] = rate (tokens per second)
-- ARGV[2] = capacity
-- ARGV[3] = now (milliseconds)
-- ARGV[4] = tokens_needed (usually 1)

local key = KEYS[1]
local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local need = tonumber(ARGV[4])

local data = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then
  tokens = capacity
  ts = now
end

local delta_ms = math.max(0, now - ts)
local add = (delta_ms / 1000.0) * rate
tokens = math.min(capacity, tokens + add)
if tokens < need then
  -- not enough tokens
  redis.call('HMSET', key, 'tokens', tokens, 'ts', now)
  return 0
else
  tokens = tokens - need
  redis.call('HMSET', key, 'tokens', tokens, 'ts', now)
  return 1
end
