-- NOTE: This script is intentionally permanent, not a version-gated fallback. It replaces the value when the
-- key is absent (currentVal == false) as well as when it matches @expected. Native Redis 8.4+ SET ... IFEQ
-- has no "or absent" allowance - it only matches true value equality - so switching to it would change
-- replace-or-create semantics that lock-renewal-style callers may depend on.
local currentVal = redis.call('get', @key)
if (currentVal == false or currentVal == @expected) then
  if (@expires ~= nil and @expires ~= '') then
    return redis.call('set', @key, @value, 'PX', @expires) and 1 or 0
  else
    -- No expiration specified - plain SET removes any existing TTL
    return redis.call('set', @key, @value) and 1 or 0
  end
else
  return -1
end
