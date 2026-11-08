-- KEYS[1] = ops stream, KEYS[2] = epoch counter (same hash slot: doc:{id}:...)
-- ARGV[1] = writer epoch, ARGV[2] = serverSeq, ARGV[3] = op JSON
local current = tonumber(redis.call('GET', KEYS[2]) or '0')
if tonumber(ARGV[1]) < current then
  return redis.error_reply('STALE_EPOCH')
end
redis.call('XADD', KEYS[1], ARGV[2] .. '-0', 'op', ARGV[3])
return tonumber(ARGV[2])
