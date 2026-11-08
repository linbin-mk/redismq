-- 看板及公共查询共用此只读脚本，既不恢复处理权，也不清理消息或索引。
local expected = {'zset', 'zset', 'hash', 'zset', 'hash', 'zset'}
for i = 1, 6 do
    local kind = redis.call('TYPE', KEYS[i]).ok
    if kind ~= 'none' and kind ~= expected[i] then return redis.error_reply('MQ_KEY_TYPE_CONFLICT') end
end
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
if ARGV[1] == 'stats' then
    return {redis.call('ZCOUNT', KEYS[1], '-inf', now), redis.call('ZCOUNT', KEYS[1], '(' .. now, '+inf'),
            redis.call('ZCARD', KEYS[2]), redis.call('ZCOUNT', KEYS[2], '-inf', now),
            redis.call('ZCARD', KEYS[4]), redis.call('HLEN', KEYS[3])}
elseif ARGV[1] == 'get' then
    return redis.call('HGET', KEYS[3], ARGV[2]) or ''
elseif ARGV[1] == 'list' then
    local index = tonumber(ARGV[2])
    local start = tonumber(ARGV[3])
    local ids = redis.call('ZRANGE', KEYS[index], start, start + tonumber(ARGV[4]) - 1)
    local messages = {}
    for _, id in ipairs(ids) do
        local raw = redis.call('HGET', KEYS[3], id)
        if raw then
            local message = cjson.decode(raw)
            if ARGV[5] == 'summary' then
                -- 列表不返回业务正文、领取凭证及指纹。
                message.payload = nil
                message.receipt = nil
                message.fingerprint = nil
                message.ready = message.state == 'SCHEDULED' and message.availableAt <= now
            end
            table.insert(messages, message)
        end
    end
    return cjson.encode(messages)
end
return redis.error_reply('MQ_UNKNOWN_READ_OPERATION')
