-- 看板及公共查询共用此只读脚本，既不恢复处理权，也不清理消息或索引。
local expected = {'zset', 'zset', 'hash', 'zset', 'hash', 'zset'}
expected[8], expected[9] = 'hash', 'string'
for _, i in ipairs({1, 2, 3, 4, 5, 6, 8, 9}) do
    local kind = redis.call('TYPE', KEYS[i]).ok
    if kind ~= 'none' and kind ~= expected[i] then return redis.error_reply('MQ_KEY_TYPE_CONFLICT') end
end
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local function payload(message)
    local body = message.payload or redis.call('HGET', KEYS[8], message.id)
    if not body then error('MQ_PAYLOAD_MISSING') end
    return body
end
if ARGV[1] == 'stats' then
    return {redis.call('ZCOUNT', KEYS[1], '-inf', now), redis.call('ZCOUNT', KEYS[1], '(' .. now, '+inf'),
            redis.call('ZCARD', KEYS[2]), redis.call('ZCOUNT', KEYS[2], '-inf', now),
            redis.call('ZCARD', KEYS[4]), redis.call('HLEN', KEYS[3]),
            tonumber(redis.call('GET', KEYS[9]) or '0'), redis.call('HLEN', KEYS[5])}
elseif ARGV[1] == 'get' then
    local raw = redis.call('HGET', KEYS[3], ARGV[2])
    if not raw then return {} end
    local message = cjson.decode(raw)
    return {raw, payload(message)}
elseif ARGV[1] == 'list' then
    local index = tonumber(ARGV[2])
    local start = tonumber(ARGV[3])
    local ids = redis.call('ZRANGE', KEYS[index], start, start + tonumber(ARGV[4]) - 1)
    local messages = {}
    local responseBytes = 0
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
                table.insert(messages, message)
            else
                local size = message.payloadBytes or string.len(message.payload or '')
                if responseBytes > 0 and responseBytes + size > 65536 then break end
                local body = payload(message)
                responseBytes = responseBytes + size
                table.insert(messages, raw)
                table.insert(messages, body)
            end
        end
    end
    if ARGV[5] == 'summary' then return cjson.encode(messages) end
    return messages
end
return redis.error_reply('MQ_UNKNOWN_READ_OPERATION')
