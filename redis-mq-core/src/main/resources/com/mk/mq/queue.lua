-- 所有键由调用方显式传入，并使用同一队列的集群哈希标签。
local expected = {'zset', 'zset', 'hash', 'zset', 'hash', 'zset'}
for i = 1, 6 do
    local kind = redis.call('TYPE', KEYS[i]).ok
    if kind ~= 'none' and kind ~= expected[i] then
        return redis.error_reply('MQ_KEY_TYPE_CONFLICT')
    end
end
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local operation = ARGV[1]
local retention = tonumber(ARGV[2])

local function signal(at)
    -- 通知是优化；无发布权限时仍由消费者兜底检查，不影响已持久化消息。
    redis.pcall('PUBLISH', KEYS[7], tostring(at))
end
local function load(id)
    local raw = redis.call('HGET', KEYS[3], id)
    if raw then return cjson.decode(raw) end
    return nil
end
local function save(message)
    redis.call('HSET', KEYS[3], message.id, cjson.encode(message))
end
local function remember(key, id, fingerprint, untilAt)
    redis.call('HSET', KEYS[5], key, cjson.encode({id = id, fingerprint = fingerprint}))
    redis.call('ZADD', KEYS[6], untilAt, key)
end
local function lookup(key)
    local untilAt = redis.call('ZSCORE', KEYS[6], key)
    if untilAt and tonumber(untilAt) > now then
        local raw = redis.call('HGET', KEYS[5], key)
        if raw then return cjson.decode(raw) end
    end
    redis.call('HDEL', KEYS[5], key)
    redis.call('ZREM', KEYS[6], key)
    return nil
end
local function cleanDedup()
    local expired = redis.call('ZRANGEBYSCORE', KEYS[6], '-inf', now, 'LIMIT', 0, 100)
    for _, key in ipairs(expired) do
        redis.call('HDEL', KEYS[5], key)
        redis.call('ZREM', KEYS[6], key)
    end
end
local function dead(message, reason)
    redis.call('ZREM', KEYS[1], message.id)
    redis.call('ZREM', KEYS[2], message.id)
    message.state = 'DEAD'
    message.receipt = ''
    message.leaseUntil = 0
    message.lastError = reason
    save(message)
    redis.call('ZADD', KEYS[4], now, message.id)
end
local function retry(message, reason)
    if message.deliveryCount >= message.maxDeliveries then
        dead(message, reason)
    else
        local delay = math.min(message.maxRetryDelay, message.retryDelay * math.pow(2, math.min(message.deliveryCount - 1, 40)))
        redis.call('ZREM', KEYS[2], message.id)
        message.state = 'SCHEDULED'
        message.receipt = ''
        message.leaseUntil = 0
        message.availableAt = now + delay
        message.lastError = reason
        save(message)
        redis.call('ZADD', KEYS[1], message.availableAt, message.id)
        signal(message.availableAt)
    end
end
local function owned(message, receipt)
    return message and message.state == 'PROCESSING' and message.receipt == receipt and message.leaseUntil > now
end
local function finish(message)
    redis.call('ZREM', KEYS[1], message.id)
    redis.call('ZREM', KEYS[2], message.id)
    redis.call('ZREM', KEYS[4], message.id)
    redis.call('HDEL', KEYS[3], message.id)
    remember('id:' .. message.id, message.id, message.fingerprint, now + retention)
end

if operation == 'send' then
    cleanDedup()
    local id, payload, fingerprint = ARGV[3], ARGV[4], ARGV[5]
    local existing = load(id)
    local previous = nil
    if existing then previous = {id = existing.id, fingerprint = existing.fingerprint}
    else previous = lookup('id:' .. id) end
    if not previous and ARGV[10] ~= '' then previous = lookup('key:' .. ARGV[10]) end
    if previous then
        if previous.fingerprint ~= fingerprint then return redis.error_reply('MQ_DUPLICATE_PAYLOAD_CONFLICT') end
        -- 业务键命中时也记住本次请求编号，窗口过后重试仍返回原消息编号。
        if previous.id ~= id then remember('id:' .. id, previous.id, fingerprint, now + retention) end
        return cjson.encode({id = previous.id, duplicate = true})
    end
    if string.len(payload) > tonumber(ARGV[13]) then return redis.error_reply('MQ_MESSAGE_TOO_LARGE') end
    if redis.call('HLEN', KEYS[3]) >= tonumber(ARGV[12]) then return redis.error_reply('MQ_QUEUE_FULL') end
    local message = {version = 1, id = id, payload = payload, fingerprint = fingerprint,
                     state = 'SCHEDULED', deliveryCount = 0, maxDeliveries = tonumber(ARGV[7]),
                     retryDelay = tonumber(ARGV[8]), maxRetryDelay = tonumber(ARGV[9]),
                     createdAt = now, availableAt = now + tonumber(ARGV[6]), leaseUntil = 0,
                     receipt = '', lastError = ''}
    save(message)
    redis.call('ZADD', KEYS[1], message.availableAt, id)
    remember('id:' .. id, id, fingerprint, now + retention)
    if ARGV[10] ~= '' then remember('key:' .. ARGV[10], id, fingerprint, now + tonumber(ARGV[11])) end
    signal(message.availableAt)
    return cjson.encode({id = id, duplicate = false})
elseif operation == 'receive' then
    cleanDedup()
    -- 恢复和领取均有批次上限，避免单次脚本长时间占用 Redis。
    local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, 100)
    for _, id in ipairs(expired) do
        local message = load(id)
        if message and message.state == 'PROCESSING' and message.leaseUntil <= now then retry(message, '处理权超时')
        elseif not message then redis.call('ZREM', KEYS[2], id) end
    end
    local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, tonumber(ARGV[3]))
    local messages = {}
    for _, id in ipairs(ids) do
        local message = load(id)
        if message and message.state == 'SCHEDULED' then
            redis.call('ZREM', KEYS[1], id)
            message.state = 'PROCESSING'
            message.deliveryCount = message.deliveryCount + 1
            message.receipt = ARGV[5] .. ':' .. id
            message.leaseUntil = now + tonumber(ARGV[4])
            save(message)
            redis.call('ZADD', KEYS[2], message.leaseUntil, id)
            table.insert(messages, message)
        elseif not message then redis.call('ZREM', KEYS[1], id) end
    end
    local nextAt = now + 60000
    for _, index in ipairs({1, 2}) do
        local head = redis.call('ZRANGE', KEYS[index], 0, 0, 'WITHSCORES')
        if head[2] then nextAt = math.min(nextAt, tonumber(head[2])) end
    end
    return cjson.encode({messages = messages, waitMillis = math.max(0, nextAt - now)})
elseif operation == 'ack' or operation == 'retry' or operation == 'reject' or operation == 'renew' then
    local message = load(ARGV[3])
    if not owned(message, ARGV[4]) then return 0 end
    if operation == 'ack' then finish(message)
    elseif operation == 'retry' then retry(message, ARGV[5])
    elseif operation == 'reject' then dead(message, ARGV[5])
    else
        message.leaseUntil = now + tonumber(ARGV[5])
        save(message)
        redis.call('ZADD', KEYS[2], message.leaseUntil, message.id)
    end
    return 1
elseif operation == 'cancel' or operation == 'reschedule' or operation == 'replay' or operation == 'discard' then
    local message = load(ARGV[3])
    if not message then return 0 end
    if operation == 'cancel' or operation == 'reschedule' then
        if message.state ~= 'SCHEDULED' then return 0 end
        if operation == 'cancel' then finish(message)
        else
            message.availableAt = now + tonumber(ARGV[4])
            save(message)
            redis.call('ZADD', KEYS[1], message.availableAt, message.id)
            signal(message.availableAt)
        end
    else
        if message.state ~= 'DEAD' then return 0 end
        if operation == 'discard' then finish(message)
        else
            redis.call('ZREM', KEYS[4], message.id)
            message.state = 'SCHEDULED'
            message.deliveryCount = 0
            message.availableAt = now + tonumber(ARGV[4])
            message.lastError = ''
            save(message)
            redis.call('ZADD', KEYS[1], message.availableAt, message.id)
            signal(message.availableAt)
        end
    end
    return 1
elseif operation == 'get' then
    return redis.call('HGET', KEYS[3], ARGV[3]) or ''
elseif operation == 'deadList' then
    local ids = redis.call('ZRANGE', KEYS[4], tonumber(ARGV[3]), tonumber(ARGV[3]) + tonumber(ARGV[4]) - 1)
    local messages = {}
    for _, id in ipairs(ids) do
        local message = load(id)
        if message then table.insert(messages, message) end
    end
    return cjson.encode(messages)
elseif operation == 'stats' then
    return {redis.call('ZCOUNT', KEYS[1], '-inf', now), redis.call('ZCOUNT', KEYS[1], '(' .. now, '+inf'),
            redis.call('ZCARD', KEYS[2]), redis.call('ZCOUNT', KEYS[2], '-inf', now),
            redis.call('ZCARD', KEYS[4]), redis.call('HLEN', KEYS[3])}
end
return redis.error_reply('MQ_UNKNOWN_OPERATION')
