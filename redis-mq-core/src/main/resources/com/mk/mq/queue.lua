-- 所有键由调用方显式传入，并使用同一队列的集群哈希标签。
local expected = {'zset', 'zset', 'hash', 'zset', 'hash', 'zset'}
expected[8], expected[9] = 'hash', 'string'
for _, i in ipairs({1, 2, 3, 4, 5, 6, 8, 9}) do
    local kind = redis.call('TYPE', KEYS[i]).ok
    if kind ~= 'none' and kind ~= expected[i] then
        return redis.error_reply('MQ_KEY_TYPE_CONFLICT')
    end
end
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local operation = ARGV[1]
local retention = tonumber(ARGV[2])
local rawBytes = redis.call('GET', KEYS[9])
local accountedBytes = tonumber(rawBytes or '0')
if not accountedBytes or accountedBytes < 0 or accountedBytes % 1 ~= 0
        or (not rawBytes and redis.call('HLEN', KEYS[8]) > 0) then
    return redis.error_reply('MQ_STORAGE_CORRUPT')
end

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
local function payload(message)
    local body = message.payload or redis.call('HGET', KEYS[8], message.id)
    if not body then error('MQ_PAYLOAD_MISSING') end
    return body
end
local function externalize(message)
    if message.payload then
        local body = message.payload
        message.payloadBytes = string.len(body)
        message.version = 2
        redis.call('HSET', KEYS[8], message.id, body)
        redis.call('INCRBY', KEYS[9], message.payloadBytes)
        message.payload = nil
        save(message)
    end
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
    return nil
end
local function cleanDedup()
    local expired = redis.call('ZRANGEBYSCORE', KEYS[6], '-inf', now, 'LIMIT', 0, 32)
    if #expired > 0 then
        redis.call('HDEL', KEYS[5], unpack(expired))
        redis.call('ZREM', KEYS[6], unpack(expired))
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
    if message.payloadBytes and message.payloadBytes > accountedBytes then error('MQ_STORAGE_CORRUPT') end
    redis.call('ZREM', KEYS[1], message.id)
    redis.call('ZREM', KEYS[2], message.id)
    redis.call('ZREM', KEYS[4], message.id)
    redis.call('HDEL', KEYS[3], message.id)
    if message.payloadBytes then
        redis.call('HDEL', KEYS[8], message.id)
        redis.call('DECRBY', KEYS[9], message.payloadBytes)
    end
    remember('id:' .. message.id, message.id, message.fingerprint, now + retention)
end

if operation == 'send' then
    -- 发送检查必须保持只读，首个写操作必须能触发 Redis OOM 检查。
    -- 先 HDEL / ZREM 会令兼容模式 Lua 进入已写入状态，后续 HSET 可能绕过 maxmemory。
    local expired = redis.call('ZRANGEBYSCORE', KEYS[6], '-inf', now, 'LIMIT', 0, 32)
    local effectiveDedup = redis.call('HLEN', KEYS[5]) - #expired
    local id, payload, fingerprint = ARGV[3], ARGV[4], ARGV[5]
    local existing = load(id)
    local previous = nil
    if existing then previous = {id = existing.id, fingerprint = existing.fingerprint}
    else previous = lookup('id:' .. id) end
    local requestKnown = previous ~= nil
    if not previous and ARGV[10] ~= '' then previous = lookup('key:' .. ARGV[10]) end
    if previous then
        if previous.fingerprint ~= fingerprint then return redis.error_reply('MQ_DUPLICATE_PAYLOAD_CONFLICT') end
        -- 业务键命中时也记住本次请求编号，窗口过后重试仍返回原消息编号。
        if previous.id ~= id and not requestKnown then
            if effectiveDedup >= tonumber(ARGV[15]) then return redis.error_reply('MQ_DEDUP_FULL') end
            remember('id:' .. id, previous.id, fingerprint, now + retention)
            cleanDedup()
        end
        return cjson.encode({id = previous.id, duplicate = true})
    end
    if string.len(payload) > tonumber(ARGV[13]) then return redis.error_reply('MQ_MESSAGE_TOO_LARGE') end
    if redis.call('HLEN', KEYS[3]) >= tonumber(ARGV[12]) then return redis.error_reply('MQ_QUEUE_FULL') end
    -- 旧数据仍可读取、消费；完成有界迁移前不允许发送，以免漏算存量正文。
    if redis.call('HLEN', KEYS[3]) ~= redis.call('HLEN', KEYS[8]) then
        return redis.error_reply('MQ_STORAGE_UPGRADE_REQUIRED')
    end
    local bytes = accountedBytes
    if bytes + string.len(payload) > tonumber(ARGV[14]) then return redis.error_reply('MQ_QUEUE_BYTES_FULL') end
    local needed = 1
    if ARGV[10] ~= '' then needed = needed + 1 end
    if effectiveDedup + needed > tonumber(ARGV[15]) then return redis.error_reply('MQ_DEDUP_FULL') end
    local message = {version = 2, id = id, payloadBytes = string.len(payload), fingerprint = fingerprint,
                     state = 'SCHEDULED', deliveryCount = 0, maxDeliveries = tonumber(ARGV[7]),
                     retryDelay = tonumber(ARGV[8]), maxRetryDelay = tonumber(ARGV[9]),
                     createdAt = now, availableAt = now + tonumber(ARGV[6]), leaseUntil = 0,
                     receipt = '', lastError = ''}
    redis.call('HSET', KEYS[8], id, payload)
    redis.call('INCRBY', KEYS[9], message.payloadBytes)
    save(message)
    redis.call('ZADD', KEYS[1], message.availableAt, id)
    -- 正文存续期间为完成后的编号防重预留位置，不能在消息完成前清走。
    remember('id:' .. id, id, fingerprint, '+inf')
    if ARGV[10] ~= '' then remember('key:' .. ARGV[10], id, fingerprint, now + tonumber(ARGV[11])) end
    cleanDedup()
    signal(message.availableAt)
    return cjson.encode({id = id, duplicate = false})
elseif operation == 'receive' then
    cleanDedup()
    -- 恢复和领取均有批次上限，避免单次脚本长时间占用 Redis。
    local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, 8)
    for _, id in ipairs(expired) do
        local message = load(id)
        if message and message.state == 'PROCESSING' and message.leaseUntil <= now then retry(message, '处理权超时')
        elseif not message then redis.call('ZREM', KEYS[2], id) end
    end
    local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, math.min(16, tonumber(ARGV[3])))
    local messages = {0}
    local responseBytes = 0
    for _, id in ipairs(ids) do
        local message = load(id)
        if message and message.state == 'SCHEDULED' then
            local size = message.payloadBytes or string.len(message.payload or '')
            -- 有界响应；正文用 RESP 返回，避免 cjson 再次转义和复制大正文。
            if responseBytes > 0 and responseBytes + size > 65536 then break end
            local body = payload(message)
            responseBytes = responseBytes + size
            redis.call('ZREM', KEYS[1], id)
            message.state = 'PROCESSING'
            message.deliveryCount = message.deliveryCount + 1
            message.receipt = ARGV[5] .. ':' .. id
            message.leaseUntil = now + tonumber(ARGV[4])
            save(message)
            redis.call('ZADD', KEYS[2], message.leaseUntil, id)
            table.insert(messages, cjson.encode(message))
            table.insert(messages, body)
        elseif not message then redis.call('ZREM', KEYS[1], id) end
    end
    local nextAt = now + 60000
    for _, index in ipairs({1, 2}) do
        local head = redis.call('ZRANGE', KEYS[index], 0, 0, 'WITHSCORES')
        if head[2] then nextAt = math.min(nextAt, tonumber(head[2])) end
    end
    messages[1] = math.max(0, nextAt - now)
    return messages
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
elseif operation == 'migrate' then
    local start = tonumber(ARGV[4])
    local ids = redis.call('ZRANGE', KEYS[tonumber(ARGV[3])], start, start + tonumber(ARGV[5]) - 1)
    local checked, workBytes = 0, 0
    for _, id in ipairs(ids) do
        local size = redis.call('HSTRLEN', KEYS[3], id)
        if checked > 0 and workBytes + size > 65536 then break end
        checked = checked + 1
        workBytes = workBytes + size
        local message = load(id)
        if message then
            externalize(message)
            -- 旧实现的存续编号可能已过期，为后续完成保留防重位置。
            remember('id:' .. id, id, message.fingerprint, '+inf')
        end
    end
    return checked
end
return redis.error_reply('MQ_UNKNOWN_OPERATION')
