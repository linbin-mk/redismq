package com.mk.mq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.RedisNoScriptException;

import java.io.IOException;
import java.io.OutputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** 仅依赖 Redis 的队列客户端，不创建或关闭宿主的 Redis 连接。 */
public final class RedisMq {
    private static final String SCRIPT = loadScript("queue.lua");
    private static final String READ_SCRIPT = loadScript("queue-read.lua");
    private static final String DIRECTORY_SCRIPT = """
            if redis.call('ZSCORE', KEYS[1], ARGV[1]) then return 1 end
            if redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[2]) then return redis.error_reply('MQ_DIRECTORY_FULL') end
            redis.call('ZADD', KEYS[1], 0, ARGV[1])
            return 1
            """;
    private static final String DIRECTORY_SHA = hash("SHA-1", DIRECTORY_SCRIPT);
    private static final String WRITE_SHA = hash("SHA-1", SCRIPT);
    private static final String READ_SHA = hash("SHA-1", READ_SCRIPT);
    private static final List<String> SUFFIXES = List.of("scheduled", "processing", "messages", "dead", "dedup", "dedup-expiry", "wakeup", "payloads", "payload-bytes");
    private final RedissonClient redis;
    private final String namespace;
    private final MqOptions options;
    private final ObjectMapper mapper;
    private final ConcurrentMap<String, Boolean> registered = new ConcurrentHashMap<>();

    public RedisMq(RedissonClient redis, String namespace) {
        this(redis, namespace, MqOptions.defaults(), new ObjectMapper().findAndRegisterModules());
    }

    public RedisMq(RedissonClient redis, String namespace, MqOptions options, ObjectMapper mapper) {
        this.redis = Objects.requireNonNull(redis, "Redis 客户端不能为空");
        this.namespace = Checks.name(namespace, "命名空间");
        this.options = Objects.requireNonNull(options, "队列选项不能为空");
        this.mapper = Objects.requireNonNull(mapper, "JSON 编码器不能为空").copy().deactivateDefaultTyping();
    }

    public SendResult send(String queue, Object payload) { return send(queue, payload, SendOptions.immediate()); }

    public SendResult send(String queue, Object payload, Duration delay) { return send(queue, payload, SendOptions.delayed(delay)); }

    /** 发送超时不代表未入队。调用方应保留选项中的消息编号并用原请求重试。 */
    public SendResult send(String queue, Object payload, SendOptions sendOptions) {
        Objects.requireNonNull(payload, "消息体不能为空");
        Objects.requireNonNull(sendOptions, "发送选项不能为空");
        Checks.queue(queue);
        String encoded = encode(payload);
        registerQueue(queue);
        JsonNode result = json(string(queue, "send", sendOptions.messageId(), encoded, digest(encoded),
                sendOptions.delay().toMillis(), sendOptions.maxDeliveries(), sendOptions.retryDelay().toMillis(),
                sendOptions.maxRetryDelay().toMillis(),
                sendOptions.deduplicationKey() == null ? "" : digest(sendOptions.deduplicationKey()),
                sendOptions.deduplicationWindow().toMillis(), options.maxQueueSize(), options.maxMessageBytes(), options.maxQueueBytes(), options.maxDedupEntries()));
        return new SendResult(result.path("id").asText(), result.path("duplicate").asBoolean());
    }

    /** 低层领取接口：调用方自行确认、续期或失败重试；数量为上限；每批最多十六条且正文合计不超过 64 KiB。 */
    public ReceiveBatch receive(String queue, int count, Duration visibility) {
        if (count < 1 || count > 100) throw new IllegalArgumentException("领取数量必须为 1 至 100");
        long lease = Checks.millis(visibility, false, "处理权期限");
        List<?> batch = eval(queue, "receive", RScript.ReturnType.MULTI, count, lease, UUID.randomUUID().toString());
        List<Delivery> messages = new ArrayList<>();
        for (int i = 1; i < batch.size(); i += 2) {
            JsonNode node = json((String) batch.get(i));
            messages.add(new Delivery(snapshot(node, (String) batch.get(i + 1)), node.path("receipt").asText()));
        }
        return new ReceiveBatch(messages, ((Number) batch.get(0)).longValue());
    }

    public <T> T decode(Delivery delivery, Class<T> payloadType) {
        try { return mapper.readValue(delivery.message().payload(), payloadType); }
        catch (JsonProcessingException e) { throw new MqException("消息反序列化失败", e); }
    }

    public boolean acknowledge(String queue, Delivery delivery) { return ownedOperation(queue, "ack", delivery, ""); }
    public boolean retry(String queue, Delivery delivery, String reason) { return ownedOperation(queue, "retry", delivery, error(reason)); }
    public boolean reject(String queue, Delivery delivery, String reason) { return ownedOperation(queue, "reject", delivery, error(reason)); }
    public boolean renew(String queue, Delivery delivery, Duration visibility) {
        return ownedOperation(queue, "renew", delivery, Checks.millis(visibility, false, "处理权期限"));
    }
    public boolean cancel(String queue, String messageId) { return pendingOperation(queue, "cancel", messageId, 0); }
    public boolean reschedule(String queue, String messageId, Duration delay) {
        return pendingOperation(queue, "reschedule", messageId, Checks.millis(delay, true, "改期延迟"));
    }
    public boolean replayDead(String queue, String messageId, Duration delay) {
        return pendingOperation(queue, "replay", messageId, Checks.millis(delay, true, "重放延迟"));
    }
    public boolean discardDead(String queue, String messageId) { return pendingOperation(queue, "discard", messageId, 0); }

    public MessageSnapshot get(String queue, String messageId) {
        Checks.name(messageId, "消息编号");
        List<?> result = read(queue, "get", RScript.ReturnType.MULTI, messageId);
        return result.isEmpty() ? null : snapshot((String) result.get(0), (String) result.get(1));
    }

    public List<MessageSnapshot> deadMessages(String queue, int offset, int count) {
        if (offset < 0 || count < 1 || count > 100) throw new IllegalArgumentException("分页起点必须非负，每页数量为 1 至 100");
        List<MessageSnapshot> result = new ArrayList<>();
        List<?> values = read(queue, "list", RScript.ReturnType.MULTI, 4, offset, count, "full");
        for (int i = 0; i < values.size(); i += 2) result.add(snapshot((String) values.get(i), (String) values.get(i + 1)));
        return List.copyOf(result);
    }

    /** 升级旧存储：按状态和偏移量迁移至多十条，不移动索引；返回检查的条数。
     * 升级期间暂停该队列的全部生产者、消费者和管理写入；依次遍历三个状态，直到返回零。
     */
    public int migrateStorage(String queue, MessageState state, int offset, int count) {
        Objects.requireNonNull(state, "消息状态不能为空");
        if (offset < 0 || count < 1 || count > 10) throw new IllegalArgumentException("迁移起点必须非负，每批数量为 1 至 10");
        int index = switch (state) { case SCHEDULED -> 1; case PROCESSING -> 2; case DEAD -> 4; };
        return ((Long) eval(queue, "migrate", RScript.ReturnType.INTEGER, index, offset, count)).intValue();
    }

    public QueueStats stats(String queue) {
        List<?> values = read(queue, "stats", RScript.ReturnType.MULTI);
        return new QueueStats(number(values, 0), number(values, 1), number(values, 2), number(values, 3), number(values, 4), number(values, 5), number(values, 6), number(values, 7));
    }

    /** 队列目录仅用于发现与监控，不参与消息状态转换，也不扫描 Redis。 */
    public void registerQueue(String queue) {
        Checks.queue(queue);
        registered.computeIfAbsent(queue, name -> {
            cachedEval(RScript.Mode.READ_WRITE, DIRECTORY_SCRIPT, RScript.ReturnType.INTEGER,
                    List.of(directoryStorageKey()), name, options.maxRegisteredQueues());
            return true;
        });
    }

    public String namespace() { return namespace; }
    public String directoryStorageKey() { return "rmq:directory:{" + namespace + "}"; }
    public int registeredQueueCount() { return redis.getScoredSortedSet(directoryStorageKey(), StringCodec.INSTANCE).size(); }
    public List<String> registeredQueues(int offset, int count) {
        if (offset < 0 || count < 1 || count > 100) throw new IllegalArgumentException("目录分页起点必须非负，每页数量为 1 至 100");
        int end = (int) Math.min(Integer.MAX_VALUE, (long) offset + count - 1);
        return List.copyOf(redis.<String>getScoredSortedSet(directoryStorageKey(), StringCodec.INSTANCE).valueRange(offset, end));
    }

    public List<MessageSummary> messages(String queue, MessageState state, int offset, int count) {
        Objects.requireNonNull(state, "消息状态不能为空");
        if (offset < 0 || count < 1 || count > 100) throw new IllegalArgumentException("消息分页起点必须非负，每页数量为 1 至 100");
        int index = switch (state) { case SCHEDULED -> 1; case PROCESSING -> 2; case DEAD -> 4; };
        List<MessageSummary> result = new ArrayList<>();
        for (JsonNode node : json(read(queue, "list", RScript.ReturnType.VALUE, index, offset, count, "summary"))) {
            MessageSnapshot item = snapshot(node);
            result.add(new MessageSummary(item.id(), item.state(), item.deliveryCount(), item.maxDeliveries(),
                    item.createdAt(), item.availableAt(), item.leaseUntil(), item.lastError(), node.path("ready").asBoolean()));
        }
        return List.copyOf(result);
    }

    /** 返回固定键清单，便于按完整名称运维；不扫描、不清空数据库。第七项是通知频道。 */
    public List<String> storageKeys(String queue) {
        Checks.queue(queue);
        String prefix = "rmq:{" + namespace + ":" + queue + "}:";
        return SUFFIXES.stream().map(prefix::concat).toList();
    }

    public <T> MqConsumer<T> consume(String queue, Class<T> payloadType, MessageHandler<T> handler, ConsumerOptions consumerOptions) {
        registerQueue(queue);
        MqConsumer<T> consumer = new MqConsumer<>(this, queue, payloadType, handler, consumerOptions);
        consumer.start();
        return consumer;
    }

    AutoCloseable subscribe(String queue, Runnable wakeup) {
        var topic = redis.getTopic(storageKeys(queue).get(6), StringCodec.INSTANCE);
        int id = topic.addListener(String.class, (channel, message) -> wakeup.run());
        return () -> topic.removeListener(id);
    }

    private boolean ownedOperation(String queue, String operation, Delivery delivery, Object argument) {
        Objects.requireNonNull(delivery, "领取记录不能为空");
        return (Long) eval(queue, operation, RScript.ReturnType.INTEGER, delivery.message().id(), delivery.receipt(), argument) == 1;
    }
    private boolean pendingOperation(String queue, String operation, String id, Object argument) {
        Checks.name(id, "消息编号");
        return (Long) eval(queue, operation, RScript.ReturnType.INTEGER, id, argument) == 1;
    }
    private String string(String queue, String operation, Object... arguments) {
        return eval(queue, operation, RScript.ReturnType.VALUE, arguments);
    }
    private <T> T eval(String queue, String operation, RScript.ReturnType returnType, Object... arguments) {
        List<Object> parameters = new ArrayList<>();
        parameters.add(operation);
        parameters.add(options.idempotencyRetention().toMillis());
        parameters.addAll(List.of(arguments));
        List<Object> keys = new ArrayList<>(storageKeys(queue));
        return cachedEval(RScript.Mode.READ_WRITE, SCRIPT, returnType, keys, parameters.toArray());
    }
    private <T> T read(String queue, String operation, RScript.ReturnType returnType, Object... arguments) {
        List<Object> parameters = new ArrayList<>();
        parameters.add(operation);
        parameters.addAll(List.of(arguments));
        return cachedEval(RScript.Mode.READ_ONLY, READ_SCRIPT, returnType,
                new ArrayList<>(storageKeys(queue)), parameters.toArray());
    }
    private <T> T cachedEval(RScript.Mode mode, String script, RScript.ReturnType type, List<Object> keys, Object... arguments) {
        var executor = redis.getScript(StringCodec.INSTANCE);
        try { return executor.evalSha(mode, script == SCRIPT ? WRITE_SHA : script == READ_SCRIPT ? READ_SHA : DIRECTORY_SHA, type, keys, arguments); }
        catch (RedisNoScriptException missing) {
            // EVAL 同时填充目标节点缓存；重启、故障切换或 SCRIPT FLUSH 后可自行恢复。
            return executor.eval(mode, script, type, keys, arguments);
        }
    }
    private String encode(Object payload) {
        LimitedOutput output = new LimitedOutput(options.maxMessageBytes());
        try { mapper.writeValue(output, payload); }
        catch (IOException exception) {
            for (Throwable cause = exception; cause != null; cause = cause.getCause()) {
                if (cause instanceof MessageTooLargeException) throw new IllegalArgumentException("消息体超过配置大小上限", exception);
            }
            throw new MqException("消息序列化失败", exception);
        }
        return output.bytes.toString(StandardCharsets.UTF_8);
    }
    /** 序列化时即限制 UTF-8 字节数，避免先生成任意大的中间字符串。 */
    private static final class LimitedOutput extends OutputStream {
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        private final int limit;
        private LimitedOutput(int limit) { this.limit = limit; }
        @Override public void write(int value) throws IOException {
            if (bytes.size() == limit) throw new MessageTooLargeException();
            bytes.write(value);
        }
        @Override public void write(byte[] value, int offset, int length) throws IOException {
            if (length > limit - bytes.size()) throw new MessageTooLargeException();
            bytes.write(value, offset, length);
        }
    }
    private static final class MessageTooLargeException extends IOException { }
    private JsonNode json(String value) {
        try { return mapper.readTree(value); }
        catch (JsonProcessingException e) { throw new MqException("Redis 消息数据解析失败", e); }
    }
    private MessageSnapshot snapshot(String metadata, String payload) {
        return snapshot(json(metadata), payload);
    }
    private static MessageSnapshot snapshot(JsonNode node, String payload) {
        ((com.fasterxml.jackson.databind.node.ObjectNode) node).put("payload", payload);
        return snapshot(node);
    }
    private static MessageSnapshot snapshot(JsonNode node) {
        int version = node.path("version").asInt();
        if (version != 1 && version != 2) throw new IllegalStateException("不支持此消息格式版本");
        return new MessageSnapshot(node.path("id").asText(), node.path("payload").asText(),
                MessageState.valueOf(node.path("state").asText()), node.path("deliveryCount").asInt(),
                node.path("maxDeliveries").asInt(), node.path("createdAt").asLong(), node.path("availableAt").asLong(),
                node.path("leaseUntil").asLong(), node.path("lastError").asText());
    }
    private static long number(List<?> values, int index) { return ((Number) values.get(index)).longValue(); }
    private static String error(String reason) {
        if (reason == null) return "未提供失败原因";
        return reason.substring(0, Math.min(reason.length(), 512));
    }
    private static String digest(String value) {
        return hash("SHA-256", value);
    }
    private static String hash(String algorithm, String value) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance(algorithm).digest(value.getBytes(StandardCharsets.UTF_8))); }
        catch (NoSuchAlgorithmException e) { throw new IllegalStateException("当前环境不支持 " + algorithm, e); }
    }
    private static String loadScript(String name) {
        try (InputStream input = RedisMq.class.getResourceAsStream(name)) {
            if (input == null) throw new IllegalStateException("缺少消息队列 Lua 资源");
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) { throw new IllegalStateException("读取消息队列 Lua 资源失败", e); }
    }
}
