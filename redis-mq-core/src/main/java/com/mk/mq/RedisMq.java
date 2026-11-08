package com.mk.mq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

import java.io.IOException;
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
    private static final List<String> SUFFIXES = List.of("scheduled", "processing", "messages", "dead", "dedup", "dedup-expiry", "wakeup");
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
        this.mapper = Objects.requireNonNull(mapper, "JSON 编码器不能为空").copy();
    }

    public SendResult send(String queue, Object payload) { return send(queue, payload, SendOptions.immediate()); }

    public SendResult send(String queue, Object payload, Duration delay) { return send(queue, payload, SendOptions.delayed(delay)); }

    /** 发送超时不代表未入队。调用方应保留选项中的消息编号并用原请求重试。 */
    public SendResult send(String queue, Object payload, SendOptions sendOptions) {
        Objects.requireNonNull(payload, "消息体不能为空");
        Objects.requireNonNull(sendOptions, "发送选项不能为空");
        String encoded;
        try { encoded = mapper.writeValueAsString(payload); }
        catch (JsonProcessingException e) { throw new MqException("消息序列化失败", e); }
        if (encoded.getBytes(StandardCharsets.UTF_8).length > options.maxMessageBytes()) {
            throw new IllegalArgumentException("消息体超过配置大小上限");
        }
        registerQueue(queue);
        JsonNode result = json(string(queue, "send", sendOptions.messageId(), encoded, digest(encoded),
                sendOptions.delay().toMillis(), sendOptions.maxDeliveries(), sendOptions.retryDelay().toMillis(),
                sendOptions.maxRetryDelay().toMillis(),
                sendOptions.deduplicationKey() == null ? "" : digest(sendOptions.deduplicationKey()),
                sendOptions.deduplicationWindow().toMillis(), options.maxQueueSize(), options.maxMessageBytes()));
        return new SendResult(result.path("id").asText(), result.path("duplicate").asBoolean());
    }

    /** 低层领取接口：调用方自行确认、续期或失败重试；每批最多一百条。 */
    public ReceiveBatch receive(String queue, int count, Duration visibility) {
        if (count < 1 || count > 100) throw new IllegalArgumentException("领取数量必须为 1 至 100");
        long lease = Checks.millis(visibility, false, "处理权期限");
        JsonNode batch = json(string(queue, "receive", count, lease, UUID.randomUUID().toString()));
        List<Delivery> messages = new ArrayList<>();
        for (JsonNode node : batch.path("messages")) {
            messages.add(new Delivery(snapshot(node), node.path("receipt").asText()));
        }
        return new ReceiveBatch(messages, batch.path("waitMillis").asLong());
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
        String result = read(queue, "get", RScript.ReturnType.VALUE, messageId);
        return result.isEmpty() ? null : snapshot(json(result));
    }

    public List<MessageSnapshot> deadMessages(String queue, int offset, int count) {
        if (offset < 0 || count < 1 || count > 100) throw new IllegalArgumentException("分页起点必须非负，每页数量为 1 至 100");
        List<MessageSnapshot> result = new ArrayList<>();
        for (JsonNode node : json(read(queue, "list", RScript.ReturnType.VALUE, 4, offset, count, "full"))) result.add(snapshot(node));
        return List.copyOf(result);
    }

    public QueueStats stats(String queue) {
        List<?> values = read(queue, "stats", RScript.ReturnType.MULTI);
        return new QueueStats(number(values, 0), number(values, 1), number(values, 2), number(values, 3), number(values, 4), number(values, 5));
    }

    /** 队列目录仅用于发现与监控，不参与消息状态转换，也不扫描 Redis。 */
    public void registerQueue(String queue) {
        Checks.name(queue, "队列名称");
        registered.computeIfAbsent(queue, name -> {
            redis.getScoredSortedSet(directoryStorageKey(), StringCodec.INSTANCE).add(0, name);
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

    /** 返回固定键清单，便于按完整名称运维；不扫描、不清空数据库。最后一项是通知频道。 */
    public List<String> storageKeys(String queue) {
        Checks.name(queue, "队列名称");
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
        return redis.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_WRITE, SCRIPT, returnType, keys, parameters.toArray());
    }
    private <T> T read(String queue, String operation, RScript.ReturnType returnType, Object... arguments) {
        List<Object> parameters = new ArrayList<>();
        parameters.add(operation);
        parameters.addAll(List.of(arguments));
        return redis.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_ONLY, READ_SCRIPT, returnType,
                new ArrayList<>(storageKeys(queue)), parameters.toArray());
    }
    private JsonNode json(String value) {
        try { return mapper.readTree(value); }
        catch (JsonProcessingException e) { throw new MqException("Redis 消息数据解析失败", e); }
    }
    private static MessageSnapshot snapshot(JsonNode node) {
        if (node.path("version").asInt() != 1) throw new IllegalStateException("不支持此消息格式版本");
        return new MessageSnapshot(node.path("id").asText(), node.path("payload").asText(),
                MessageState.valueOf(node.path("state").asText()), node.path("deliveryCount").asInt(),
                node.path("maxDeliveries").asInt(), node.path("createdAt").asLong(), node.path("availableAt").asLong(),
                node.path("leaseUntil").asLong(), node.path("lastError").asText());
    }
    private static long number(List<?> values, int index) { return ((Number) values.get(index)).longValue(); }
    private static String error(String reason) {
        if (reason == null) return "未提供失败原因";
        return reason.substring(0, Math.min(reason.length(), 2048));
    }
    private static String digest(String value) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8))); }
        catch (NoSuchAlgorithmException e) { throw new IllegalStateException("当前环境不支持 SHA-256", e); }
    }
    private static String loadScript(String name) {
        try (InputStream input = RedisMq.class.getResourceAsStream(name)) {
            if (input == null) throw new IllegalStateException("缺少消息队列 Lua 资源");
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) { throw new IllegalStateException("读取消息队列 Lua 资源失败", e); }
    }
}
