package com.mk.mq;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.redisson.Redisson;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/** 仅连接显式传入的隔离测试 Redis；按完整键名清理本轮随机命名空间。 */
@EnabledIfSystemProperty(named = "mq.redis", matches = ".+")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RedisMqIntegrationTest {
    private RedissonClient redis;
    private RedisMq mq;
    private String namespace;
    private final Set<String> queues = new HashSet<>();
    private static final Duration LEASE = Duration.ofSeconds(3);

    @BeforeAll void connect() {
        Config config = new Config().setThreads(4).setNettyThreads(4);
        config.useSingleServer().setAddress(System.getProperty("mq.redis"))
                .setConnectionMinimumIdleSize(2).setConnectionPoolSize(16).setRetryAttempts(0).setTimeout(2000);
        redis = Redisson.create(config);
    }
    @AfterAll void disconnect() { if (redis != null) redis.shutdown(); }
    @BeforeEach void isolate() {
        namespace = "test:" + UUID.randomUUID();
        mq = client(MqOptions.defaults());
        queues.clear();
    }
    @AfterEach void clean() {
        for (String queue : queues) redis.getKeys().delete(mq.storageKeys(queue).toArray(String[]::new));
        redis.getKeys().delete(mq.directoryStorageKey());
    }
    private RedisMq client(MqOptions options) { return new RedisMq(redis, namespace, options, new ObjectMapper()); }
    private String queue(String name) { queues.add(name); return name; }
    private Delivery take(String queue) { return mq.receive(queue, 1, LEASE).deliveries().get(0); }
    private long bytes(String queue) {
        String value = redis.<String>getBucket(mq.storageKeys(queue).get(8), StringCodec.INSTANCE).get();
        return value == null ? 0 : Long.parseLong(value);
    }
    private static SendOptions fast(int attempts) {
        return SendOptions.immediate().withRetry(attempts, Duration.ofMillis(10), Duration.ofMillis(20));
    }
    static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) fail("等待状态变化超时");
            Thread.sleep(10);
        }
    }

    @Test void deliveryAndCompletedRequestAreIdempotent() {
        String q = queue("basic");
        SendOptions request = SendOptions.immediate();
        SendResult first = mq.send(q, Map.of("内容", "任务"), request);
        assertFalse(first.duplicate());
        assertEquals(new SendResult(first.messageId(), true), mq.send(q, Map.of("内容", "任务"), request));
        Delivery delivery = take(q);
        assertEquals(Map.of("内容", "任务"), mq.decode(delivery, Map.class));
        assertTrue(mq.acknowledge(q, delivery));
        assertFalse(mq.acknowledge(q, delivery));
        assertNull(mq.get(q, first.messageId()));
        assertEquals(0, bytes(q));
        assertEquals(new SendResult(first.messageId(), true), mq.send(q, Map.of("内容", "任务"), request));
        assertThrows(RuntimeException.class, () -> mq.send(q, "different", request));
    }

    @Test void delayedCancelAndRescheduleRespectState() throws Exception {
        String q = queue("delay");
        String id = mq.send(q, "later", Duration.ofHours(1)).messageId();
        assertEquals(0, mq.receive(q, 10, LEASE).deliveries().size());
        assertEquals(1, mq.stats(q).delayed());
        assertTrue(mq.reschedule(q, id, Duration.ZERO));
        Delivery delivery = take(q);
        assertFalse(mq.cancel(q, id));
        assertFalse(mq.reschedule(q, id, Duration.ZERO));
        assertTrue(mq.acknowledge(q, delivery));
        String other = mq.send(q, "cancel").messageId();
        assertTrue(mq.cancel(q, other));
        assertFalse(mq.cancel(q, other));
        assertEquals(0, bytes(q));
    }

    @Test void timeoutInvalidatesEveryOldOwnershipOperation() throws Exception {
        String q = queue("expired");
        mq.send(q, "work", fast(2));
        Delivery stale = mq.receive(q, 1, Duration.ofMillis(50)).deliveries().get(0);
        Thread.sleep(65);
        assertFalse(mq.acknowledge(q, stale));
        assertFalse(mq.retry(q, stale, "old"));
        assertFalse(mq.reject(q, stale, "old"));
        assertFalse(mq.renew(q, stale, LEASE));
        assertTrue(mq.receive(q, 1, LEASE).deliveries().isEmpty());
        Thread.sleep(20);
        Delivery current = take(q);
        assertNotEquals(stale.receipt(), current.receipt());
        assertEquals(2, current.message().deliveryCount());
        assertFalse(mq.acknowledge(q, stale));
        assertTrue(mq.renew(q, current, LEASE));
        assertTrue(mq.retry(q, current, "exhausted"));
        assertEquals(MessageState.DEAD, mq.get(q, current.message().id()).state());
    }

    @Test void deadLettersCanBeReplayedRejectedAndDiscarded() {
        String q = queue("dead");
        String id = mq.send(q, "poison").messageId();
        assertTrue(mq.reject(q, take(q), "invalid"));
        assertEquals("\"poison\"", mq.deadMessages(q, 0, 100).get(0).payload());
        assertEquals(1, mq.stats(q).dead());
        assertTrue(mq.replayDead(q, id, Duration.ZERO));
        Delivery next = take(q);
        assertEquals(1, next.message().deliveryCount());
        assertTrue(mq.reject(q, next, "still invalid"));
        assertTrue(mq.discardDead(q, id));
        assertEquals(0, bytes(q));
        assertEquals(0, mq.stats(q).total());
    }

    @Test void countAndBytesIncludeProcessingAndDeadMessages() {
        String q = queue("capacity");
        mq = client(new MqOptions(3, 16, Duration.ofHours(1), 16, 100));
        mq.send(q, "123456"); // JSON 共八字节。
        mq.send(q, "123456");
        assertEquals(16, bytes(q));
        assertThrows(RuntimeException.class, () -> mq.send(q, "x"));
        Delivery processing = take(q);
        assertThrows(RuntimeException.class, () -> mq.send(q, "x"));
        assertTrue(mq.reject(q, processing, "dead"));
        assertThrows(RuntimeException.class, () -> mq.send(q, "x"));
        assertTrue(mq.discardDead(q, processing.message().id()));
        mq.send(q, "x");
        assertEquals(11, bytes(q));
        mq.send(q, "x");
        assertEquals(3, mq.stats(q).total());
        assertThrows(RuntimeException.class, () -> mq.send(q, "x"));
        assertThrows(IllegalArgumentException.class, () -> mq.send(q, "x".repeat(20)));
    }

    @Test void dedupAliasesCannotGrowWithoutBoundAndRetriesStillWorkWhenFull() {
        String q = queue("dedup");
        mq = client(new MqOptions(10, 64, Duration.ofHours(1), 1024, 3));
        SendOptions first = SendOptions.immediate().withDeduplication("business", Duration.ofHours(1));
        String id = mq.send(q, "same", first).messageId();
        SendOptions alias = first.withMessageId(UUID.randomUUID().toString());
        assertEquals(id, mq.send(q, "same", alias).messageId());
        assertEquals(id, mq.send(q, "same", alias).messageId());
        assertEquals(id, mq.send(q, "same", first).messageId());
        assertThrows(RuntimeException.class, () -> mq.send(q, "same", first.withMessageId(UUID.randomUUID().toString())));
        assertThrows(RuntimeException.class, () -> mq.send(q, "new"));
        assertTrue(mq.acknowledge(q, take(q)));
        assertEquals(id, mq.send(q, "same", first).messageId());
        assertEquals(3, redis.getMap(mq.storageKeys(q).get(4), StringCodec.INSTANCE).size());
    }

    @Test void expiredAliasCanBeRememberedAgainWhileBusinessWindowRemainsActive() throws Exception {
        String q = queue("alias-expiry");
        mq = client(new MqOptions(10, 64, Duration.ofMillis(40), 1024, 3));
        SendOptions first = SendOptions.immediate().withDeduplication("business", Duration.ofHours(1));
        String id = mq.send(q, "same", first).messageId();
        SendOptions alias = first.withMessageId(UUID.randomUUID().toString());
        assertEquals(id, mq.send(q, "same", alias).messageId());
        Thread.sleep(60);
        assertEquals(id, mq.send(q, "same", alias).messageId());
        assertEquals(id, mq.send(q, "same", alias).messageId());
        assertEquals(3, mq.stats(q).dedupEntries());
        assertEquals(1, mq.stats(q).total());
    }

    @Test void expiredDedupEntriesAreReclaimedButLiveIdsStayReserved() throws Exception {
        String q = queue("retention");
        mq = client(new MqOptions(10, 64, Duration.ofMillis(40), 1024, 2));
        SendOptions first = SendOptions.immediate();
        mq.send(q, "a", first);
        Thread.sleep(60);
        mq.send(q, "b");
        assertThrows(RuntimeException.class, () -> mq.send(q, "c"));
        assertEquals(2, redis.getMap(mq.storageKeys(q).get(4), StringCodec.INSTANCE).size());
        Delivery one = take(q);
        assertTrue(mq.acknowledge(q, one));
        Thread.sleep(60);
        mq.send(q, "c");
        assertEquals(2, mq.stats(q).total());
    }

    @Test void concurrentClaimsHaveOneOwner() throws Exception {
        String q = queue("race");
        for (int i = 0; i < 100; i++) mq.send(q, i);
        ExecutorService pool = Executors.newFixedThreadPool(8);
        Set<String> ids = ConcurrentHashMap.newKeySet();
        try {
            List<Callable<Void>> tasks = new ArrayList<>();
            for (int i = 0; i < 8; i++) tasks.add(() -> {
                while (true) {
                    var batch = mq.receive(q, 10, LEASE).deliveries();
                    if (batch.isEmpty()) break;
                    for (var delivery : batch) {
                        assertTrue(ids.add(delivery.message().id()));
                        assertTrue(mq.acknowledge(q, delivery));
                    }
                }
                return null;
            });
            for (Future<Void> future : pool.invokeAll(tasks)) future.get();
        } finally { pool.shutdownNow(); }
        assertEquals(100, ids.size());
        assertEquals(0, mq.stats(q).total());
        assertEquals(0, bytes(q));
    }

    @Test void largeClaimsAndFullDeadPagesHaveByteBudgets() {
        String q = queue("large");
        String body = "中".repeat(21000);
        for (int i = 0; i < 12; i++) mq.send(q, body);
        var batch = mq.receive(q, 100, LEASE).deliveries();
        assertEquals(1, batch.size());
        for (var delivery : batch) {
            assertEquals(body, mq.decode(delivery, String.class));
            assertTrue(mq.reject(q, delivery, "test"));
        }
        while (mq.stats(q).ready() > 0) {
            for (var delivery : mq.receive(q, 100, LEASE).deliveries()) mq.reject(q, delivery, "test");
        }
        assertEquals(1, mq.deadMessages(q, 0, 100).size());
        assertEquals(12, mq.messages(q, MessageState.DEAD, 0, 100).size());
        String metadata = redis.<String, String>getMap(mq.storageKeys(q).get(2), StringCodec.INSTANCE).values().iterator().next();
        assertTrue(metadata.length() < 2048);
        assertFalse(metadata.contains(body));
    }

    @Test void readQueriesDoNotRecoverExpiredLeases() throws Exception {
        String q = queue("readonly");
        mq.send(q, "work");
        Delivery delivery = mq.receive(q, 1, Duration.ofMillis(10)).deliveries().get(0);
        Thread.sleep(20);
        for (int i = 0; i < 3; i++) {
            assertEquals(1, mq.stats(q).expiredLeases());
            assertEquals(MessageState.PROCESSING, mq.get(q, delivery.message().id()).state());
            assertEquals(1, mq.messages(q, MessageState.PROCESSING, 0, 100).size());
        }
        assertEquals(1, mq.get(q, delivery.message().id()).deliveryCount());
    }

    @Test void storageUpgradeIsBoundedRepeatableAndKeepsPayloadAndBytes() {
        String q = queue("legacy");
        String id = mq.send(q, "legacy").messageId();
        List<String> keys = mq.storageKeys(q);
        // 构造原版内联正文格式，不依赖旧构件或其他命名空间。
        redis.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_WRITE,
                "local m=cjson.decode(redis.call('HGET',KEYS[1],ARGV[1])); m.payload=redis.call('HGET',KEYS[2],ARGV[1]); m.payloadBytes=nil; m.version=1; redis.call('HSET',KEYS[1],ARGV[1],cjson.encode(m)); redis.call('HDEL',KEYS[2],ARGV[1]); redis.call('DEL',KEYS[3]); return 1",
                RScript.ReturnType.INTEGER, List.of(keys.get(2), keys.get(7), keys.get(8)), id);
        assertEquals("\"legacy\"", mq.get(q, id).payload());
        assertThrows(RuntimeException.class, () -> mq.send(q, "new"));
        assertEquals(1, mq.migrateStorage(q, MessageState.SCHEDULED, 0, 10));
        assertEquals(8, bytes(q));
        assertEquals(1, mq.migrateStorage(q, MessageState.SCHEDULED, 0, 10));
        assertEquals(8, bytes(q));
        mq.send(q, "new");
        assertEquals(13, bytes(q));
        assertTrue(mq.cancel(q, id));
        assertEquals(5, bytes(q));
    }

    @Test void wrongKeyTypeFailsBeforeMutation() {
        String q = queue("wrong-type");
        String key = mq.storageKeys(q).get(7);
        redis.getBucket(key, StringCodec.INSTANCE).set("wrong");
        assertThrows(RuntimeException.class, () -> mq.send(q, "work"));
        assertEquals(0, redis.getMap(mq.storageKeys(q).get(2), StringCodec.INSTANCE).size());
    }

    @Test void missingByteCounterFailsBeforeAcknowledgmentMutatesState() {
        String q = queue("corrupt-counter");
        mq.send(q, "work");
        Delivery delivery = take(q);
        redis.getKeys().delete(mq.storageKeys(q).get(8));
        assertThrows(RuntimeException.class, () -> mq.acknowledge(q, delivery));
        assertNotNull(mq.get(q, delivery.message().id()));
        assertEquals(1, mq.stats(q).processing());
    }

    @Test void missingPayloadIsNotDeliveredAsAnEmptySuccessfulTask() {
        String q = queue("missing-payload");
        String id = mq.send(q, "work").messageId();
        redis.getMap(mq.storageKeys(q).get(7), StringCodec.INSTANCE).remove(id);
        assertThrows(RuntimeException.class, () -> mq.receive(q, 1, LEASE));
        assertEquals(1, mq.stats(q).ready());
        assertThrows(RuntimeException.class, () -> mq.get(q, id));
    }

    @Test void scriptFlushIsRecoveredWithoutLosingIdempotency() {
        String q = queue("script-cache");
        SendOptions request = SendOptions.immediate();
        String id = mq.send(q, "work", request).messageId();
        redis.getScript().scriptFlush();
        assertEquals(id, mq.send(q, "work", request).messageId());
        assertEquals(id, mq.get(q, id).id());
        assertTrue(mq.acknowledge(q, take(q)));
    }

    @Test void namespaceIsolationAndDirectoryBoundAreEnforced() {
        String q = queue("isolated");
        mq = client(new MqOptions(10, 64, Duration.ofHours(1), 1024, 100, 1));
        mq.registerQueue(q);
        mq.registerQueue(q);
        assertThrows(RuntimeException.class, () -> mq.registerQueue(queue("second")));
        assertEquals(List.of(q), mq.registeredQueues(0, 100));
        RedisMq other = new RedisMq(redis, namespace + ":other");
        assertEquals(0, other.stats(q).total());
        assertThrows(IllegalArgumentException.class, () -> mq.send("other:isolated", "work"));
    }

    @Test void automaticConsumerRenewsWhileHandlingAndAcknowledges() throws Exception {
        String q = queue("consumer");
        AtomicInteger handled = new AtomicInteger();
        ConsumerOptions options = new ConsumerOptions(2, Duration.ofMillis(300), Duration.ofSeconds(3), Duration.ofMillis(20), Duration.ofSeconds(2));
        try (var consumer = mq.consume(q, String.class, (body, context) -> {
            Thread.sleep(650);
            handled.incrementAndGet();
        }, options)) {
            for (int i = 0; i < 4; i++) mq.send(q, "work");
            await(() -> mq.stats(q).total() == 0);
            assertEquals(4, handled.get());
        }
    }

    @Test void concurrentProducersCannotExceedByteOrCountLimits() throws Exception {
        String q = queue("producer-race");
        mq = client(new MqOptions(20, 32, Duration.ofHours(1), 100, 100));
        ExecutorService pool = Executors.newFixedThreadPool(8);
        AtomicInteger accepted = new AtomicInteger();
        try {
            List<Callable<Void>> tasks = new ArrayList<>();
            for (int i = 0; i < 80; i++) tasks.add(() -> {
                try { mq.send(q, "12345678"); accepted.incrementAndGet(); }
                catch (org.redisson.client.RedisException full) { assertTrue(full.getMessage().contains("MQ_QUEUE_BYTES_FULL")); }
                return null;
            });
            for (var result : pool.invokeAll(tasks)) result.get();
        } finally { pool.shutdownNow(); }
        assertEquals(10, accepted.get());
        assertEquals(100, mq.stats(q).payloadBytes());
        assertEquals(10, mq.stats(q).dedupEntries());
    }

    @Test void timeoutStormIsRecoveredInBoundedBatches() throws Exception {
        String q = queue("timeout-storm");
        for (int i = 0; i < 100; i++) mq.send(q, "work", fast(1));
        int claimed = 0;
        while (claimed < 100) claimed += mq.receive(q, 100, Duration.ofSeconds(1)).deliveries().size();
        Thread.sleep(1100);
        assertTrue(mq.receive(q, 1, LEASE).deliveries().isEmpty());
        assertEquals(8, mq.stats(q).dead());
        assertEquals(92, mq.stats(q).processing());
        for (int i = 0; i < 12; i++) mq.receive(q, 1, LEASE);
        assertEquals(100, mq.stats(q).dead());
        assertEquals(0, mq.stats(q).processing());
    }

    @Test void forcedShutdownDoesNotAcknowledgeLateCompletion() throws Exception {
        String q = queue("shutdown");
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch exited = new CountDownLatch(1);
        var options = new ConsumerOptions(1, Duration.ofMillis(300), Duration.ofSeconds(10), Duration.ofMillis(20), Duration.ZERO);
        var consumer = mq.consume(q, String.class, (body, context) -> {
            entered.countDown();
            while (release.getCount() != 0) {
                try { release.await(); }
                catch (InterruptedException ignored) { /* 模拟无法及时中断的外部业务调用。 */ }
            }
            exited.countDown();
        }, options);
        try {
            mq.send(q, "work", fast(2));
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            consumer.close();
            release.countDown();
            assertTrue(exited.await(2, TimeUnit.SECONDS));
            assertEquals(1, mq.stats(q).total());
            Thread.sleep(350);
            mq.receive(q, 1, LEASE);
            Thread.sleep(20);
            Delivery recovered = take(q);
            assertEquals(2, recovered.message().deliveryCount());
            assertTrue(mq.acknowledge(q, recovered));
        } finally { release.countDown(); consumer.close(); }
    }

    @Test void metricsObserveOnlyAcceptedTransitionsAndCannotBreakDelivery() throws Exception {
        String q = queue("observed");
        var counts = new java.util.concurrent.atomic.AtomicLongArray(MqMetricsListener.Event.values().length);
        var observation = mq.observe((name, event, count, nanos) -> counts.addAndGet(event.ordinal(), count));
        try {
            SendOptions request = fast(2);
            mq.send(q, "work", request); mq.send(q, "work", request);
            var delivery = take(q);
            assertTrue(mq.retry(q, delivery, "retry"));
            assertFalse(mq.retry(q, delivery, "stale"));
            Thread.sleep(20);
            assertTrue(mq.acknowledge(q, take(q)));
            assertEquals(1, counts.get(MqMetricsListener.Event.SENT.ordinal()));
            assertEquals(1, counts.get(MqMetricsListener.Event.DUPLICATE.ordinal()));
            assertEquals(2, counts.get(MqMetricsListener.Event.RECEIVED.ordinal()));
            assertEquals(1, counts.get(MqMetricsListener.Event.RETRIED.ordinal()));
            assertEquals(1, counts.get(MqMetricsListener.Event.ACKNOWLEDGED.ordinal()));
            assertThrows(IllegalStateException.class, () -> mq.observe((name, event, count, nanos) -> { }));
        } finally { observation.close(); }
        try (var ignored = mq.observe((name, event, count, nanos) -> { throw new IllegalStateException("观测器故障"); })) {
            mq.send(q, "still works"); assertTrue(mq.acknowledge(q, take(q)));
        }
    }

    @Test void expirationMetricsIncludeServerSideRecoveryAndDeadTransitions() throws Exception {
        String q = queue("observed-expiry");
        var counts = new java.util.concurrent.atomic.AtomicLongArray(MqMetricsListener.Event.values().length);
        try (var observation = mq.observe((name, event, count, nanos) -> counts.addAndGet(event.ordinal(), count))) {
            mq.send(q, "work", fast(1));
            mq.receive(q, 1, Duration.ofMillis(50)); Thread.sleep(70);
            mq.receive(q, 1, LEASE);
            assertEquals(1, counts.get(MqMetricsListener.Event.EXPIRED.ordinal()));
            assertEquals(1, counts.get(MqMetricsListener.Event.DEAD.ordinal()));
        }
    }

    @Test void processingDeadlineDoesNotWaitForHeartbeatInterval() throws Exception {
        String q = queue("deadline");
        CountDownLatch interrupted = new CountDownLatch(1);
        ConsumerOptions options = new ConsumerOptions(1, Duration.ofSeconds(30), Duration.ofMillis(100), Duration.ofMillis(20), Duration.ofSeconds(1));
        try (var consumer = mq.consume(q, String.class, (body, context) -> {
            try { Thread.sleep(5000); }
            catch (InterruptedException e) { interrupted.countDown(); throw e; }
        }, options)) {
            mq.send(q, "work");
            assertTrue(interrupted.await(2, TimeUnit.SECONDS));
            assertEquals(1, mq.stats(q).total());
        }
    }
}
