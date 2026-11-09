package com.mk.mq.dashboard.mysql;

import com.mk.mq.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperties;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfSystemProperties({@EnabledIfSystemProperty(named = "mq.redis", matches = ".+"), @EnabledIfSystemProperty(named = "mq.mysql", matches = ".+")})
class MysqlHistoryIntegrationTest {
    private RedissonClient redis;
    private String namespace;
    private PersistenceProperties properties;
    @BeforeEach void connect() throws Exception {
        Config config = new Config().setThreads(2).setNettyThreads(2);
        config.useSingleServer().setAddress(System.getProperty("mq.redis")).setConnectionMinimumIdleSize(1).setConnectionPoolSize(8);
        redis = Redisson.create(config);
        namespace = "history-test:" + UUID.randomUUID();
        properties = MysqlTestSupport.properties();
        // 每个用例自行准备空库，不能依赖其他测试类先建表。
        // 尤其数据库不可达用例本身不会访问正常数据源，但清理仍需要这些表。
        try (var source = MysqlTestSupport.source(properties)) { MysqlTestSupport.initialize(source); }
    }
    @AfterEach void close() throws Exception {
        RedisMq mq = new RedisMq(redis, namespace);
        for (String queue : mq.registeredQueues(0, 100)) redis.getKeys().delete(mq.storageKeys(queue).toArray(String[]::new));
        redis.getKeys().delete(mq.directoryStorageKey());
        try (var source = MysqlTestSupport.source(properties)) { MysqlTestSupport.removeScope(source, properties.getSourceId()); }
        redis.shutdown();
    }
    @Test void gracefulCollectorHandoverKeepsHistoryAndRejectsDuplicateOwnership() throws Exception {
        RedisMq one = new RedisMq(redis, namespace), two = new RedisMq(redis, namespace);
        one.registerQueue("jobs");
        try (var sourceOne = MysqlTestSupport.source(properties); var sourceTwo = MysqlTestSupport.source(properties)) {
            MysqlTestSupport.initialize(sourceOne);
            try (var first = new MysqlHistory(one, properties, sourceOne); var second = new MysqlHistory(two, properties, sourceTwo)) {
                first.start(); HistoryWebE2ETest.await(() -> first.status().collector());
                second.start(); HistoryWebE2ETest.await(() -> "RUNNING".equals(second.status().state()));
                assertFalse(second.status().collector());
                one.send("jobs", "one"); two.send("jobs", "two");
                HistoryWebE2ETest.await(() -> {
                    var result = first.query("jobs", null, null, 100);
                    return result.summary() != null && result.summary().counts().get("sent") == 2;
                });
                first.close();
                assertThrows(IllegalStateException.class, first::start);
                try (var detached = one.observe((queue, event, count, nanos) -> { })) { /* 关闭后钩子已卸载。 */ }
                HistoryWebE2ETest.await(() -> second.status().collector());
                two.send("jobs", "after-handover");
                HistoryWebE2ETest.await(() -> {
                    var result = second.query("jobs", null, null, 100);
                    return result.summary() != null && result.summary().counts().get("sent") == 3;
                });
            }
        }
    }
    @Test void cleanupContinuesEvenWhenRedisSamplingIsUnavailable() throws Exception {
        try (var source = MysqlTestSupport.source(properties)) {
            MysqlTestSupport.initialize(source);
            var fixture = new HistoryRepository(source, properties, namespace, UUID.randomUUID().toString());
            var lease = fixture.lease(namespace);
            fixture.snapshots(java.util.List.of(new HistoryRepository.Sample("jobs", lease.now() - 6L * 86400000,
                    new QueueStats(1, 0, 0, 0, 0, 1))), lease.token());
            fixture.release();
            RedissonClient unavailable = (RedissonClient) java.lang.reflect.Proxy.newProxyInstance(RedissonClient.class.getClassLoader(),
                    new Class<?>[]{RedissonClient.class}, (proxy, method, args) -> { throw new IllegalStateException("模拟 Redis 不可用"); });
            var offline = new RedisMq(unavailable, namespace);
            try (var history = new MysqlHistory(offline, properties, source)) {
                history.start();
                HistoryWebE2ETest.await(() -> history.status().cleanupRows() == 1);
                assertTrue(history.status().lastCleanupAt() > 0);
                assertEquals(0, history.status().lastSampleAt());
            }
        }
    }
    @Test void lateSamplingCannotReopenStateAfterClose() throws Exception {
        var entered = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        var finished = new java.util.concurrent.CountDownLatch(1);
        RedissonClient blocked = (RedissonClient) java.lang.reflect.Proxy.newProxyInstance(RedissonClient.class.getClassLoader(),
                new Class<?>[]{RedissonClient.class}, (proxy, method, args) -> {
                    if (!method.getName().equals("getScoredSortedSet")) throw new AssertionError(method.getName());
                    return java.lang.reflect.Proxy.newProxyInstance(org.redisson.api.RScoredSortedSet.class.getClassLoader(),
                            new Class<?>[]{org.redisson.api.RScoredSortedSet.class}, (p, m, a) -> {
                                if (!m.getName().equals("size")) throw new AssertionError(m.getName());
                                entered.countDown();
                                while (release.getCount() != 0) {
                                    try { release.await(); } catch (InterruptedException ignored) { /* 模拟迟到的客户端 RPC。 */ }
                                }
                                finished.countDown();
                                return 0;
                            });
                });
        try (var source = MysqlTestSupport.source(properties); var history = new MysqlHistory(new RedisMq(blocked, namespace), properties, source)) {
            history.start(); assertTrue(entered.await(10, java.util.concurrent.TimeUnit.SECONDS));
            history.close(); assertEquals("STOPPED", history.status().state());
            release.countDown(); assertTrue(finished.await(2, java.util.concurrent.TimeUnit.SECONDS));
            for (int i = 0; i < 10; i++) { Thread.sleep(50); assertEquals("STOPPED", history.status().state()); }
        } finally { release.countDown(); }
    }
    @Test void unreachableMysqlDoesNotBlockSendReceiveOrAcknowledgment() throws Exception {
        var broken = MysqlTestSupport.properties();
        broken.getDatasource().setUrl("jdbc:mysql://127.0.0.1:1/unavailable");
        RedisMq mq = new RedisMq(redis, namespace);
        try (var source = MysqlTestSupport.source(broken); var history = new MysqlHistory(mq, broken, source)) {
            history.start(); HistoryWebE2ETest.await(() -> "DEGRADED".equals(history.status().state()));
            long start = System.nanoTime();
            for (int i = 0; i < 100; i++) {
                mq.send("jobs", "work");
                var delivery = mq.receive("jobs", 1, java.time.Duration.ofSeconds(5)).deliveries().get(0);
                assertTrue(mq.acknowledge("jobs", delivery));
            }
            assertTrue(System.nanoTime() - start < java.time.Duration.ofSeconds(10).toNanos(), "MQ 操作等待了历史数据库");
            assertEquals(0, mq.stats("jobs").total());
            assertTrue(history.status().pendingBuckets() <= broken.getMaxQueues() * broken.getPendingMinutes());
            assertTrue(history.status().writeFailures() > 0);
        }
    }
}
