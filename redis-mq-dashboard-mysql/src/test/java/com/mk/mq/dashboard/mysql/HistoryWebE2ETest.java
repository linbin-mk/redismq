package com.mk.mq.dashboard.mysql;

import com.mk.mq.*;
import com.mk.mq.dashboard.DashboardHistory;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperties;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import java.sql.*;
import java.time.Duration;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(classes = HistoryE2EApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EnabledIfSystemProperties({@EnabledIfSystemProperty(named = "mq.redis", matches = ".+"), @EnabledIfSystemProperty(named = "mq.mysql", matches = ".+")})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HistoryWebE2ETest {
    private static final PersistenceProperties OPTIONS = MysqlTestSupport.properties();
    private static final String NAMESPACE = "web:" + UUID.randomUUID();
    @Autowired RedisMq mq;
    @Autowired RedissonClient redis;
    @Autowired MysqlHistory history;
    @Autowired HistoryDataSource source;
    @Autowired TestRestTemplate http;
    @DynamicPropertySource static void configuration(DynamicPropertyRegistry registry) throws Exception {
        try (var source = MysqlTestSupport.source(OPTIONS)) { MysqlTestSupport.initialize(source); }
        registry.add("redis-mq.namespace", () -> NAMESPACE);
        registry.add("redis-mq.dashboard.enabled", () -> true);
        registry.add("redis-mq.dashboard.password", () -> HistoryE2EApplication.PASSWORD);
        registry.add("redis-mq.dashboard.persistence.enabled", () -> true);
        registry.add("redis-mq.dashboard.persistence.source-id", OPTIONS::getSourceId);
        registry.add("redis-mq.dashboard.persistence.flush-interval", () -> "1s");
        registry.add("redis-mq.dashboard.persistence.sample-interval", () -> "10s");
        registry.add("redis-mq.dashboard.persistence.cleanup-interval", () -> "1s");
        registry.add("redis-mq.dashboard.persistence.lease-duration", () -> "10s");
        registry.add("redis-mq.dashboard.persistence.datasource.url", () -> OPTIONS.getDatasource().getUrl());
        registry.add("redis-mq.dashboard.persistence.datasource.username", () -> OPTIONS.getDatasource().getUsername());
        registry.add("redis-mq.dashboard.persistence.datasource.password", () -> OPTIONS.getDatasource().getPassword());
    }
    private TestRestTemplate authenticated() { return http.withBasicAuth("monitor", HistoryE2EApplication.PASSWORD); }
    private DashboardHistory.Result query(String queue) {
        return authenticated().getForObject("/redis-mq/dashboard/api/queues/" + queue + "/history", DashboardHistory.Result.class);
    }
    static void await(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(25).toNanos();
        while (!condition.getAsBoolean()) { if (System.nanoTime() > deadline) fail("E2E 等待状态超时"); Thread.sleep(100); }
    }
    @AfterAll void clean() throws Exception {
        history.close();
        for (String queue : mq.registeredQueues(0, 100)) redis.getKeys().delete(mq.storageKeys(queue).toArray(String[]::new));
        redis.getKeys().delete(mq.directoryStorageKey());
        MysqlTestSupport.removeScope(source, OPTIONS.getSourceId());
    }
    @Test void realHttpRequiresAuthenticationAndPreservesReadOnlyContract() {
        assertEquals(401, http.getForEntity("/redis-mq/dashboard/api/history/status", String.class).getStatusCode().value());
        assertEquals(200, authenticated().getForEntity("/redis-mq/dashboard/history.js", String.class).getStatusCode().value());
        assertEquals(400, authenticated().getForEntity("/redis-mq/dashboard/api/queues/jobs/history?points=1001", String.class).getStatusCode().value());
        assertEquals(400, authenticated().getForEntity("/redis-mq/dashboard/api/queues/jobs/history?from=100&to=50", String.class).getStatusCode().value());
        assertEquals(403, authenticated().postForEntity("/redis-mq/dashboard/api/history/delete", null, String.class).getStatusCode().value());
    }
    @Test void realMessagesBecomeDurableHistoryWithRetriesFailuresAndLatencies() throws Exception {
        String queue = "runtime";
        try (var consumer = mq.consume(queue, String.class, (body, context) -> {
            Thread.sleep(5);
            int n = Integer.parseInt(body);
            if (n % 11 == 0) throw new RejectMessageException("E2E 拒绝");
            if (n % 5 == 0 && context.deliveryCount() == 1) throw new IllegalArgumentException("E2E 重试");
        }, new ConsumerOptions(2, Duration.ofSeconds(2), Duration.ofSeconds(10), Duration.ofMillis(20), Duration.ofSeconds(2)))) {
            for (int i = 1; i <= 33; i++) mq.send(queue, Integer.toString(i), SendOptions.immediate().withRetry(3, Duration.ofMillis(10), Duration.ofMillis(20)));
            await(() -> mq.stats(queue).dead() == 3 && mq.stats(queue).total() == 3);
            await(() -> { var result = query(queue); return result.summary() != null && result.summary().counts().get("acknowledged") == 30; });
            await(() -> query(queue).points().stream().anyMatch(DashboardHistory.Point::snapshotPresent));
            var result = query(queue);
            assertEquals(33, result.summary().counts().get("sent"));
            assertEquals(3, result.summary().counts().get("dead"));
            assertTrue(result.summary().counts().get("retried") > 0);
            assertTrue(result.summary().counts().get("handler_failure") >= 3);
            assertTrue(result.summary().processing().count() >= 33);
            assertTrue(result.summary().processing().p95Millis() > 0);
            assertTrue(result.points().stream().anyMatch(p -> p.peak() != null && p.peak().dead() == 3));
        }
    }
    @Test void independentInstancesMergeMetricsAndFailOverCollector() throws Exception {
        await(() -> "RUNNING".equals(history.status().state()));
        PersistenceProperties p = MysqlTestSupport.properties(); p.setSourceId(OPTIONS.getSourceId());
        RedisMq secondMq = new RedisMq(redis, NAMESPACE);
        try (var secondSource = MysqlTestSupport.source(p); var second = new MysqlHistory(secondMq, p, secondSource)) {
            second.start();
            await(() -> "RUNNING".equals(second.status().state()));
            assertFalse(second.status().collector());
            String queue = "cluster";
            for (int i = 0; i < 5; i++) { mq.send(queue, "one"); secondMq.send(queue, "two"); }
            await(() -> { var r = query(queue); return r.summary() != null && r.summary().counts().get("sent") == 10; });
            try (Connection c = source.connection(); PreparedStatement s = c.prepareStatement("SELECT COUNT(DISTINCT instance_id) FROM rmq_monitor_metrics WHERE queue_name='cluster' AND scope_id=(SELECT scope_id FROM rmq_monitor_lease WHERE source_id=?)")) {
                s.setString(1, OPTIONS.getSourceId()); try (ResultSet r = s.executeQuery()) { r.next(); assertEquals(2, r.getInt(1)); }
            }
        }
        assertTrue(history.status().collector());
    }
}
