package com.mk.mq.dashboard.mysql;

import com.mk.mq.*;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import java.time.Duration;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/** 只存在于测试 classpath 的本机 E2E 宿主；不进入任何发布 JAR。 */
@SpringBootConfiguration
@EnableAutoConfiguration
public class HistoryE2EApplication {
    @Bean(destroyMethod = "shutdown") RedissonClient redis() {
        Config config = new Config().setThreads(4).setNettyThreads(4);
        config.useSingleServer().setAddress(System.getProperty("mq.redis", "redis://127.0.0.1:16379"))
                .setConnectionMinimumIdleSize(2).setConnectionPoolSize(16).setRetryAttempts(0).setTimeout(2000);
        return Redisson.create(config);
    }
    @Bean(initMethod = "start", destroyMethod = "close")
    @ConditionalOnProperty(name = "mq.e2e.traffic", havingValue = "true")
    FixtureTraffic fixtureTraffic(RedisMq mq) { return new FixtureTraffic(mq); }

    @RestController
    @ConditionalOnProperty(name = "mq.e2e.traffic", havingValue = "true")
    static class FixtureStatus {
        private final FixtureTraffic traffic;
        FixtureStatus(FixtureTraffic traffic) { this.traffic = traffic; }
        @GetMapping("/fixture/status") Map<String, Object> status() {
            return Map.of("synthetic", true, "accepted", traffic.accepted.get(), "succeeded", traffic.succeeded.get(), "rejected", traffic.rejected.get());
        }
    }
    static class FixtureTraffic implements AutoCloseable {
        final AtomicLong accepted = new AtomicLong(), succeeded = new AtomicLong(), rejected = new AtomicLong();
        final RedisMq mq;
        final ScheduledExecutorService producer = Executors.newSingleThreadScheduledExecutor(task -> { Thread t = new Thread(task, "history-e2e-traffic"); t.setDaemon(true); return t; });
        MqConsumer<String> consumer;
        FixtureTraffic(RedisMq mq) { this.mq = mq; }
        void start() {
            mq.registerQueue("analysis-demo");
            consumer = mq.consume("analysis-demo", String.class, (body, context) -> {
                Thread.sleep(10);
                long value = Long.parseLong(body);
                if (value % 17 == 0) { rejected.incrementAndGet(); throw new RejectMessageException("E2E 合成拒绝"); }
                if (value % 7 == 0 && context.deliveryCount() == 1) throw new IllegalStateException("E2E 合成重试");
                succeeded.incrementAndGet();
            }, new ConsumerOptions(2, Duration.ofSeconds(2), Duration.ofSeconds(10), Duration.ofMillis(50), Duration.ofSeconds(2)));
            producer.scheduleWithFixedDelay(() -> {
                if (accepted.get() >= 10000) return;
                try {
                    long value = accepted.get() + 1;
                    mq.send("analysis-demo", Long.toString(value), SendOptions.immediate().withRetry(3, Duration.ofMillis(100), Duration.ofMillis(200)));
                    accepted.incrementAndGet();
                } catch (RuntimeException ignored) { /* 仅测试流量，连接失败不向控制台输出凭据。 */ }
            }, 0, 250, TimeUnit.MILLISECONDS);
        }
        @Override public void close() { producer.shutdownNow(); if (consumer != null) consumer.close(); }
    }
    public static void main(String[] arguments) throws Exception {
        PersistenceProperties p = MysqlTestSupport.properties();
        p.setSourceId("local-e2e-synthetic");
        try (HistoryDataSource source = MysqlTestSupport.source(p)) { MysqlTestSupport.initialize(source); }
        SpringApplication application = new SpringApplication(HistoryE2EApplication.class);
        Map<String, Object> defaults = new HashMap<>();
        defaults.put("server.address", "127.0.0.1"); defaults.put("server.port", "18083");
        defaults.put("spring.application.name", "redis-mq-history-e2e");
        defaults.put("redis-mq.namespace", "history:e2e");
        defaults.put("redis-mq.dashboard.enabled", true);
        defaults.put("redis-mq.dashboard.persistence.enabled", true); defaults.put("redis-mq.dashboard.persistence.source-id", p.getSourceId());
        defaults.put("redis-mq.dashboard.persistence.datasource.url", p.getDatasource().getUrl());
        defaults.put("redis-mq.dashboard.persistence.datasource.username", p.getDatasource().getUsername());
        defaults.put("redis-mq.dashboard.persistence.datasource.password", p.getDatasource().getPassword());
        defaults.put("redis-mq.dashboard.persistence.flush-interval", "1s"); defaults.put("redis-mq.dashboard.persistence.sample-interval", "10s");
        defaults.put("redis-mq.dashboard.persistence.cleanup-interval", "1s"); defaults.put("redis-mq.dashboard.persistence.lease-duration", "10s");
        defaults.put("mq.e2e.traffic", true);
        application.setDefaultProperties(defaults);
        var context = application.run(arguments);
        seedSyntheticHistory(context.getBean(HistoryDataSource.class), context.getBean(PersistenceProperties.class), context.getBean(RedisMq.class));
    }
    private static void seedSyntheticHistory(HistoryDataSource source, PersistenceProperties p, RedisMq mq) throws Exception {
        var repository = new HistoryRepository(source, p, mq.namespace(), UUID.randomUUID().toString());
        HistoryRepository.Lease lease = null;
        for (int i = 0; i < 20 && lease == null; i++) {
            try { lease = repository.lease(mq.namespace()); } catch (SQLException e) { Thread.sleep(100); }
        }
        if (lease == null) throw new IllegalStateException("E2E 分析表未就绪");
        long now = lease.now();
        List<MinuteMetrics.Row> rows = new ArrayList<>();
        for (int i = 0; i < 72; i++) {
            long at = (now - (72L - i) * 60000) / 60000 * 60000;
            long[] data = new long[MetricLayout.COLUMNS.size()];
            data[MqMetricsListener.Event.SENT.ordinal()] = 80 + i % 19;
            data[MqMetricsListener.Event.ACKNOWLEDGED.ordinal()] = 75 + i % 19;
            data[MqMetricsListener.Event.HANDLER_FAILURE.ordinal()] = i % 3;
            data[MqMetricsListener.Event.RETRIED.ordinal()] = i % 3;
            data[MqMetricsListener.Event.PROCESSING_TIME.ordinal()] = 80;
            data[MetricLayout.EVENTS.length + 2] = 800000000;
            data[MetricLayout.EVENTS.length + 3] = 10000000;
            data[MetricLayout.EVENTS.length + 4 + 16 + MetricLayout.bin(10000000)] = 80;
            rows.add(new MinuteMetrics.Row("analysis-demo", at, i + 1, at + 60000, data, 80));
        }
        repository.metrics(rows);
        // 只为可视化回归构造历史快照，明确标识为合成数据，不伪装为真实吞吐测量。
        try (Connection c = source.connection(); PreparedStatement s = c.prepareStatement("INSERT INTO rmq_monitor_snapshot(scope_id,queue_name,bucket_ms,fence_token,`ready`,`delayed`,processing,expired_leases,dead,total,payload_bytes,dedup_entries) SELECT scope_id,'analysis-demo',?,0,?,10,2,0,3,?,1024,100 FROM rmq_monitor_lease WHERE source_id=? ON DUPLICATE KEY UPDATE `ready`=VALUES(`ready`)")) {
            for (int i = 0; i < 72; i++) {
                long at = (now - (72L - i) * 60000) / 60000 * 60000;
                s.setLong(1, at); s.setLong(2, 10 + i % 23); s.setLong(3, 25 + i % 23); s.setString(4, p.getSourceId()); s.addBatch();
            }
            s.executeBatch();
        }
        System.out.println("HISTORY_E2E_READY http://127.0.0.1:18083/redis-mq/dashboard/ synthetic=true");
    }
}
