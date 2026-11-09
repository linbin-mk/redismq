package com.mk.mq.dashboard.mysql;

import com.mk.mq.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperties;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.redisson.Redisson;
import org.redisson.config.Config;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfSystemProperties({@EnabledIfSystemProperty(named = "mq.redis", matches = ".+"),
        @EnabledIfSystemProperty(named = "mq.mysql", matches = ".+"), @EnabledIfSystemProperty(named = "mq.load", matches = "true")})
class MysqlHistoryLoadTest {
    @Test void enabledHistoryKeepsConcurrentMqFlowAndPersistsAggregateCounts() throws Exception {
        Config config = new Config().setThreads(4).setNettyThreads(4);
        config.useSingleServer().setAddress(System.getProperty("mq.redis")).setConnectionMinimumIdleSize(2).setConnectionPoolSize(32);
        var redis = Redisson.create(config);
        var mq = new RedisMq(redis, "history-load:" + UUID.randomUUID());
        var options = MysqlTestSupport.properties();
        ExecutorService producers = Executors.newFixedThreadPool(8);
        Set<String> handled = ConcurrentHashMap.newKeySet();
        String body = "x".repeat(1024);
        try (var source = MysqlTestSupport.source(options)) {
            MysqlTestSupport.initialize(source);
            try (var history = new MysqlHistory(mq, options, source)) {
                history.start(); HistoryWebE2ETest.await(() -> "RUNNING".equals(history.status().state()));
                long started = System.nanoTime();
                try (var consumer = mq.consume("jobs", String.class, (value, context) -> {
                    assertEquals(body, value); assertTrue(handled.add(context.messageId()));
                }, new ConsumerOptions(8, Duration.ofSeconds(3), Duration.ofSeconds(30), Duration.ofMillis(20), Duration.ofSeconds(3)))) {
                    List<Callable<Void>> tasks = new ArrayList<>();
                    for (int i = 0; i < 8; i++) {
                        int index = i;
                        tasks.add(() -> { for (int j = index; j < 4000; j += 8) mq.send("jobs", body); return null; });
                    }
                    for (var future : producers.invokeAll(tasks)) future.get();
                    HistoryWebE2ETest.await(() -> mq.stats("jobs").total() == 0);
                    double seconds = (System.nanoTime() - started) / 1000000000.0;
                    assertEquals(4000, handled.size());
                    HistoryWebE2ETest.await(() -> {
                        var data = history.query("jobs", null, null, 100);
                        return data.summary() != null && data.summary().counts().get("acknowledged") == 4000;
                    });
                    var data = history.query("jobs", null, null, 100);
                    assertEquals(4000, data.summary().counts().get("sent"));
                    assertEquals(4000, data.summary().processing().count());
                    assertTrue(history.status().pendingBuckets() <= options.getMaxQueues() * options.getPendingMinutes());
                    assertEquals(0, history.status().droppedEvents());
                    System.out.printf(Locale.ROOT, "MYSQL_HISTORY_LOAD messages=4000 seconds=%.3f messagesPerSecond=%.1f dropped=%d%n", seconds, 4000 / seconds, history.status().droppedEvents());
                }
            } finally { MysqlTestSupport.removeScope(source, options.getSourceId()); }
        } finally {
            producers.shutdownNow();
            redis.getKeys().delete(mq.storageKeys("jobs").toArray(String[]::new)); redis.getKeys().delete(mq.directoryStorageKey()); redis.shutdown();
        }
    }
}
