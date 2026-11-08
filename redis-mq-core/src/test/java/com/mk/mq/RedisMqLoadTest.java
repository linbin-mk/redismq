package com.mk.mq;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperties;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

/** 显式开启的端到端负载验证，包含 JSON 编码、通知、并发业务执行和 ACK。 */
@EnabledIfSystemProperties({@EnabledIfSystemProperty(named = "mq.redis", matches = ".+"),
        @EnabledIfSystemProperty(named = "mq.load", matches = "true")})
class RedisMqLoadTest {
    @Test void concurrentEndToEndLoadRemainsBounded() throws Exception {
        Config config = new Config().setThreads(4).setNettyThreads(4);
        config.useSingleServer().setAddress(System.getProperty("mq.redis"))
                .setConnectionMinimumIdleSize(2).setConnectionPoolSize(32).setRetryAttempts(0).setTimeout(3000);
        RedissonClient redis = Redisson.create(config);
        RedisMq mq = new RedisMq(redis, "load:" + UUID.randomUUID());
        String queue = "tasks";
        int total = 4000;
        String payload = "x".repeat(1024);
        Set<String> received = ConcurrentHashMap.newKeySet();
        AtomicInteger duplicates = new AtomicInteger();
        ExecutorService producers = Executors.newFixedThreadPool(8);
        long started = System.nanoTime();
        try (var consumer = mq.consume(queue, String.class, (body, context) -> {
            assertEquals(payload, body);
            if (!received.add(context.messageId())) duplicates.incrementAndGet();
        }, new ConsumerOptions(8, Duration.ofSeconds(3), Duration.ofSeconds(30), Duration.ofMillis(20), Duration.ofSeconds(5)))) {
            List<Callable<Void>> tasks = new ArrayList<>();
            for (int producer = 0; producer < 8; producer++) {
                int index = producer;
                tasks.add(() -> {
                    for (int i = index; i < total; i += 8) {
                        mq.send(queue, payload, SendOptions.immediate().withMessageId("request-" + i));
                    }
                    return null;
                });
            }
            for (var result : producers.invokeAll(tasks)) result.get();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (mq.stats(queue).total() != 0) {
                assertTrue(System.nanoTime() < deadline, "负载验证超时");
                Thread.sleep(10);
            }
            double seconds = (System.nanoTime() - started) / 1e9;
            assertEquals(total, received.size());
            assertEquals(0, duplicates.get());
            assertEquals(0, mq.stats(queue).payloadBytes());
            assertEquals(total, mq.stats(queue).dedupEntries());
            // 不只检查 Redis；防止高速回调取消的定时任务滞留 JVM 调度队列。
            for (String name : List.of("renewal", "deadlines")) {
                Field field = MqConsumer.class.getDeclaredField(name);
                field.setAccessible(true);
                var executor = (ScheduledThreadPoolExecutor) field.get(consumer);
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
                while (!executor.getQueue().isEmpty() && System.nanoTime() < until) Thread.sleep(5);
                assertTrue(executor.getQueue().isEmpty(), name + " 残留了已取消任务");
            }
            System.out.printf(Locale.ROOT, "MQ_LOAD messages=%d seconds=%.3f messagesPerSecond=%.1f duplicates=%d payloadBytes=%d%n",
                    total, seconds, total / seconds, duplicates.get(), mq.stats(queue).payloadBytes());
        } finally {
            producers.shutdownNow();
            try {
                redis.getKeys().delete(mq.storageKeys(queue).toArray(String[]::new));
                redis.getKeys().delete(mq.directoryStorageKey());
            } finally { redis.shutdown(); }
        }
    }
}
