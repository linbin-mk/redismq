package com.mk.mq;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.redisson.Redisson;
import org.redisson.config.Config;
import org.redisson.client.RedisException;
import org.redisson.client.codec.StringCodec;
import java.time.Duration;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

/** 必须指向专用 maxmemory=8mb、noeviction 的空 Redis，不能指向业务实例。 */
@EnabledIfSystemProperty(named = "mq.oom.redis", matches = ".+")
class RedisMqOomTest {
    @Test void redisMemoryLimitRejectsNewWritesButAllowsDuplicatesAndAcknowledgments() {
        Config config = new Config().setThreads(2).setNettyThreads(2);
        config.useSingleServer().setAddress(System.getProperty("mq.oom.redis"))
                .setConnectionMinimumIdleSize(1).setConnectionPoolSize(4).setRetryAttempts(0).setTimeout(2000);
        var redis = Redisson.create(config);
        var mq = new RedisMq(redis, "oom:" + UUID.randomUUID());
        String queue = "tasks";
        String body = "x".repeat(65534); // 包含 JSON 引号恰好 64 KiB。
        SendOptions first = SendOptions.immediate().withDeduplication("existing", Duration.ofHours(1));
        try {
            String firstId = mq.send(queue, body, first).messageId();
            Delivery processing = mq.receive(queue, 1, Duration.ofMinutes(1)).deliveries().get(0);
            int accepted = 1;
            boolean rejected = false;
            for (int i = 0; i < 512; i++) {
                try { mq.send(queue, body); accepted++; }
                catch (RedisException full) {
                    assertTrue(full.getMessage().contains("maxmemory"), full.getMessage());
                    rejected = true;
                    break;
                }
            }
            assertTrue(rejected, "Redis maxmemory 被脚本绕过，或测试实例未设置 8 MiB 限制");
            assertEquals(accepted, mq.stats(queue).total());
            assertEquals((long) accepted * 65536, mq.stats(queue).payloadBytes());
            assertEquals(accepted, redis.getMap(mq.storageKeys(queue).get(7), StringCodec.INSTANCE).size());
            assertEquals(firstId, mq.send(queue, body, first).messageId());
            // 64 KiB 正文被拒绝后，短防重别名仍可能容纳；连接缓冲释放也会改变即时余量。
            // 两种结果都合法，但别名绝不能创建第二条消息或写入正文。
            try {
                SendResult alias = mq.send(queue, body, first.withMessageId(UUID.randomUUID().toString()));
                assertTrue(alias.duplicate());
                assertEquals(firstId, alias.messageId());
            } catch (RedisException full) { assertTrue(full.getMessage().contains("maxmemory"), full.getMessage()); }
            assertEquals(accepted, mq.stats(queue).total());
            assertEquals((long) accepted * 65536, mq.stats(queue).payloadBytes());
            Delivery draining = mq.receive(queue, 1, Duration.ofMinutes(1)).deliveries().get(0);
            assertTrue(mq.acknowledge(queue, processing), "OOM 状态下仍须能够确认并释放正文");
            assertTrue(mq.acknowledge(queue, draining), "OOM 状态下已有积压仍须能够继续领取并释放空间");
            assertEquals(accepted - 2, mq.stats(queue).total());
            assertEquals((long) (accepted - 2) * 65536, mq.stats(queue).payloadBytes());
            System.out.println("MQ_OOM accepted=" + accepted + " newSendRejected=true duplicateAllowed=true ackAllowed=true");
        } finally {
            try {
                redis.getKeys().delete(mq.storageKeys(queue).toArray(String[]::new));
                redis.getKeys().delete(mq.directoryStorageKey());
            } finally { redis.shutdown(); }
        }
    }
}
