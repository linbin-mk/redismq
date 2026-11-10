package com.mk.mq;

import java.time.Duration;

/** 每个 RedisMq 实例共享的异步发送限制；不改变同步发送或宿主 Redis 连接配置。 */
public record ProducerOptions(int concurrency, int maxPending, Duration sendTimeout, Duration shutdownTimeout) {
    public ProducerOptions {
        if (concurrency < 1 || concurrency > 256) throw new IllegalArgumentException("发送并发数必须为 1 至 256");
        if (maxPending < 1 || maxPending > 100000) throw new IllegalArgumentException("发送等待容量必须为 1 至 100000");
        Checks.millis(sendTimeout, false, "异步发送超时");
        Checks.millis(shutdownTimeout, true, "发送停机等待时间");
    }

    public static ProducerOptions defaults() {
        return new ProducerOptions(4, 256, Duration.ofSeconds(5), Duration.ofSeconds(30));
    }
}
