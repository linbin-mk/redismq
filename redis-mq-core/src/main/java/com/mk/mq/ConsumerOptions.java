package com.mk.mq;

import java.time.Duration;

public record ConsumerOptions(int concurrency, Duration visibility, Duration maxProcessingTime,
                              Duration pollInterval, Duration shutdownTimeout) {
    public ConsumerOptions {
        if (concurrency < 1 || concurrency > 256) throw new IllegalArgumentException("消费并发数必须为 1 至 256");
        if (Checks.millis(visibility, false, "处理权期限") < 300) throw new IllegalArgumentException("处理权期限不能低于 300 毫秒");
        Checks.millis(maxProcessingTime, false, "最长执行时间");
        Checks.millis(pollInterval, false, "兜底检查间隔");
        Checks.millis(shutdownTimeout, true, "停机等待时间");
    }
    public static ConsumerOptions defaults() {
        return new ConsumerOptions(4, Duration.ofSeconds(60), Duration.ofMinutes(5), Duration.ofMillis(500), Duration.ofSeconds(30));
    }
}
