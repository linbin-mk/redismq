package com.mk.mq;

import java.time.Duration;

/** 容量包含待执行、处理中和死信；去重记录采用有界批次清理。 */
public record MqOptions(int maxQueueSize, int maxMessageBytes, Duration idempotencyRetention) {
    public MqOptions {
        if (maxQueueSize < 1 || maxMessageBytes < 1) throw new IllegalArgumentException("队列容量和消息大小必须为正数");
        Checks.millis(idempotencyRetention, false, "消息编号保留时间");
    }
    public static MqOptions defaults() { return new MqOptions(100000, 65536, Duration.ofHours(24)); }
}
