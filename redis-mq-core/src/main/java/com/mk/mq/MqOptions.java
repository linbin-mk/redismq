package com.mk.mq;

import java.time.Duration;

/** 容量包含待执行、处理中和死信；达到容量时拒绝新请求，不驱逐防重记录。 */
public record MqOptions(int maxQueueSize, int maxMessageBytes, Duration idempotencyRetention,
                        long maxQueueBytes, int maxDedupEntries, int maxRegisteredQueues) {
    public MqOptions {
        if (maxQueueSize < 1 || maxMessageBytes < 1 || maxMessageBytes > 65536) {
            throw new IllegalArgumentException("队列容量必须为正数，消息大小必须为 1 至 65536 字节");
        }
        if (maxQueueBytes < maxMessageBytes || maxQueueBytes > 1099511627776L || maxDedupEntries < 1 || maxRegisteredQueues < 1) {
            throw new IllegalArgumentException("正文总容量必须介于单条消息上限和 1 TiB 之间，防重容量必须为正数");
        }
        Checks.millis(idempotencyRetention, false, "消息编号保留时间");
    }
    /** 保留原三参数构造方式，使用默认的字节容量和防重容量。 */
    public MqOptions(int maxQueueSize, int maxMessageBytes, Duration idempotencyRetention) {
        this(maxQueueSize, maxMessageBytes, idempotencyRetention, 64L * 1024 * 1024, 200000, 1024);
    }
    public MqOptions(int maxQueueSize, int maxMessageBytes, Duration idempotencyRetention, long maxQueueBytes, int maxDedupEntries) {
        this(maxQueueSize, maxMessageBytes, idempotencyRetention, maxQueueBytes, maxDedupEntries, 1024);
    }
    public static MqOptions defaults() { return new MqOptions(100000, 65536, Duration.ofHours(24)); }
}
