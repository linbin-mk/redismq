package com.mk.mq;

/** 处理中包含已过期处理权；字节数为已迁移正文的 UTF-8 大小，不含 Redis 数据结构开销。 */
public record QueueStats(long ready, long delayed, long processing, long expiredLeases, long dead, long total,
                         long payloadBytes, long dedupEntries) {
    public QueueStats(long ready, long delayed, long processing, long expiredLeases, long dead, long total) {
        this(ready, delayed, processing, expiredLeases, dead, total, 0, 0);
    }
}
