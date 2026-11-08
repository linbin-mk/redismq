package com.mk.mq;

import java.time.Duration;
import java.util.UUID;

/** 同一发送请求重试时应复用本对象；新的业务事件应使用新的消息编号。 */
public record SendOptions(String messageId, Duration delay, int maxDeliveries, Duration retryDelay,
                          Duration maxRetryDelay, String deduplicationKey, Duration deduplicationWindow) {
    public SendOptions {
        Checks.name(messageId, "消息编号");
        Checks.millis(delay, true, "投递延迟");
        if (maxDeliveries < 1 || maxDeliveries > 1000) throw new IllegalArgumentException("最大投递次数必须为 1 至 1000");
        long base = Checks.millis(retryDelay, false, "重试延迟");
        if (Checks.millis(maxRetryDelay, false, "最大重试延迟") < base) throw new IllegalArgumentException("最大重试延迟不能小于初始延迟");
        if (deduplicationKey != null && (deduplicationKey.isBlank() || deduplicationKey.length() > 256)) {
            throw new IllegalArgumentException("业务去重键长度必须为 1 至 256");
        }
        Checks.millis(deduplicationWindow, false, "去重窗口");
    }
    public static SendOptions immediate() { return delayed(Duration.ZERO); }
    public static SendOptions delayed(Duration delay) {
        return new SendOptions(UUID.randomUUID().toString(), delay, 5, Duration.ofSeconds(5), Duration.ofMinutes(5), null, Duration.ofHours(24));
    }
    public SendOptions withMessageId(String id) {
        return new SendOptions(id, delay, maxDeliveries, retryDelay, maxRetryDelay, deduplicationKey, deduplicationWindow);
    }
    public SendOptions withDeduplication(String key, Duration window) {
        return new SendOptions(messageId, delay, maxDeliveries, retryDelay, maxRetryDelay, key, window);
    }
    public SendOptions withRetry(int deliveries, Duration base, Duration maximum) {
        return new SendOptions(messageId, delay, deliveries, base, maximum, deduplicationKey, deduplicationWindow);
    }
}
