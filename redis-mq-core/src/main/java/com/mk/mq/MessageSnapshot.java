package com.mk.mq;

/** 完成或取消后清理消息正文，去重标记仍按保留时间存在。 */
public record MessageSnapshot(String id, String payload, MessageState state, int deliveryCount,
                              int maxDeliveries, long createdAt, long availableAt, long leaseUntil,
                              String lastError) { }
