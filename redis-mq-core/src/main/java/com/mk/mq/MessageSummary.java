package com.mk.mq;

/** 消息列表元数据，不包含业务正文和领取凭证。 */
public record MessageSummary(String id, MessageState state, int deliveryCount, int maxDeliveries,
                             long createdAt, long availableAt, long leaseUntil, String lastError, boolean ready) { }
