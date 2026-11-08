package com.mk.mq;

public record MessageContext(String queue, String messageId, int deliveryCount, long createdAt) { }
