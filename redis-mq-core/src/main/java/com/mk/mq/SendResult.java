package com.mk.mq;

/** Redis 已确认接受；duplicate 为 true 表示命中已有消息的防重记录，不表示新增一条消息。 */
public record SendResult(String messageId, boolean duplicate) { }
