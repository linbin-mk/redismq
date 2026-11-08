package com.mk.mq;

/** 处理中数量包含已过期但尚未恢复的处理权，过期数量单独返回。 */
public record QueueStats(long ready, long delayed, long processing, long expiredLeases, long dead, long total) { }
