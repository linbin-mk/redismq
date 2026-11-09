package com.mk.mq;

/** 可选分析钩子：实现只能做有界内存操作，不能在调用线程执行 I/O。统计不构成业务审计。 */
@FunctionalInterface
public interface MqMetricsListener {
    enum Event {
        SENT, DUPLICATE, RECEIVED, ACKNOWLEDGED, RETRIED, DEAD, EXPIRED,
        HANDLER_SUCCESS, HANDLER_FAILURE, PROCESSING_TIMEOUT, CLAIM_LATENCY, PROCESSING_TIME
    }

    /** count 为事件次数；仅耗时事件使用 durationNanos。 */
    void record(String queue, Event event, long count, long durationNanos);
}
