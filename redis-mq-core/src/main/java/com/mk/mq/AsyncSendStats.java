package com.mk.mq;

/** 本实例异步发送累计统计，与 Redis 队列容量及 MySQL 历史指标独立。
 * succeeded 包含 duplicate；failed 不包含超时和取消。耗时从受理到首次终态，不含调用线程的序列化。
 * outstanding 包含准备、等待、尚未结束的底层调用和结果通知；超时后 inFlight 仍可能大于零。
 */
public record AsyncSendStats(long accepted, long succeeded, long duplicates, long failed,
                             long rejected, long timedOut, long cancelled, int preparing,
                             int pending, int inFlight, int outstanding,
                             long totalDurationNanos, long maxDurationNanos, boolean closed) { }
