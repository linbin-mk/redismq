package com.mk.mq.dashboard;

import com.mk.mq.QueueStats;
import java.util.List;
import java.util.Map;

/** 可选历史查询接口。基础看板不依赖 JDBC，扩展实现不得参与消息投递事务。 */
public interface DashboardHistory {
    Status status();
    Result query(String queue, Long from, Long to, int points);

    /** 历史查询的可读错误，不误报为 Redis 故障，也不暴露底层连接信息。 */
    final class UnavailableException extends RuntimeException {
        public UnavailableException(String message) { super(message); }
    }
    final class QueryLimitException extends IllegalArgumentException {
        public QueryLimitException() { super("查询记录过多，请缩短历史时间范围"); }
    }

    record Status(boolean enabled, String state, String sourceId, String namespace, long retentionMillis,
                  long sampleIntervalMillis, boolean collector, long lastWriteAt, long lastSampleAt,
                  long lastCleanupAt, int pendingBuckets, long droppedEvents, long writeFailures,
                  int skippedQueues, long cleanupRows, String error) {
        public static Status disabled() {
            return new Status(false, "DISABLED", "", "", 0, 0, false, 0, 0, 0, 0, 0, 0, 0, 0, null);
        }
    }
    record Timing(long count, double averageMillis, double maxMillis, double p95Millis) { }
    record Point(long at, QueueStats peak, Map<String, Long> counts, Timing processing, Timing claim,
                 boolean snapshotPresent, boolean metricsPresent, boolean incomplete) { }
    record Summary(Map<String, Long> counts, Timing processing, Timing claim, boolean incomplete) { }
    record Result(String sourceId, String namespace, String queue, long from, long to, long resolutionMillis,
                  long retentionMillis, String gaugeAggregation, Summary summary, List<Point> points) {
        public Result { points = List.copyOf(points); }
    }
}
