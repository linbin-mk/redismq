package com.mk.mq.dashboard.mysql;

import com.mk.mq.MqMetricsListener;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

/** 消息线程仅做有界内存累计，不等待数据库，也不排队保存逐条事件。 */
final class MinuteMetrics implements MqMetricsListener {
    record Key(String queue, long minute) { }
    record Row(String queue, long minute, long sequence, long observedUntil, long[] data, long observations) { }
    private final ConcurrentHashMap<Key, Bucket> buckets = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> admitted = new ConcurrentHashMap<>();
    private final AtomicInteger admittedCount = new AtomicInteger();
    private final AtomicInteger size = new AtomicInteger();
    private final AtomicLong sequence = new AtomicLong();
    private final LongAdder dropped = new LongAdder();
    private final int maxQueues;
    private final int pendingMinutes;
    private final int capacity;
    private final Set<String> filter;
    private final LongSupplier clock;
    MinuteMetrics(PersistenceProperties properties, LongSupplier clock) {
        maxQueues = properties.getMaxQueues();
        pendingMinutes = properties.getPendingMinutes();
        capacity = maxQueues * pendingMinutes;
        filter = Set.copyOf(properties.getQueues());
        this.clock = clock;
    }
    @Override public void record(String queue, Event event, long count, long nanos) {
        if (count <= 0 || nanos < 0 || (!filter.isEmpty() && !filter.contains(queue))) return;
        if (admitted.computeIfAbsent(queue, name -> {
            if (admittedCount.incrementAndGet() > maxQueues) { admittedCount.decrementAndGet(); return null; }
            return true;
        }) == null) { dropped.add(count); return; }
        long minute = Math.max(0, clock.getAsLong()) / 60000 * 60000;
        Key key = new Key(queue, minute);
        Bucket bucket = buckets.get(key);
        if (bucket == null) {
            bucket = buckets.computeIfAbsent(key, ignored -> {
                if (size.incrementAndGet() > capacity) { size.decrementAndGet(); return null; }
                return new Bucket();
            });
            if (bucket == null) { dropped.add(count); return; }
        }
        bucket.values.addAndGet(event.ordinal(), count);
        bucket.observations.add(count);
        if (event == Event.CLAIM_LATENCY || event == Event.PROCESSING_TIME) {
            int offset = MetricLayout.EVENTS.length + (event == Event.PROCESSING_TIME ? 2 : 0);
            Bucket.addSaturated(bucket.values, offset, nanos);
            bucket.values.accumulateAndGet(offset + 1, nanos, Math::max);
            int bin = MetricLayout.EVENTS.length + 4 + (event == Event.PROCESSING_TIME ? MetricLayout.BOUNDS_MS.length : 0);
            bucket.values.addAndGet(bin + MetricLayout.bin(nanos), count);
        }
    }
    void prune(long now) {
        long earliest = now / 60000 * 60000 - (pendingMinutes - 1L) * 60000;
        buckets.forEach((key, bucket) -> {
            if (key.minute < earliest || key.minute > now + 60000) {
                if (buckets.remove(key, bucket)) {
                    size.decrementAndGet();
                    dropped.add(Math.max(0, bucket.observations.sum() - bucket.persistedObservations));
                }
            }
        });
    }
    List<Row> snapshot(long now) {
        prune(now);
        List<Row> result = new ArrayList<>();
        buckets.forEach((key, bucket) -> {
                long[] data = new long[MetricLayout.COLUMNS.size()];
                for (int i = 0; i < data.length; i++) data[i] = bucket.values.get(i);
                result.add(new Row(key.queue, key.minute, sequence.incrementAndGet(), Math.min(now, key.minute + 60000), data, bucket.observations.sum()));
        });
        return result;
    }
    void persisted(List<Row> rows) {
        for (Row row : rows) {
            Bucket bucket = buckets.get(new Key(row.queue, row.minute));
            if (bucket != null) bucket.persistedObservations = Math.max(bucket.persistedObservations, row.observations);
        }
    }
    int size() { return size.get(); }
    long dropped() { return dropped.sum(); }
    private static final class Bucket {
        final AtomicLongArray values = new AtomicLongArray(MetricLayout.COLUMNS.size());
        final LongAdder observations = new LongAdder();
        volatile long persistedObservations;
        static void addSaturated(AtomicLongArray values, int index, long value) {
            values.accumulateAndGet(index, value, (left, right) -> left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right);
        }
    }
}
