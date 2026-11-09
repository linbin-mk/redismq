package com.mk.mq.dashboard.mysql;

import com.mk.mq.MqMetricsListener.Event;
import com.mk.mq.dashboard.DashboardHistory;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

final class MetricLayout {
    private MetricLayout() { }
    static final Event[] EVENTS = Event.values();
    static final long[] BOUNDS_MS = {1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000, 300000, Long.MAX_VALUE};
    static final List<String> COLUMNS;
    static {
        List<String> columns = new ArrayList<>();
        for (Event event : EVENTS) columns.add(event.name().toLowerCase(Locale.ROOT));
        columns.addAll(List.of("claim_nanos", "claim_max_nanos", "processing_nanos", "processing_max_nanos"));
        for (String kind : List.of("claim", "processing")) for (int i = 0; i < BOUNDS_MS.length; i++) columns.add(kind + "_b" + i);
        COLUMNS = List.copyOf(columns);
    }
    static int bin(long nanos) {
        double millis = nanos / 1000000.0;
        for (int i = 0; i < BOUNDS_MS.length; i++) if (millis <= BOUNDS_MS[i]) return i;
        return BOUNDS_MS.length - 1;
    }
    static DashboardHistory.Timing timing(long[] data, boolean processing) {
        int offset = EVENTS.length;
        long count = data[processing ? Event.PROCESSING_TIME.ordinal() : Event.CLAIM_LATENCY.ordinal()];
        long sum = data[offset + (processing ? 2 : 0)];
        long max = data[offset + (processing ? 3 : 1)];
        int bins = offset + 4 + (processing ? BOUNDS_MS.length : 0);
        long threshold = Math.max(1, (long) Math.ceil(count * .95));
        long running = 0;
        double p95 = 0;
        for (int i = 0; i < BOUNDS_MS.length && count > 0; i++) {
            running = running > Long.MAX_VALUE - data[bins + i] ? Long.MAX_VALUE : running + data[bins + i];
            if (running >= threshold) { p95 = Math.min(max / 1000000.0, BOUNDS_MS[i]); break; }
        }
        return new DashboardHistory.Timing(count, count == 0 ? 0 : sum / (double) count / 1000000,
                max / 1000000.0, p95);
    }
}
