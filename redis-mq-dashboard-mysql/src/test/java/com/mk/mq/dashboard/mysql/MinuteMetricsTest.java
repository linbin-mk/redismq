package com.mk.mq.dashboard.mysql;

import com.mk.mq.MqMetricsListener.Event;
import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.jupiter.api.Assertions.*;

class MinuteMetricsTest {
    private PersistenceProperties options(int queues, int minutes) {
        PersistenceProperties properties = new PersistenceProperties();
        properties.setMaxQueues(queues); properties.setPendingMinutes(minutes);
        return properties;
    }
    @Test void concurrentEventsAreCountedWithoutPerMessageBuffering() throws Exception {
        var metrics = new MinuteMetrics(options(1, 2), () -> 60000);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            List<Callable<Void>> tasks = new ArrayList<>();
            for (int i = 0; i < 8; i++) tasks.add(() -> {
                for (int j = 0; j < 1000; j++) metrics.record("jobs", Event.SENT, 1, 0);
                return null;
            });
            for (var future : executor.invokeAll(tasks)) future.get();
        } finally { executor.shutdownNow(); }
        assertEquals(8000, metrics.snapshot(60000).get(0).data()[Event.SENT.ordinal()]);
        assertEquals(1, metrics.size());
        assertEquals(0, metrics.dropped());
    }
    @Test void cardinalityAndOutageWindowsAreBounded() {
        AtomicLong clock = new AtomicLong(60000);
        var metrics = new MinuteMetrics(options(2, 2), clock::get);
        for (int i = 0; i < 1000; i++) metrics.record("queue-" + i, Event.SENT, 1, 0);
        assertEquals(2, metrics.size());
        assertEquals(998, metrics.dropped());
        for (int i = 0; i < 10; i++) {
            clock.addAndGet(60000);
            metrics.prune(clock.get());
            metrics.record("queue-0", Event.SENT, 1, 0);
            assertTrue(metrics.size() <= 4);
        }
        assertTrue(metrics.dropped() > 998);
        assertEquals(2, metrics.snapshot(clock.get()).size());
    }
    @Test void persistedWindowsAreNotReportedAsLostAndPercentilesMergeBins() {
        AtomicLong clock = new AtomicLong(60000);
        var metrics = new MinuteMetrics(options(1, 2), clock::get);
        for (int i = 0; i < 99; i++) metrics.record("q", Event.PROCESSING_TIME, 1, 5000000);
        metrics.record("q", Event.PROCESSING_TIME, 1, 1000000000);
        var rows = metrics.snapshot(clock.get());
        var timing = MetricLayout.timing(rows.get(0).data(), true);
        assertEquals(100, timing.count());
        assertEquals(5, timing.p95Millis());
        assertEquals(1000, timing.maxMillis());
        assertEquals(14.95, timing.averageMillis(), .001);
        metrics.persisted(rows);
        clock.set(300000);
        metrics.prune(clock.get());
        assertEquals(0, metrics.size());
        assertEquals(0, metrics.dropped());
    }
    @Test void validatedConfigurationIsFrozenAgainstExternalRebinding() {
        var p = options(2, 3); p.setSourceId("freeze");
        p.getDatasource().setUrl("jdbc:mysql://localhost/history"); p.getDatasource().setUsername("test");
        var copy = p.snapshot();
        p.setRetention(java.time.Duration.ofSeconds(-1)); p.setMaxQueues(Integer.MAX_VALUE);
        p.getDatasource().setUrl("jdbc:mysql://other/unexpected");
        assertEquals(java.time.Duration.ofDays(5), copy.getRetention());
        assertEquals(2, copy.getMaxQueues());
        assertEquals("jdbc:mysql://localhost/history", copy.getDatasource().getUrl());
    }
    @Test void jdbcUrlCannotDisableManagedNetworkTimeouts() {
        var p = options(2, 3); p.setSourceId("timeouts"); p.getDatasource().setUsername("test");
        p.getDatasource().setUrl("jdbc:mysql://localhost/history?socketTimeout=0");
        assertThrows(IllegalArgumentException.class, p::validate);
        p.getDatasource().setUrl("jdbc:mysql://localhost/history?connectTimeout=60000");
        assertThrows(IllegalArgumentException.class, p::validate);
        p.getDatasource().setUrl("jdbc:mysql://localhost/history?socketTimeout=1000");
        assertDoesNotThrow(p::validate);
    }
    @Test void rejectsUnboundedOrAmbiguousConfiguration() {
        var p = options(1025, 3);
        p.setSourceId("example"); p.getDatasource().setUrl("jdbc:mysql://localhost/history"); p.getDatasource().setUsername("test");
        assertThrows(IllegalArgumentException.class, p::validate);
        p.setMaxQueues(1); p.setQueues(List.of("a:b"));
        assertThrows(IllegalArgumentException.class, p::validate);
        p.setQueues(List.of("a")); p.setSampleInterval(java.time.Duration.ofMillis(100));
        assertThrows(IllegalArgumentException.class, p::validate);
    }
}
