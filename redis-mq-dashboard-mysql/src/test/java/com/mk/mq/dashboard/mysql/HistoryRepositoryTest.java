package com.mk.mq.dashboard.mysql;

import com.mk.mq.MqMetricsListener.Event;
import com.mk.mq.QueueStats;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfSystemProperty(named = "mq.mysql", matches = ".+")
class HistoryRepositoryTest {
    private PersistenceProperties properties;
    private HistoryDataSource source;
    private HistoryRepository repo;
    private HistoryRepository.Lease lease;
    @BeforeEach void setup() throws Exception {
        properties = MysqlTestSupport.properties();
        source = MysqlTestSupport.source(properties);
        MysqlTestSupport.initialize(source);
        repo = new HistoryRepository(source, properties, "test:prod", UUID.randomUUID().toString());
        lease = repo.lease("test:prod");
    }
    @AfterEach void close() throws Exception {
        if (source != null) { try { MysqlTestSupport.removeScope(source, properties.getSourceId()); } finally { source.close(); } }
    }
    private MinuteMetrics.Row row(long at, long seq, long count) {
        long[] data = new long[MetricLayout.COLUMNS.size()]; data[Event.SENT.ordinal()] = count;
        return new MinuteMetrics.Row("jobs", at, seq, at + 60000, data, count);
    }
    @Test void cumulativeUpsertsIgnoreRetriesAndOutOfOrderResults() throws Exception {
        long at = lease.now() / 60000 * 60000;
        repo.metrics(List.of(row(at, 2, 20)));
        repo.metrics(List.of(row(at, 2, 20), row(at, 1, 5)));
        var result = repo.query("test:prod", "jobs", at, at + 60000, 100);
        assertEquals(20, result.points().stream().filter(p -> p.counts() != null).mapToLong(p -> p.counts().get("sent")).sum());
        assertTrue(result.points().stream().anyMatch(p -> p.metricsPresent() && !p.snapshotPresent()));
    }
    @Test void twoInstancesSumCountersButOnlyLeaseOwnerCanWriteSnapshots() throws Exception {
        var second = new HistoryRepository(source, properties, "test:prod", UUID.randomUUID().toString());
        assertFalse(second.lease("test:prod").owner());
        long at = lease.now() / 60000 * 60000;
        repo.metrics(List.of(row(at, 1, 7))); second.metrics(List.of(row(at, 1, 3)));
        var sample = new HistoryRepository.Sample("jobs", at, new QueueStats(1, 2, 3, 0, 4, 10, 100, 20));
        repo.snapshots(List.of(sample), lease.token());
        assertThrows(SQLException.class, () -> second.snapshots(List.of(sample), lease.token()));
        var result = repo.query("test:prod", "jobs", at, at + 60000, 100);
        assertEquals(10, result.points().stream().filter(p -> p.counts() != null).mapToLong(p -> p.counts().get("sent")).sum());
        repo.release();
        var next = second.lease("test:prod"); assertTrue(next.owner()); assertTrue(next.token() > lease.token());
        assertThrows(SQLException.class, () -> repo.snapshots(List.of(sample), lease.token()));
        second.snapshots(List.of(sample), next.token());
    }
    @Test void retentionFiltersImmediatelyAndCleanupIsScopedAndRepeatable() throws Exception {
        long expired = lease.now() - 6L * 86400000;
        var stats = new QueueStats(1, 0, 0, 0, 0, 1);
        repo.snapshots(List.of(new HistoryRepository.Sample("jobs", expired, stats)), lease.token());
        // 模拟升级前留下的过期指标，正常写入路径会拒绝写入保留窗口外的数据。
        try (Connection c = source.connection(); PreparedStatement s = c.prepareStatement("INSERT INTO rmq_monitor_metrics(scope_id,queue_name,bucket_ms,instance_id,sequence_no,observed_until_ms,sent) SELECT scope_id,'jobs',?,'fixture',1,?,1 FROM rmq_monitor_lease WHERE source_id=?")) {
            s.setLong(1, expired); s.setLong(2, expired + 60000); s.setString(3, properties.getSourceId()); s.executeUpdate();
        }
        var otherProperties = MysqlTestSupport.properties();
        var other = new HistoryRepository(source, otherProperties, "test:prod", UUID.randomUUID().toString());
        var otherLease = other.lease("test:prod");
        try {
            other.snapshots(List.of(new HistoryRepository.Sample("jobs", expired, stats)), otherLease.token());
            assertTrue(repo.query("test:prod", "jobs", expired, lease.now(), 100).points().stream().noneMatch(p -> p.snapshotPresent() || p.metricsPresent()));
            assertEquals(2, repo.cleanup(lease.token()).deleted());
            assertEquals(0, repo.cleanup(lease.token()).deleted());
            assertEquals(1, other.cleanup(otherLease.token()).deleted());
        } finally { MysqlTestSupport.removeScope(source, otherProperties.getSourceId()); }
    }
    @Test void namesAreCaseSensitiveAndQueriesHaveHardPointLimits() throws Exception {
        long at = lease.now() / 60000 * 60000;
        repo.snapshots(List.of(new HistoryRepository.Sample("Jobs", at, new QueueStats(5, 0, 0, 0, 0, 5)),
                new HistoryRepository.Sample("jobs", at, new QueueStats(2, 0, 0, 0, 0, 2))), lease.token());
        var upper = repo.query("test:prod", "Jobs", at, at + 60000, 100);
        assertEquals(5, upper.points().stream().filter(p -> p.peak() != null).findFirst().orElseThrow().peak().ready());
        var result = repo.query("test:prod", "jobs", lease.now() - 5L * 86400000, null, 17);
        assertTrue(result.points().size() <= 17);
    }
    @Test void borrowedConnectionsKeepTheirOriginalTransactionAndReadOnlyState() throws Exception {
        try (Connection real = source.connection()) {
            javax.sql.DataSource borrowed = (javax.sql.DataSource) java.lang.reflect.Proxy.newProxyInstance(javax.sql.DataSource.class.getClassLoader(),
                    new Class<?>[]{javax.sql.DataSource.class}, (proxy, method, args) -> {
                        if (!method.getName().equals("getConnection")) throw new AssertionError(method.getName());
                        return java.lang.reflect.Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class}, (p, m, a) -> {
                            if (m.getName().equals("close")) return null;
                            try { return m.invoke(real, a); } catch (java.lang.reflect.InvocationTargetException e) { throw e.getCause(); }
                        });
                    });
            var options = MysqlTestSupport.properties(); options.setSourceId(properties.getSourceId());
            options.getDatasource().setUrl(null); options.getDatasource().setBeanName("borrowed");
            var beans = new org.springframework.beans.factory.support.StaticListableBeanFactory(); beans.addBean("borrowed", borrowed);
            try (var internal = new HistoryDataSource(options, beans)) {
                var repository = new HistoryRepository(internal, options, "test:prod", UUID.randomUUID().toString());
                long at = lease.now() / 60000 * 60000;
                repository.metrics(List.of(row(at, 1, 1)));
                assertTrue(real.getAutoCommit());
                repository.query("test:prod", "jobs", null, null, 10);
                assertFalse(real.isReadOnly());
                assertTrue(real.getAutoCommit());
                // 兼容宿主连接池默认关闭自动提交，租约和释放也必须真正提交。
                repo.release();
                real.setAutoCommit(false);
                assertTrue(repository.lease("test:prod").owner());
                assertFalse(real.getAutoCommit());
                assertFalse(repo.lease("test:prod").owner());
                repository.metrics(List.of(row(at, 2, 2)));
                assertFalse(real.getAutoCommit());
                repository.release();
                assertFalse(real.getAutoCommit());
                assertTrue(repo.lease("test:prod").owner());
                real.setAutoCommit(true);
            }
            assertFalse(real.isClosed());
        }
    }
    @Test void partialQueryStartAlignsToExistingMinuteInsteadOfCreatingFalseGaps() throws Exception {
        long at = lease.now() / 60000 * 60000 - 120000;
        repo.metrics(List.of(row(at, 1, 7)));
        var result = repo.query("test:prod", "jobs", at + 30000, at + 60000, 10);
        assertEquals(at, result.from());
        assertEquals(7, result.summary().counts().get("sent"));
        assertTrue(result.points().get(0).metricsPresent());
    }
    @Test void oversizedHistoryCannotBypassTheIndexScanBudget() throws Exception {
        byte[] scope;
        try (Connection c = source.connection(); PreparedStatement s = c.prepareStatement("SELECT scope_id FROM rmq_monitor_lease WHERE source_id=?")) {
            s.setString(1, properties.getSourceId()); try (ResultSet r = s.executeQuery()) { r.next(); scope = r.getBytes(1); }
        }
        long at = lease.now() / 60000 * 60000;
        try (Connection c = source.connection(); PreparedStatement s = c.prepareStatement("INSERT INTO rmq_monitor_metrics(scope_id,queue_name,bucket_ms,instance_id,sequence_no,observed_until_ms) VALUES(?,?,?,?,1,?)")) {
            for (int i = 0; i <= 100000; i++) {
                s.setBytes(1, scope); s.setString(2, "dense"); s.setLong(3, at); s.setString(4, "fixture-" + i); s.setLong(5, at + 60000); s.addBatch();
                if (i % 1000 == 999 || i == 100000) s.executeBatch();
            }
        }
        assertThrows(com.mk.mq.dashboard.DashboardHistory.QueryLimitException.class,
                () -> repo.query("test:prod", "dense", at, at + 60000, 100));
    }

    @Test void activeConfigurationMismatchCannotChangeRetentionButExpiredLeaseCan() throws Exception {
        var changed = MysqlTestSupport.properties();
        changed.setSourceId(properties.getSourceId()); changed.setRetention(java.time.Duration.ofDays(1));
        var second = new HistoryRepository(source, changed, "test:prod", UUID.randomUUID().toString());
        assertThrows(SQLException.class, () -> second.lease("test:prod"));
        repo.release();
        assertTrue(second.lease("test:prod").owner());
        assertThrows(SQLException.class, () -> repo.metrics(List.of(row(lease.now() / 60000 * 60000, 1, 1))));
    }
}
