package com.mk.mq.dashboard.mysql;

import com.mk.mq.QueueStats;
import com.mk.mq.dashboard.DashboardHistory;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/** 所有历史写入独立于消息事务；清理及查询始终限制 scope、时间范围和批次。 */
final class HistoryRepository {
    record Lease(boolean owner, long token, long until, long now) { }
    record Sample(String queue, long at, QueueStats stats) { }
    record Cleanup(long deleted, boolean behind) { }
    private static final List<String> GAUGES = List.of("ready", "delayed", "processing", "expired_leases", "dead", "total", "payload_bytes", "dedup_entries");
    private static final String NOW = "SELECT CAST(UNIX_TIMESTAMP(NOW(3))*1000 AS UNSIGNED)";
    private final HistoryDataSource source;
    private final PersistenceProperties properties;
    private final byte[] scope;
    private final byte[] configuration;
    private final String instance;
    private final String metricInsert;
    private final String snapshotInsert;

    HistoryRepository(HistoryDataSource source, PersistenceProperties properties, String namespace, String instance) {
        this.source = source;
        properties = properties.snapshot();
        this.properties = properties;
        this.instance = instance;
        scope = hash(properties.getSourceId() + "\0" + namespace);
        configuration = hash(properties.getRetention() + "|" + properties.getSampleInterval() + "|"
                + properties.getLeaseDuration() + "|" + properties.getMaxQueues() + "|" + properties.getQueues().stream().sorted().toList());
        metricInsert = insert("rmq_monitor_metrics", List.of("scope_id", "queue_name", "bucket_ms", "instance_id", "sequence_no", "observed_until_ms"),
                MetricLayout.COLUMNS, "sequence_no", false);
        snapshotInsert = insert("rmq_monitor_snapshot", List.of("scope_id", "queue_name", "bucket_ms", "fence_token"), GAUGES, "fence_token", true);
    }
    private static byte[] hash(String value) {
        try { return MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)); }
        catch (NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }
    private static String quoted(String name) { return "`" + name + "`"; }
    private static String insert(String table, List<String> keys, List<String> values, String sequence, boolean equal) {
        List<String> all = new ArrayList<>(keys);
        all.addAll(values);
        List<String> update = new ArrayList<>(values);
        if (!equal) update.add("observed_until_ms");
        String condition = "VALUES(" + quoted(sequence) + ")" + (equal ? ">=" : ">") + quoted(sequence);
        String setters = update.stream().map(name -> quoted(name) + "=IF(" + condition + ",VALUES(" + quoted(name) + ")," + quoted(name) + ")").collect(Collectors.joining(","));
        // 序号最后更新，避免 MySQL SET 从左到右求值导致其他列错误跳过。
        return "INSERT INTO " + table + " (" + all.stream().map(HistoryRepository::quoted).collect(Collectors.joining(",")) + ") VALUES ("
                + String.join(",", Collections.nCopies(all.size(), "?")) + ") ON DUPLICATE KEY UPDATE " + setters
                + "," + quoted(sequence) + "=GREATEST(" + quoted(sequence) + ",VALUES(" + quoted(sequence) + "))";
    }
    private static PreparedStatement prepare(Connection connection, String sql, Object... values) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(sql);
        try {
            statement.setQueryTimeout(1);
            for (int i = 0; i < values.length; i++) statement.setObject(i + 1, values[i]);
            return statement;
        } catch (SQLException | RuntimeException e) { statement.close(); throw e; }
    }
    static long now(Connection connection) throws SQLException {
        try (PreparedStatement statement = prepare(connection, NOW); ResultSet result = statement.executeQuery()) {
            result.next(); return result.getLong(1);
        }
    }
    Lease lease(String namespace) throws SQLException {
        try (Connection connection = source.connection()) {
            boolean autoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                long now = now(connection);
                try (PreparedStatement statement = prepare(connection,
                        "INSERT IGNORE INTO rmq_monitor_lease(scope_id,source_id,namespace,config_hash,owner_id) VALUES(?,?,?,?,?)",
                        scope, properties.getSourceId(), namespace, configuration, "")) { statement.executeUpdate(); }
                try (PreparedStatement statement = prepare(connection,
                        "UPDATE rmq_monitor_lease SET token=IF(owner_id=? AND lease_until_ms>?,token,token+1),owner_id=?,config_hash=?,lease_until_ms=? WHERE scope_id=? AND (lease_until_ms<=? OR (owner_id=? AND config_hash=?))",
                        instance, now, instance, configuration, now + properties.getLeaseDuration().toMillis(), scope, now, instance, configuration)) { statement.executeUpdate(); }
                try (PreparedStatement statement = prepare(connection,
                        "SELECT owner_id,token,lease_until_ms,config_hash,schema_version FROM rmq_monitor_lease WHERE scope_id=?", scope);
                     ResultSet result = statement.executeQuery()) {
                    if (!result.next() || result.getInt(5) != 1) throw new SQLException("MONITOR_SCHEMA_VERSION", "42000");
                    if (!Arrays.equals(configuration, result.getBytes(4))) throw new SQLException("MONITOR_CONFIG_MISMATCH", "45000");
                    Lease lease = new Lease(instance.equals(result.getString(1)), result.getLong(2), result.getLong(3), now);
                    connection.commit();
                    return lease;
                }
            } catch (SQLException | RuntimeException e) { rollback(connection, e); throw e; }
            finally { connection.setAutoCommit(autoCommit); }
        }
    }
    private void lockScope(Connection connection, Long token) throws SQLException {
        try (PreparedStatement statement = prepare(connection,
                "SELECT owner_id,token,lease_until_ms,config_hash FROM rmq_monitor_lease WHERE scope_id=?" + (token == null ? " FOR SHARE" : " FOR UPDATE"), scope);
             ResultSet result = statement.executeQuery()) {
            if (!result.next() || !Arrays.equals(configuration, result.getBytes(4))) throw new SQLException("MONITOR_CONFIG_MISMATCH", "45000");
            long until = result.getLong(3);
            if (token != null && (!instance.equals(result.getString(1)) || token != result.getLong(2))) throw new SQLException("MONITOR_LEASE_LOST", "40001");
            if (until <= now(connection)) throw new SQLException("MONITOR_LEASE_EXPIRED", "40001");
        }
    }
    void metrics(List<MinuteMetrics.Row> rows) throws SQLException {
        if (rows.isEmpty()) return;
        try (Connection connection = source.connection()) {
            boolean autoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                lockScope(connection, null);
                long now = now(connection);
                try (PreparedStatement statement = prepare(connection, metricInsert)) {
                    for (MinuteMetrics.Row row : rows) {
                        if (row.minute() < now - properties.getRetention().toMillis() || row.minute() > now + 60000) continue;
                        statement.setBytes(1, scope); statement.setString(2, row.queue()); statement.setLong(3, row.minute());
                        statement.setString(4, instance); statement.setLong(5, row.sequence()); statement.setLong(6, row.observedUntil());
                        for (int i = 0; i < row.data().length; i++) statement.setLong(7 + i, row.data()[i]);
                        statement.addBatch();
                    }
                    statement.executeBatch();
                }
                connection.commit();
            } catch (SQLException | RuntimeException e) { rollback(connection, e); throw e; }
            finally { connection.setAutoCommit(autoCommit); }
        }
    }
    void snapshots(List<Sample> samples, long token) throws SQLException {
        if (samples.isEmpty()) return;
        try (Connection connection = source.connection()) {
            boolean autoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                lockScope(connection, token);
                try (PreparedStatement statement = prepare(connection, snapshotInsert)) {
                    for (Sample sample : samples) {
                        statement.setBytes(1, scope); statement.setString(2, sample.queue()); statement.setLong(3, sample.at()); statement.setLong(4, token);
                        QueueStats value = sample.stats();
                        long[] data = {value.ready(), value.delayed(), value.processing(), value.expiredLeases(), value.dead(), value.total(), value.payloadBytes(), value.dedupEntries()};
                        for (int i = 0; i < data.length; i++) statement.setLong(5 + i, data[i]);
                        statement.addBatch();
                    }
                    statement.executeBatch();
                }
                connection.commit();
            } catch (SQLException | RuntimeException e) { rollback(connection, e); throw e; }
            finally { connection.setAutoCommit(autoCommit); }
        }
    }
    Cleanup cleanup(long token) throws SQLException {
        long started = System.nanoTime();
        long deleted = 0;
        boolean behind = false;
        for (int batch = 0; batch < 64 && System.nanoTime() - started < 2000000000L; batch++) {
            int affected = 0;
            try (Connection connection = source.connection()) {
                boolean autoCommit = connection.getAutoCommit();
                connection.setAutoCommit(false);
                try {
                    lockScope(connection, token);
                    long cutoff = now(connection) - properties.getRetention().toMillis();
                    for (String table : List.of("rmq_monitor_snapshot", "rmq_monitor_metrics")) {
                        try (PreparedStatement statement = prepare(connection,
                                "DELETE FROM " + table + " WHERE scope_id=? AND bucket_ms<? ORDER BY bucket_ms LIMIT ?",
                                scope, cutoff, properties.getCleanupBatchSize())) { affected += statement.executeUpdate(); }
                    }
                    connection.commit();
                } catch (SQLException | RuntimeException e) { rollback(connection, e); throw e; }
                finally { connection.setAutoCommit(autoCommit); }
            }
            deleted += affected;
            if (affected == 0) return new Cleanup(deleted, false);
            behind = true;
        }
        return new Cleanup(deleted, behind);
    }
    DashboardHistory.Result query(String namespace, String queue, Long requestedFrom, Long requestedTo, int limit) throws SQLException {
        try (Connection connection = source.connection()) {
            long now = now(connection);
            long to = requestedTo == null ? now : Math.min(now, requestedTo);
            long earliest = Math.max(0, now - properties.getRetention().toMillis());
            long from = Math.max(earliest, requestedFrom == null ? to - Math.min(3600000, properties.getRetention().toMillis()) : requestedFrom);
            // 指标以分钟存储；对齐查询起点，避免把已有的首个部分分钟伪装成采集缺口。
            // 保留边界向上取整，绝不重新暴露已到期的窗口。
            from = Math.max((earliest + 59999) / 60000 * 60000, from / 60000 * 60000);
            if (from >= to) throw new IllegalArgumentException("查询范围不在保留窗口内");
            long base = Math.max(60000, properties.getSampleInterval().toMillis());
            // 同时限制首尾不完整桶，保证实际返回点数不会超过请求上限。
            long resolution = Math.max(base, ((to - from + limit - 1) / limit + base - 1) / base * base);
            while ((to - 1) / resolution - from / resolution + 1 > limit) resolution += base;
            // LIMIT 只限制聚合后的点数；先用覆盖索引限定最多扫描十万条，避免大范围慢 SQL。
            for (String table : List.of("rmq_monitor_snapshot", "rmq_monitor_metrics")) {
                try (PreparedStatement statement = prepare(connection,
                        "SELECT bucket_ms FROM " + table + " FORCE INDEX(PRIMARY) WHERE scope_id=? AND queue_name=? AND bucket_ms>=? AND bucket_ms<? LIMIT 100000,1",
                        scope, queue, from, to); ResultSet result = statement.executeQuery()) {
                    if (result.next()) throw new DashboardHistory.QueryLimitException();
                }
            }
            TreeMap<Long, MutablePoint> points = new TreeMap<>();
            for (long at = from / resolution * resolution; at < to; at += resolution) points.put(at, new MutablePoint(at));
            String maxes = GAUGES.stream().map(name -> "MAX(" + quoted(name) + ")").collect(Collectors.joining(","));
            try (PreparedStatement statement = prepare(connection,
                    "SELECT FLOOR(bucket_ms/?)*? AS point_at," + maxes + " FROM rmq_monitor_snapshot FORCE INDEX(PRIMARY) WHERE scope_id=? AND queue_name=? AND bucket_ms>=? AND bucket_ms<? GROUP BY point_at ORDER BY point_at LIMIT ?",
                    resolution, resolution, scope, queue, from, to, limit);
                 ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    MutablePoint point = points.get(result.getLong(1));
                    if (point != null) point.peak = new QueueStats(result.getLong(2), result.getLong(3), result.getLong(4), result.getLong(5), result.getLong(6), result.getLong(7), result.getLong(8), result.getLong(9));
                }
            }
            String aggregates = MetricLayout.COLUMNS.stream().map(name ->
                    name.endsWith("max_nanos") ? "MAX(" + quoted(name) + ")" : "LEAST(SUM(" + quoted(name) + "),9223372036854775807)").collect(Collectors.joining(","));
            try (PreparedStatement statement = prepare(connection,
                    "SELECT FLOOR(bucket_ms/?)*? AS point_at," + aggregates + ",MAX(observed_until_ms < bucket_ms+60000) FROM rmq_monitor_metrics FORCE INDEX(PRIMARY) WHERE scope_id=? AND queue_name=? AND bucket_ms>=? AND bucket_ms<? GROUP BY point_at ORDER BY point_at LIMIT ?",
                    resolution, resolution, scope, queue, from, to, limit);
                 ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    MutablePoint point = points.get(result.getLong(1));
                    if (point == null) continue;
                    point.data = new long[MetricLayout.COLUMNS.size()];
                    for (int i = 0; i < point.data.length; i++) point.data[i] = result.getLong(i + 2);
                    point.incomplete = result.getBoolean(point.data.length + 2);
                }
            }
            long[] totals = new long[MetricLayout.COLUMNS.size()];
            boolean observed = false, incomplete = false;
            for (MutablePoint point : points.values()) {
                incomplete |= point.incomplete || (point.peak == null && point.data == null);
                if (point.data == null) continue;
                observed = true;
                for (int i = 0; i < totals.length; i++) {
                    long value = point.data[i];
                    totals[i] = MetricLayout.COLUMNS.get(i).endsWith("max_nanos") ? Math.max(totals[i], value)
                            : totals[i] > Long.MAX_VALUE - value ? Long.MAX_VALUE : totals[i] + value;
                }
            }
            Map<String, Long> counts = new LinkedHashMap<>();
            for (int i = 0; i < MetricLayout.EVENTS.length; i++) counts.put(MetricLayout.COLUMNS.get(i), totals[i]);
            DashboardHistory.Summary summary = observed ? new DashboardHistory.Summary(Collections.unmodifiableMap(counts),
                    MetricLayout.timing(totals, true), MetricLayout.timing(totals, false), incomplete) : null;
            return new DashboardHistory.Result(properties.getSourceId(), namespace, queue, from, to, resolution,
                    properties.getRetention().toMillis(), "MAX", summary, points.values().stream().map(MutablePoint::result).toList());
        }
    }
    void release() throws SQLException {
        try (Connection connection = source.connection()) {
            boolean autoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try (PreparedStatement statement = prepare(connection,
                    "UPDATE rmq_monitor_lease SET lease_until_ms=0 WHERE scope_id=? AND owner_id=?", scope, instance)) {
                statement.executeUpdate();
                connection.commit();
            } catch (SQLException | RuntimeException e) { rollback(connection, e); throw e; }
            finally { connection.setAutoCommit(autoCommit); }
        }
    }
    private static void rollback(Connection connection, Exception original) {
        try { connection.rollback(); } catch (SQLException rollback) { original.addSuppressed(rollback); }
    }
    private static final class MutablePoint {
        final long at;
        QueueStats peak;
        long[] data;
        boolean incomplete;
        MutablePoint(long at) { this.at = at; }
        DashboardHistory.Point result() {
            Map<String, Long> counts = null;
            if (data != null) {
                counts = new LinkedHashMap<>();
                for (int i = 0; i < MetricLayout.EVENTS.length; i++) counts.put(MetricLayout.COLUMNS.get(i), data[i]);
                counts = Collections.unmodifiableMap(counts);
            }
            return new DashboardHistory.Point(at, peak, counts, data == null ? null : MetricLayout.timing(data, true),
                    data == null ? null : MetricLayout.timing(data, false), peak != null, data != null, incomplete);
        }
    }
}
