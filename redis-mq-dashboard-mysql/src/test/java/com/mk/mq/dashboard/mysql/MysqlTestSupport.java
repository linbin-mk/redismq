package com.mk.mq.dashboard.mysql;

import org.springframework.beans.factory.support.StaticListableBeanFactory;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Duration;
import java.util.UUID;

final class MysqlTestSupport {
    private MysqlTestSupport() { }
    static PersistenceProperties properties() {
        PersistenceProperties p = new PersistenceProperties();
        p.setEnabled(true); p.setSourceId("test-" + UUID.randomUUID());
        p.setFlushInterval(Duration.ofSeconds(1)); p.setSampleInterval(Duration.ofSeconds(10));
        p.setCleanupInterval(Duration.ofSeconds(1)); p.setLeaseDuration(Duration.ofSeconds(10));
        p.getDatasource().setUrl(System.getProperty("mq.mysql"));
        p.getDatasource().setUsername(System.getenv().getOrDefault("MYSQL_USER", "rmq_history"));
        p.getDatasource().setPassword(System.getenv("MYSQL_PASSWORD"));
        return p;
    }
    static HistoryDataSource source(PersistenceProperties properties) {
        return new HistoryDataSource(properties, new StaticListableBeanFactory());
    }
    static void initialize(HistoryDataSource source) throws Exception {
        String sql;
        try (var input = MysqlTestSupport.class.getResourceAsStream("/com/mk/mq/dashboard/mysql/V1__monitor_history.sql")) {
            sql = new String(input.readAllBytes(), StandardCharsets.UTF_8).lines().filter(line -> !line.stripLeading().startsWith("--")).collect(java.util.stream.Collectors.joining("\n"));
        }
        Connection ready = null;
        for (int attempt = 0; attempt < 20; attempt++) {
            try { ready = source.connection(); break; }
            catch (SQLTransientConnectionException unavailable) { if (attempt == 19) throw unavailable; Thread.sleep(100); }
        }
        try (Connection connection = ready; Statement statement = connection.createStatement()) {
            for (String command : sql.split(";")) if (!command.isBlank()) statement.execute(command);
        }
    }
    static void removeScope(HistoryDataSource source, String sourceId) throws SQLException {
        try (Connection connection = source.connection()) {
            byte[] scope;
            try (PreparedStatement s = connection.prepareStatement("SELECT scope_id FROM rmq_monitor_lease WHERE source_id=?")) {
                s.setString(1, sourceId);
                try (ResultSet r = s.executeQuery()) { if (!r.next()) return; scope = r.getBytes(1); }
            }
            for (String table : new String[]{"rmq_monitor_metrics", "rmq_monitor_snapshot", "rmq_monitor_lease"}) {
                try (PreparedStatement s = connection.prepareStatement("DELETE FROM " + table + " WHERE scope_id=? LIMIT 1000")) {
                    s.setBytes(1, scope);
                    while (s.executeUpdate() != 0) { /* 测试大数据集也按有界批次回收。 */ }
                }
            }
        }
    }
}
