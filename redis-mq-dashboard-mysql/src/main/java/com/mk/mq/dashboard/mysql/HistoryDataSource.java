package com.mk.mq.dashboard.mysql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.BeanFactory;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/** 包装成内部资源而非 DataSource Bean，避免影响宿主的数据源自动配置。 */
public final class HistoryDataSource implements AutoCloseable {
    private final DataSource source;
    private final boolean owned;
    public HistoryDataSource(PersistenceProperties properties, BeanFactory beans) {
        var options = properties.getDatasource();
        if (options.getBeanName() != null && !options.getBeanName().isBlank()) {
            source = beans.getBean(options.getBeanName(), DataSource.class);
            owned = false;
        } else {
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(options.getUrl());
            config.setUsername(options.getUsername());
            config.setPassword(options.getPassword());
            config.setPoolName("redis-mq-history");
            config.setMaximumPoolSize(options.getMaximumPoolSize());
            config.setMinimumIdle(0);
            config.setConnectionTimeout(500);
            config.setValidationTimeout(500);
            config.setInitializationFailTimeout(-1);
            config.setIdleTimeout(60000);
            config.setMaxLifetime(600000);
            config.addDataSourceProperty("connectTimeout", "1000");
            config.addDataSourceProperty("socketTimeout", "2000");
            config.addDataSourceProperty("rewriteBatchedStatements", "true");
            source = new HikariDataSource(config);
            owned = true;
        }
    }
    boolean owned() { return owned; }
    Connection connection() throws SQLException { return source.getConnection(); }
    @Override public void close() { if (owned) ((HikariDataSource) source).close(); }
}
