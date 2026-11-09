package com.mk.mq.dashboard.mysql;

import org.springframework.boot.context.properties.ConfigurationProperties;
import java.time.Duration;
import java.util.List;
import java.util.regex.Pattern;

@ConfigurationProperties("redis-mq.dashboard.persistence")
public class PersistenceProperties {
    private boolean enabled;
    private String sourceId;
    private Duration retention = Duration.ofDays(5);
    private Duration sampleInterval = Duration.ofMinutes(1);
    private Duration flushInterval = Duration.ofSeconds(10);
    private Duration cleanupInterval = Duration.ofMinutes(1);
    private Duration leaseDuration = Duration.ofSeconds(30);
    private int maxQueues = 128;
    private int pendingMinutes = 3;
    private int cleanupBatchSize = 1000;
    private List<String> queues = List.of();
    private Datasource datasource = new Datasource();
    static final Pattern NAME = Pattern.compile("[a-zA-Z0-9._:-]{1,160}");

    public void validate() {
        if (sourceId == null || !sourceId.matches("[a-zA-Z0-9._:-]{1,64}")) throw new IllegalArgumentException("持久化 source-id 必须为 1 至 64 位安全标识");
        millis(retention, 60000, 366L * 86400000, "保留时长");
        millis(sampleInterval, 10000, 3600000, "采样间隔");
        millis(flushInterval, 1000, 30000, "写入间隔");
        millis(cleanupInterval, 1000, 3600000, "清理间隔");
        millis(leaseDuration, 10000, 300000, "采集租约");
        if (leaseDuration.toMillis() < flushInterval.toMillis() * 3) throw new IllegalArgumentException("采集租约不能小于写入间隔的三倍");
        if (maxQueues < 1 || maxQueues > 1024 || pendingMinutes < 2 || pendingMinutes > 10
                || cleanupBatchSize < 1 || cleanupBatchSize > 1000) throw new IllegalArgumentException("分析容量配置超出支持范围");
        if (queues.size() > maxQueues || queues.stream().distinct().count() != queues.size()) throw new IllegalArgumentException("历史队列列表不能重复或超出容量");
        queues.forEach(PersistenceProperties::queue);
        if (datasource == null) throw new IllegalArgumentException("历史数据源配置不能为空");
        if (datasource.beanName != null && !datasource.beanName.isBlank() && datasource.url != null && !datasource.url.isBlank()) {
            throw new IllegalArgumentException("历史数据源不能同时配置 URL 和已有 Bean");
        }
        if (datasource.maximumPoolSize < 1 || datasource.maximumPoolSize > 4) throw new IllegalArgumentException("监控连接池容量必须为 1 至 4");
        if (datasource.beanName == null || datasource.beanName.isBlank()) {
            if (datasource.url == null || !datasource.url.startsWith("jdbc:mysql://")
                    || datasource.username == null || datasource.username.isBlank()) throw new IllegalArgumentException("启用 MySQL 历史需要配置独立 URL、用户名，或指定 DataSource Bean");
            int query = datasource.url.indexOf('?');
            if (query >= 0) for (String item : datasource.url.substring(query + 1).split("&")) {
                String[] pair = item.split("=", 2);
                String key = java.net.URLDecoder.decode(pair[0], java.nio.charset.StandardCharsets.UTF_8).toLowerCase(java.util.Locale.ROOT);
                if (key.equals("connecttimeout") || key.equals("sockettimeout")) {
                    long maximum = key.equals("connecttimeout") ? 1000 : 2000;
                    long configured;
                    try { configured = Long.parseLong(java.net.URLDecoder.decode(pair.length == 2 ? pair[1] : "", java.nio.charset.StandardCharsets.UTF_8)); }
                    catch (NumberFormatException exception) { throw new IllegalArgumentException("JDBC 超时参数必须是有界毫秒数", exception); }
                    if (configured < 1 || configured > maximum) throw new IllegalArgumentException("JDBC URL 不能覆盖监控连接的超时上限");
                }
            }
        }
    }
    /** 配置在启动时冻结；外部配置刷新需重建实例，不能越过校验改变清理边界。 */
    PersistenceProperties snapshot() {
        validate();
        PersistenceProperties copy = new PersistenceProperties();
        copy.enabled = enabled; copy.sourceId = sourceId; copy.retention = retention;
        copy.sampleInterval = sampleInterval; copy.flushInterval = flushInterval; copy.cleanupInterval = cleanupInterval;
        copy.leaseDuration = leaseDuration; copy.maxQueues = maxQueues; copy.pendingMinutes = pendingMinutes;
        copy.cleanupBatchSize = cleanupBatchSize; copy.queues = List.copyOf(queues);
        copy.datasource.url = datasource.url; copy.datasource.username = datasource.username; copy.datasource.password = datasource.password;
        copy.datasource.beanName = datasource.beanName; copy.datasource.maximumPoolSize = datasource.maximumPoolSize;
        return copy;
    }
    static void queue(String queue) {
        if (queue == null || !NAME.matcher(queue).matches() || queue.indexOf(':') >= 0) throw new IllegalArgumentException("历史队列名称不合法");
    }
    private static void millis(Duration value, long min, long max, String name) {
        if (value == null || value.isNegative() || value.getNano() % 1000000 != 0) throw new IllegalArgumentException(name + "必须为毫秒精度正数");
        try {
            if (value.toMillis() < min || value.toMillis() > max) throw new IllegalArgumentException(name + "超出支持范围");
        } catch (ArithmeticException e) { throw new IllegalArgumentException(name + "超出支持范围", e); }
    }
    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean value) { enabled = value; }
    public String getSourceId() { return sourceId; }
    public void setSourceId(String value) { sourceId = value; }
    public Duration getRetention() { return retention; }
    public void setRetention(Duration value) { retention = value; }
    public Duration getSampleInterval() { return sampleInterval; }
    public void setSampleInterval(Duration value) { sampleInterval = value; }
    public Duration getFlushInterval() { return flushInterval; }
    public void setFlushInterval(Duration value) { flushInterval = value; }
    public Duration getCleanupInterval() { return cleanupInterval; }
    public void setCleanupInterval(Duration value) { cleanupInterval = value; }
    public Duration getLeaseDuration() { return leaseDuration; }
    public void setLeaseDuration(Duration value) { leaseDuration = value; }
    public int getMaxQueues() { return maxQueues; }
    public void setMaxQueues(int value) { maxQueues = value; }
    public int getPendingMinutes() { return pendingMinutes; }
    public void setPendingMinutes(int value) { pendingMinutes = value; }
    public int getCleanupBatchSize() { return cleanupBatchSize; }
    public void setCleanupBatchSize(int value) { cleanupBatchSize = value; }
    public List<String> getQueues() { return queues; }
    public void setQueues(List<String> value) { queues = List.copyOf(value); }
    public Datasource getDatasource() { return datasource; }
    public void setDatasource(Datasource value) { datasource = value; }

    public static class Datasource {
        private String url;
        private String username;
        private String password;
        private String beanName;
        private int maximumPoolSize = 2;
        public String getUrl() { return url; }
        public void setUrl(String value) { url = value; }
        public String getUsername() { return username; }
        public void setUsername(String value) { username = value; }
        public String getPassword() { return password; }
        public void setPassword(String value) { password = value; }
        public String getBeanName() { return beanName; }
        public void setBeanName(String value) { beanName = value; }
        public int getMaximumPoolSize() { return maximumPoolSize; }
        public void setMaximumPoolSize(int value) { maximumPoolSize = value; }
    }
}
