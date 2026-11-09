package com.mk.mq.dashboard.mysql;

import com.mk.mq.RedisMq;
import com.mk.mq.dashboard.DashboardHistory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/** 单线程异步写入，DB 故障不进入业务就绪状态，也不阻塞 MQ 的回调线程。 */
public final class MysqlHistory implements DashboardHistory, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(MysqlHistory.class);
    private final RedisMq mq;
    private final PersistenceProperties properties;
    private final HistoryRepository repository;
    private final boolean releaseOnClose;
    private final MinuteMetrics metrics;
    private final ScheduledThreadPoolExecutor worker;
    private final Semaphore reads = new Semaphore(1);
    private final AtomicBoolean running = new AtomicBoolean();
    private final LongAdder failures = new LongAdder();
    private final LongAdder cleanupRows = new LongAdder();
    private volatile String state = "STARTING";
    private volatile String error;
    private volatile long offsetMillis;
    private volatile long lastWriteAt;
    private volatile long lastSampleAt;
    private volatile long lastCleanupAt;
    private volatile boolean collector;
    private volatile int skippedQueues;
    private volatile boolean cleanupBehind;
    private AutoCloseable observation;
    private boolean closed;
    private int consecutiveFailures;
    private volatile long nextAttempt;
    private volatile boolean storageUnavailable;
    private long nextSample;
    private long nextCleanup;

    public MysqlHistory(RedisMq mq, PersistenceProperties properties, HistoryDataSource source) {
        properties = properties.snapshot();
        this.mq = mq;
        this.properties = properties;
        releaseOnClose = source.owned();
        repository = new HistoryRepository(source, properties, mq.namespace(), UUID.randomUUID().toString());
        metrics = new MinuteMetrics(properties, this::now);
        worker = new ScheduledThreadPoolExecutor(1, task -> {
            Thread thread = new Thread(task, "redis-mq-history-writer");
            thread.setDaemon(true);
            return thread;
        });
        worker.setRemoveOnCancelPolicy(true);
    }
    public synchronized void start() {
        if (closed) throw new IllegalStateException("已关闭的历史分析实例不能重新启动");
        if (!running.compareAndSet(false, true)) return;
        try { observation = mq.observe(metrics); }
        catch (RuntimeException e) { running.set(false); worker.shutdownNow(); throw e; }
        worker.scheduleWithFixedDelay(this::tick, 0, properties.getFlushInterval().toMillis(), TimeUnit.MILLISECONDS);
    }
    private long now() { return Math.max(0, System.currentTimeMillis() + offsetMillis); }
    private static boolean due(long deadline) { return deadline == 0 || System.nanoTime() - deadline >= 0; }
    private void tick() {
        if (!running.get()) return;
        metrics.prune(now());
        if (!due(nextAttempt)) return;
        try {
            HistoryRepository.Lease lease = repository.lease(mq.namespace());
            offsetMillis = lease.now() - System.currentTimeMillis();
            collector = lease.owner();
            // 回收不依赖 Redis 可用性，也不被后续采样失败跳过。
            if (collector && due(nextCleanup)) {
                HistoryRepository.Cleanup result = repository.cleanup(lease.token());
                cleanupRows.add(result.deleted());
                cleanupBehind = result.behind();
                lastCleanupAt = now();
                nextCleanup = System.nanoTime() + (cleanupBehind ? properties.getFlushInterval() : properties.getCleanupInterval()).toNanos();
            }
            List<MinuteMetrics.Row> rows = metrics.snapshot(now());
            repository.metrics(rows);
            metrics.persisted(rows);
            lastWriteAt = now();
            if (collector && due(nextSample)) {
                List<HistoryRepository.Sample> samples = sample(now());
                repository.snapshots(samples, lease.token());
                lastSampleAt = now();
                nextSample = System.nanoTime() + properties.getSampleInterval().toNanos();
            }
            if (!running.get()) return;
            consecutiveFailures = 0;
            storageUnavailable = false;
            nextAttempt = 0;
            state = cleanupBehind ? "CLEANUP_BEHIND" : "RUNNING";
            error = cleanupBehind ? "过期分析数据正在分批回收，查询已排除过期记录" : null;
        } catch (RuntimeException | SQLException exception) {
            if (!running.get()) return;
            collector = false;
            failures.increment();
            consecutiveFailures++;
            state = "DEGRADED";
            storageUnavailable = exception instanceof SQLException sql
                    && (sql.getSQLState() == null || (!sql.getSQLState().startsWith("40") && !sql.getSQLState().startsWith("45")));
            error = failureMessage(exception);
            long delay = Math.min(30000, properties.getFlushInterval().toMillis() * (1L << Math.min(consecutiveFailures - 1, 5)));
            nextAttempt = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(delay);
            if (consecutiveFailures == 1 || consecutiveFailures % 10 == 0) log.warn("MQ 历史分析降级：{}", error);
        }
    }
    private List<HistoryRepository.Sample> sample(long timestamp) {
        List<String> names;
        if (!properties.getQueues().isEmpty()) {
            names = properties.getQueues();
            skippedQueues = 0;
        } else {
            int total = mq.registeredQueueCount();
            skippedQueues = Math.max(0, total - properties.getMaxQueues());
            names = new ArrayList<>();
            while (names.size() < Math.min(total, properties.getMaxQueues())) {
                List<String> page = mq.registeredQueues(names.size(), Math.min(100, properties.getMaxQueues() - names.size()));
                if (page.isEmpty()) break;
                names.addAll(page);
            }
        }
        List<HistoryRepository.Sample> samples = new ArrayList<>();
        long bucket = timestamp / properties.getSampleInterval().toMillis() * properties.getSampleInterval().toMillis();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        for (String queue : names) {
            if (!running.get()) break;
            if (System.nanoTime() - deadline >= 0) { skippedQueues += names.size() - samples.size(); break; }
            samples.add(new HistoryRepository.Sample(queue, bucket, mq.stats(queue)));
        }
        return samples;
    }
    private static String failureMessage(Exception exception) {
        if (exception instanceof SQLException sql) {
            if ("45000".equals(sql.getSQLState())) return "同一监控源的采样或保留配置不一致，请统一配置并等待旧租约结束";
            if (sql.getSQLState() != null && sql.getSQLState().startsWith("42")) return "分析表不存在或版本不兼容，请检查 V1 初始化 SQL";
            // 不返回 JDBC URL、SQL 参数、数据库账号或底层异常全文。
            return "历史数据库暂不可用（SQLState " + (sql.getSQLState() == null ? "unknown" : sql.getSQLState()) + "）";
        }
        return "队列采样暂不可用，实时 MQ 功能保持独立";
    }
    @Override public Status status() {
        return new Status(true, state, properties.getSourceId(), mq.namespace(), properties.getRetention().toMillis(),
                properties.getSampleInterval().toMillis(), collector, lastWriteAt, lastSampleAt, lastCleanupAt,
                metrics.size(), metrics.dropped(), failures.sum(), skippedQueues, cleanupRows.sum(), error);
    }
    @Override public Result query(String queue, Long from, Long to, int points) {
        PersistenceProperties.queue(queue);
        if (points < 1 || points > 1000 || (from != null && from < 0) || (to != null && to < 0)
                || (from != null && to != null && from >= to)) throw new IllegalArgumentException("历史查询参数不合法");
        if (!running.get()) throw new UnavailableException("历史分析未运行");
        if (storageUnavailable && !due(nextAttempt)) throw new UnavailableException("历史数据库正在退避重连，请稍后查询");
        if (!reads.tryAcquire()) throw new UnavailableException("历史查询繁忙，请稍后重试");
        try { return repository.query(mq.namespace(), queue, from, to, points); }
        catch (SQLException e) { throw new UnavailableException(failureMessage(e)); }
        finally { reads.release(); }
    }
    @Override public synchronized void close() {
        if (closed) return;
        closed = true;
        if (!running.getAndSet(false)) { worker.shutdownNow(); state = "STOPPED"; return; }
        try { if (observation != null) observation.close(); }
        catch (Exception e) { log.warn("卸载 MQ 分析收集器失败"); }
        worker.shutdownNow();
        try {
            if (worker.awaitTermination(2500, TimeUnit.MILLISECONDS) && releaseOnClose) repository.release();
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        catch (SQLException ignored) { /* DB 不可用时由租约到期接管，不阻塞服务停止。 */ }
        collector = false;
        state = "STOPPED";
    }
}
