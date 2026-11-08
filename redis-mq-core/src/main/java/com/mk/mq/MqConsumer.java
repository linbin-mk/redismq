package com.mk.mq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** 每个监听器独立限流；业务线程、调度线程和续期线程互不共用。 */
public final class MqConsumer<T> implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(MqConsumer.class);
    private final RedisMq mq;
    private final String queue;
    private final Class<T> payloadType;
    private final MessageHandler<T> handler;
    private final ConsumerOptions options;
    private final Semaphore slots;
    private final Semaphore wakeup = new Semaphore(0);
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean abandoned = new AtomicBoolean();
    private final ExecutorService workers;
    private final ScheduledExecutorService renewal;
    private final ScheduledExecutorService deadlines;
    private final Thread dispatcher;
    private AutoCloseable subscription;

    MqConsumer(RedisMq mq, String queue, Class<T> payloadType, MessageHandler<T> handler, ConsumerOptions options) {
        this.mq = Objects.requireNonNull(mq);
        this.queue = Checks.queue(queue);
        this.payloadType = Objects.requireNonNull(payloadType);
        this.handler = Objects.requireNonNull(handler);
        this.options = Objects.requireNonNull(options);
        slots = new Semaphore(options.concurrency());
        workers = Executors.newFixedThreadPool(options.concurrency(), threads("业务"));
        renewal = scheduler(Math.min(options.concurrency(), 4), "续期");
        deadlines = scheduler(1, "超时");
        dispatcher = threads("调度").newThread(this::dispatch);
    }

    void start() {
        if (!started.compareAndSet(false, true)) throw new IllegalStateException("消费者不能重复启动");
        running.set(true);
        try { subscription = mq.subscribe(queue, this::signal); }
        catch (RuntimeException e) { log.warn("队列 {} 通知订阅失败，使用兜底检查", queue, e); }
        dispatcher.start();
    }

    private void dispatch() {
        while (running.get()) {
            wakeup.drainPermits();
            long wait = options.pollInterval().toMillis();
            try {
                int count = Math.min(100, slots.availablePermits());
                if (count > 0) {
                    ReceiveBatch batch = mq.receive(queue, count, options.visibility());
                    wait = batch.deliveries().isEmpty()
                            ? Math.max(10, Math.min(wait, batch.nextCheckAfterMillis())) : 0;
                    for (Delivery delivery : batch.deliveries()) {
                        if (!running.get()) break;
                        slots.acquireUninterruptibly();
                        try { submit(delivery); }
                        catch (RuntimeException e) {
                            slots.release();
                            log.warn("队列 {} 消息 {} 未能交给业务线程，处理权到期后恢复", queue, delivery.message().id(), e);
                        }
                    }
                }
            } catch (RuntimeException e) {
                if (running.get()) log.warn("队列 {} 领取失败，稍后重试", queue, e);
            }
            try { wakeup.tryAcquire(wait, TimeUnit.MILLISECONDS); }
            catch (InterruptedException e) { if (!running.get()) break; }
        }
    }

    private void submit(Delivery delivery) {
        AtomicBoolean done = new AtomicBoolean();
        AtomicBoolean lost = new AtomicBoolean();
        AtomicReference<Thread> businessThread = new AtomicReference<>();
        long startedAt = System.nanoTime();
        long interval = Math.max(50, options.visibility().toMillis() / 3);
        var heartbeat = renewal.scheduleWithFixedDelay(() -> {
            if (done.get() || lost.get()) return;
            try {
                if (!mq.renew(queue, delivery, options.visibility())) lost.set(true);
            } catch (RuntimeException e) {
                // 确认不了处理权时不再提交成功结果，后续交给超时恢复。
                lost.set(true);
                log.warn("队列 {} 消息 {} 续期失败，等待处理权超时恢复", queue, delivery.message().id(), e);
            }
        }, interval, interval, TimeUnit.MILLISECONDS);
        var deadline = deadlines.schedule(() -> {
            synchronized (businessThread) {
                if (!done.get()) {
                    lost.set(true);
                    Thread thread = businessThread.get();
                    if (thread != null) thread.interrupt();
                    log.warn("队列 {} 消息 {} 超过最长执行时间，停止续期并请求中断", queue, delivery.message().id());
                }
            }
        }, options.maxProcessingTime().toMillis(), TimeUnit.MILLISECONDS);
        try {
            workers.execute(() -> {
                businessThread.set(Thread.currentThread());
                try {
                    if (lost.get() || abandoned.get()) return;
                    T payload = mq.decode(delivery, payloadType);
                    MessageSnapshot message = delivery.message();
                    handler.handle(payload, new MessageContext(queue, message.id(), message.deliveryCount(), message.createdAt()));
                    if (System.nanoTime() - startedAt >= options.maxProcessingTime().toNanos()) lost.set(true);
                    if (!lost.get() && !abandoned.get() && !mq.acknowledge(queue, delivery)) {
                        log.warn("队列 {} 消息 {} 已失去处理权，未确认完成", queue, message.id());
                    }
                } catch (Exception e) {
                    if (System.nanoTime() - startedAt >= options.maxProcessingTime().toNanos()) lost.set(true);
                    if (!lost.get() && !abandoned.get()) {
                        try {
                            String reason = e.getClass().getSimpleName() + ": " + e.getMessage();
                            if (e instanceof RejectMessageException) mq.reject(queue, delivery, reason);
                            else mq.retry(queue, delivery, reason);
                        } catch (RuntimeException retryError) {
                            log.warn("队列 {} 消息 {} 失败状态写入失败，等待超时恢复", queue, delivery.message().id(), retryError);
                        }
                    }
                    log.warn("队列 {} 消息 {} 第 {} 次处理失败", queue, delivery.message().id(), delivery.message().deliveryCount(), e);
                } finally {
                    synchronized (businessThread) {
                        done.set(true);
                        businessThread.set(null);
                        // 超时中断必须在业务线程归还前结束，避免影响下一次回调。
                        Thread.interrupted();
                    }
                    heartbeat.cancel(false);
                    deadline.cancel(false);
                    slots.release();
                    signal();
                }
            });
        } catch (RuntimeException e) {
            done.set(true);
            heartbeat.cancel(false);
            deadline.cancel(false);
            throw e;
        }
    }

    private void signal() { if (wakeup.availablePermits() == 0) wakeup.release(); }

    /** 停止领取，等待已有回调结束；超时后停止续期，消息交给其他消费者恢复。 */
    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) return;
        signal();
        dispatcher.interrupt();
        if (subscription != null) {
            try { subscription.close(); }
            catch (Exception e) { log.warn("队列 {} 取消通知订阅失败", queue, e); }
        }
        workers.shutdown();
        try {
            if (!workers.awaitTermination(options.shutdownTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
                abandoned.set(true);
                workers.shutdownNow();
            }
        } catch (InterruptedException e) {
            abandoned.set(true);
            workers.shutdownNow();
            Thread.currentThread().interrupt();
        } finally {
            renewal.shutdownNow();
            deadlines.shutdownNow();
        }
    }

    private ScheduledExecutorService scheduler(int count, String role) {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(count, threads(role));
        // 快速消息取消续期和超时任务后立即移除，避免调度队列随吞吐量增长。
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    private ThreadFactory threads(String role) {
        AtomicInteger sequence = new AtomicInteger();
        return task -> {
            Thread thread = new Thread(task, "redis-mq-" + queue + "-" + role + "-" + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }
}
