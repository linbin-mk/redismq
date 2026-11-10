package com.mk.mq;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/** 所有状态由 gate 保护；结果主动完成在独立线程上执行，取消由调用者完成。 */
final class AsyncSender implements AutoCloseable {
    private enum State { PREPARING, WAITING, RUNNING, FINISHED }
    private enum Outcome { SUCCESS, FAILURE, TIMEOUT, CANCELLED }
    private final Object gate = new Object();
    private final ProducerOptions options;
    private final int capacity;
    private final Set<Task> tasks = new HashSet<>();
    private final ThreadPoolExecutor workers;
    private final ThreadPoolExecutor notifications;
    private final ScheduledThreadPoolExecutor deadlines;
    private final ThreadLocal<Boolean> publishing = new ThreadLocal<>();
    private boolean closed;
    private int preparing, pending, inFlight;
    private long accepted, succeeded, duplicates, failed, rejected, timedOut, cancelled;
    private long totalDurationNanos, maxDurationNanos;

    AsyncSender(ProducerOptions options) {
        this.options = options;
        capacity = options.concurrency() + options.maxPending();
        workers = pool(options.concurrency(), options.maxPending(), "worker");
        // 每个受理任务至多一个通知；通知结束前不归还本地容量，慢回调也不能撑大内存。
        notifications = pool(Math.min(options.concurrency(), 4), capacity, "result");
        deadlines = new ScheduledThreadPoolExecutor(1, threads("deadline"));
        deadlines.setRemoveOnCancelPolicy(true);
    }

    CompletableFuture<SendResult> send(Supplier<Callable<SendResult>> preparation) {
        Task task = new Task();
        synchronized (gate) {
            if (closed) throw reject(SendRejectedException.Reason.CLOSED);
            if (tasks.size() == capacity) throw reject(SendRejectedException.Reason.CAPACITY);
            tasks.add(task);
            preparing++;
        }
        Callable<SendResult> action;
        try { action = preparation.get(); }
        catch (RuntimeException | Error error) {
            synchronized (gate) { preparing--; tasks.remove(task); }
            throw error;
        }
        synchronized (gate) {
            preparing--;
            if (closed) {
                tasks.remove(task);
                throw reject(SendRejectedException.Reason.CLOSED);
            }
            task.action = action;
            task.state = State.WAITING;
            task.startedAt = System.nanoTime();
            pending++;
            try {
                task.deadline = deadlines.schedule(() -> expire(task), options.sendTimeout().toMillis(), TimeUnit.MILLISECONDS);
                workers.execute(task);
            } catch (RejectedExecutionException error) {
                if (task.deadline != null) task.deadline.cancel(false);
                pending--;
                tasks.remove(task);
                throw reject(SendRejectedException.Reason.CAPACITY);
            }
            accepted++;
            return task.future;
        }
    }

    private SendRejectedException reject(SendRejectedException.Reason reason) {
        rejected++;
        return new SendRejectedException(reason);
    }

    private void expire(Task task) {
        synchronized (gate) {
            if (task.outcome == null) {
                finish(task, Outcome.TIMEOUT, null, new SendTimeoutException(task.state == State.RUNNING));
            }
        }
    }

    /** 调用时持有 gate；状态先固定，之后才通知用户，迟到的 Redis 返回不能覆盖终态。 */
    private void finish(Task task, Outcome outcome, SendResult result, Throwable error) {
        if (task.outcome != null) return;
        task.outcome = outcome;
        task.result = result;
        task.error = error;
        if (task.deadline != null) task.deadline.cancel(false);
        if (task.state == State.WAITING) {
            workers.remove(task);
            pending--;
            task.state = State.FINISHED;
            task.action = null;
        }
        switch (outcome) {
            case SUCCESS -> { succeeded++; if (result.duplicate()) duplicates++; }
            case FAILURE -> failed++;
            case TIMEOUT -> timedOut++;
            case CANCELLED -> cancelled++;
        }
        long elapsed = Math.max(0, System.nanoTime() - task.startedAt);
        totalDurationNanos = totalDurationNanos > Long.MAX_VALUE - elapsed ? Long.MAX_VALUE : totalDurationNanos + elapsed;
        maxDurationNanos = Math.max(maxDurationNanos, elapsed);
        if (outcome != Outcome.CANCELLED) notifications.execute(task::publish);
    }

    AsyncSendStats stats() {
        synchronized (gate) {
            return new AsyncSendStats(accepted, succeeded, duplicates, failed, rejected, timedOut, cancelled,
                    preparing, pending, inFlight, tasks.size(), totalDurationNanos, maxDurationNanos, closed);
        }
    }

    @Override public void close() {
        synchronized (gate) {
            if (closed) return;
            closed = true;
            workers.shutdown();
        }
        long until = System.nanoTime() + options.shutdownTimeout().toNanos();
        boolean interrupted = false;
        try { workers.awaitTermination(Math.max(0, until - System.nanoTime()), TimeUnit.NANOSECONDS); }
        catch (InterruptedException error) { interrupted = true; }
        synchronized (gate) {
            for (Task task : List.copyOf(tasks)) {
                if (task.state != State.PREPARING && task.outcome == null) {
                    finish(task, Outcome.FAILURE, null, new SendShutdownException(task.state == State.RUNNING));
                }
            }
            workers.shutdownNow();
            deadlines.shutdownNow();
            notifications.shutdown();
        }
        // 允许用户从结果回调关闭发送器，不等待当前通知线程自身退出。
        if (!Boolean.TRUE.equals(publishing.get())) {
            try { notifications.awaitTermination(Math.max(0, until - System.nanoTime()), TimeUnit.NANOSECONDS); }
            catch (InterruptedException error) { interrupted = true; }
        }
        if (interrupted) Thread.currentThread().interrupt();
    }

    private final class Task implements Runnable {
        private State state = State.PREPARING;
        private Outcome outcome;
        private Callable<SendResult> action;
        private ScheduledFuture<?> deadline;
        private long startedAt;
        private SendResult result;
        private Throwable error;
        private boolean notified;
        private final CompletableFuture<SendResult> future = new CompletableFuture<>() {
            @Override public boolean cancel(boolean mayInterruptIfRunning) {
                synchronized (gate) {
                    if (outcome != null) return false;
                    finish(Task.this, Outcome.CANCELLED, null, null);
                }
                // 取消只停止本地等待；不尝试中断可能已经写入 Redis 的请求。
                try { return super.cancel(false); }
                finally { notificationFinished(); }
            }
        };

        @Override public void run() {
            synchronized (gate) {
                if (outcome != null) return;
                // 即使期限线程暂未得到调度，也不启动已经过期的等待任务。
                if (System.nanoTime() - startedAt >= options.sendTimeout().toNanos()) {
                    finish(this, Outcome.TIMEOUT, null, new SendTimeoutException(false));
                    return;
                }
                state = State.RUNNING;
                pending--;
                inFlight++;
            }
            SendResult value = null;
            Throwable failure = null;
            try { value = action.call(); }
            catch (Throwable error) { failure = error; }
            finally {
                synchronized (gate) {
                    inFlight--;
                    state = State.FINISHED;
                    action = null;
                    if (outcome == null) {
                        if (System.nanoTime() - startedAt >= options.sendTimeout().toNanos()) {
                            finish(this, Outcome.TIMEOUT, null, new SendTimeoutException(true));
                        } else {
                            finish(this, failure == null ? Outcome.SUCCESS : Outcome.FAILURE, value, failure);
                        }
                    }
                    releaseIfFinished();
                }
            }
        }

        private void publish() {
            publishing.set(true);
            try {
                if (error == null) future.complete(result);
                else future.completeExceptionally(error);
            } finally { publishing.remove(); notificationFinished(); }
        }

        private void notificationFinished() {
            synchronized (gate) { notified = true; releaseIfFinished(); }
        }

        private void releaseIfFinished() {
            if (state == State.FINISHED && notified) tasks.remove(this);
        }
    }

    private static ThreadPoolExecutor pool(int concurrency, int capacity, String role) {
        return new ThreadPoolExecutor(concurrency, concurrency, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(capacity), threads(role), new ThreadPoolExecutor.AbortPolicy());
    }

    private static ThreadFactory threads(String role) {
        AtomicInteger sequence = new AtomicInteger();
        return action -> {
            Thread thread = new Thread(action, "redis-mq-send-" + role + "-" + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }
}
