package com.mk.mq;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

class AsyncSenderTest {
    private static ProducerOptions options(Duration timeout, Duration shutdown) {
        return new ProducerOptions(1, 1, timeout, shutdown);
    }

    private static SendResult success() { return new SendResult("request", false); }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= until) fail("等待发送器状态超时");
            Thread.sleep(5);
        }
    }

    /** 模拟忽略中断的底层 I/O：关闭和结果超时都不能假设它已经停止。 */
    private static void hold(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try { latch.await(); break; }
            catch (InterruptedException error) { interrupted = true; }
        }
        if (interrupted) Thread.currentThread().interrupt();
    }

    private static Throwable failure(CompletableFuture<?> future) {
        return assertThrows(ExecutionException.class, () -> future.get(3, TimeUnit.SECONDS)).getCause();
    }

    @Test void rejectsBeforePreparingAndNeverRunsOnCallingThread() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        Thread caller = Thread.currentThread();
        try (AsyncSender sender = new AsyncSender(options(Duration.ofSeconds(5), Duration.ZERO))) {
            var first = sender.send(() -> () -> {
                assertNotSame(caller, Thread.currentThread());
                entered.countDown(); hold(release); return success();
            });
            try {
                assertTrue(entered.await(2, TimeUnit.SECONDS));
                var queued = sender.send(() -> AsyncSenderTest::success);
                AtomicBoolean prepared = new AtomicBoolean();
                var error = assertThrows(SendRejectedException.class, () -> sender.send(() -> {
                    prepared.set(true); return AsyncSenderTest::success;
                }));
                assertEquals(SendRejectedException.Reason.CAPACITY, error.reason());
                assertFalse(prepared.get());
                assertFalse(first.isDone()); assertFalse(queued.isDone());
                assertEquals(1, sender.stats().pending());
                assertEquals(1, sender.stats().inFlight());
                release.countDown();
                assertEquals(success(), first.get(2, TimeUnit.SECONDS));
                assertEquals(success(), queued.get(2, TimeUnit.SECONDS));
                await(() -> sender.stats().outstanding() == 0);
            } finally { release.countDown(); }
        }
    }

    @Test void timeoutsKeepPhysicalConcurrencyAndRemoveExpiredPendingWork() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        AtomicInteger sends = new AtomicInteger();
        try (AsyncSender sender = new AsyncSender(options(Duration.ofMillis(300), Duration.ZERO))) {
            var first = sender.send(() -> () -> {
                sends.incrementAndGet(); entered.countDown(); hold(release); return success();
            });
            try {
                assertTrue(entered.await(2, TimeUnit.SECONDS));
                assertTrue(assertInstanceOf(SendTimeoutException.class, failure(first)).outcomeUnknown());
                var queued = sender.send(() -> () -> { sends.incrementAndGet(); return success(); });
                assertThrows(SendRejectedException.class, () -> sender.send(() -> AsyncSenderTest::success));
                assertFalse(assertInstanceOf(SendTimeoutException.class, failure(queued)).outcomeUnknown());
                await(() -> sender.stats().outstanding() == 1);
                assertEquals(1, sender.stats().inFlight());
                assertEquals(0, sender.stats().pending());
                release.countDown();
                await(() -> sender.stats().outstanding() == 0);
                assertEquals(1, sends.get());
                assertEquals(2, sender.stats().timedOut());
                assertEquals(0, sender.stats().succeeded());
                assertInstanceOf(SendTimeoutException.class, failure(first));
            } finally { release.countDown(); }
        }
    }

    @Test void cancellationWithdrawsQueuedWorkWithoutInterruptingOrFreeingRunningCall() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean(), queuedRan = new AtomicBoolean();
        try (AsyncSender sender = new AsyncSender(options(Duration.ofSeconds(5), Duration.ZERO))) {
            var first = sender.send(() -> () -> {
                entered.countDown(); hold(release); interrupted.set(Thread.currentThread().isInterrupted()); return success();
            });
            try {
                assertTrue(entered.await(2, TimeUnit.SECONDS));
                assertTrue(first.cancel(true));
                var queued = sender.send(() -> () -> { queuedRan.set(true); return success(); });
                assertThrows(SendRejectedException.class, () -> sender.send(() -> AsyncSenderTest::success));
                assertTrue(queued.cancel(false));
                assertEquals(1, sender.stats().outstanding());
                assertEquals(1, sender.stats().inFlight());
                release.countDown();
                await(() -> sender.stats().outstanding() == 0);
                assertFalse(interrupted.get()); assertFalse(queuedRan.get());
                assertTrue(first.isCancelled()); assertEquals(2, sender.stats().cancelled());
            } finally { release.countDown(); }
        }
    }

    @Test void recordsSuccessDuplicateFailureAndReleasesFailedPreparation() throws Exception {
        try (AsyncSender sender = new AsyncSender(options(Duration.ofSeconds(5), Duration.ofSeconds(1)))) {
            assertThrows(IllegalArgumentException.class, () -> sender.send(() -> { throw new IllegalArgumentException("编码失败"); }));
            assertEquals(0, sender.stats().outstanding());
            assertEquals(success(), sender.send(() -> AsyncSenderTest::success).get(2, TimeUnit.SECONDS));
            assertTrue(sender.send(() -> () -> new SendResult("request", true)).get(2, TimeUnit.SECONDS).duplicate());
            IllegalStateException cause = new IllegalStateException("Redis 拒绝");
            assertSame(cause, failure(sender.send(() -> () -> { throw cause; })));
            await(() -> sender.stats().outstanding() == 0);
            var stats = sender.stats();
            assertEquals(3, stats.accepted()); assertEquals(2, stats.succeeded());
            assertEquals(1, stats.duplicates()); assertEquals(1, stats.failed());
            assertTrue(stats.totalDurationNanos() >= stats.maxDurationNanos());
        }
    }

    @Test void boundedShutdownSettlesWaitingAndRunningCallsWithDifferentCertainty() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        AsyncSender sender = new AsyncSender(options(Duration.ofSeconds(5), Duration.ofMillis(30)));
        try {
            var first = sender.send(() -> () -> { entered.countDown(); hold(release); return success(); });
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            var queued = sender.send(() -> AsyncSenderTest::success);
            long start = System.nanoTime();
            sender.close();
            assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(2));
            assertTrue(assertInstanceOf(SendShutdownException.class, failure(first)).outcomeUnknown());
            assertFalse(assertInstanceOf(SendShutdownException.class, failure(queued)).outcomeUnknown());
            assertEquals(SendRejectedException.Reason.CLOSED,
                    assertThrows(SendRejectedException.class, () -> sender.send(() -> AsyncSenderTest::success)).reason());
            assertEquals(1, sender.stats().inFlight());
            release.countDown();
            await(() -> sender.stats().outstanding() == 0);
            assertEquals(2, sender.stats().failed());
            sender.close();
        } finally { release.countDown(); sender.close(); }
    }

    @Test void gracefulCloseDrainsAcceptedWork() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        AsyncSender sender = new AsyncSender(options(Duration.ofSeconds(5), Duration.ofSeconds(2)));
        ExecutorService closer = Executors.newSingleThreadExecutor();
        try {
            var first = sender.send(() -> () -> { entered.countDown(); hold(release); return success(); });
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            var second = sender.send(() -> AsyncSenderTest::success);
            var closing = closer.submit(sender::close);
            await(() -> sender.stats().closed());
            assertFalse(closing.isDone());
            release.countDown();
            closing.get(3, TimeUnit.SECONDS);
            assertEquals(success(), first.get(1, TimeUnit.SECONDS));
            assertEquals(success(), second.get(1, TimeUnit.SECONDS));
            assertEquals(0, sender.stats().outstanding());
        } finally { release.countDown(); sender.close(); closer.shutdownNow(); }
    }

    @Test void preparationsAreBoundedAndCannotSubmitAfterConcurrentClose() throws Exception {
        CountDownLatch preparing = new CountDownLatch(2), release = new CountDownLatch(1);
        AsyncSender sender = new AsyncSender(options(Duration.ofSeconds(5), Duration.ZERO));
        ExecutorService callers = Executors.newFixedThreadPool(2);
        try {
            Callable<CompletableFuture<SendResult>> submit = () -> sender.send(() -> {
                preparing.countDown(); hold(release); return AsyncSenderTest::success;
            });
            var first = callers.submit(submit); var second = callers.submit(submit);
            assertTrue(preparing.await(2, TimeUnit.SECONDS));
            assertEquals(2, sender.stats().preparing());
            assertThrows(SendRejectedException.class, () -> sender.send(() -> AsyncSenderTest::success));
            sender.close(); release.countDown();
            assertEquals(SendRejectedException.Reason.CLOSED,
                    assertInstanceOf(SendRejectedException.class, assertThrows(ExecutionException.class,
                            () -> first.get(2, TimeUnit.SECONDS)).getCause()).reason());
            assertInstanceOf(SendRejectedException.class, assertThrows(ExecutionException.class,
                    () -> second.get(2, TimeUnit.SECONDS)).getCause());
            assertEquals(0, sender.stats().outstanding()); assertEquals(0, sender.stats().accepted());
        } finally { release.countDown(); sender.close(); callers.shutdownNow(); }
    }

    @Test void concurrentAdmissionAndCompletionStayWithinConfiguredBounds() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger running = new AtomicInteger(), peak = new AtomicInteger();
        ConcurrentLinkedQueue<CompletableFuture<SendResult>> accepted = new ConcurrentLinkedQueue<>();
        ExecutorService callers = Executors.newFixedThreadPool(16);
        try (AsyncSender sender = new AsyncSender(new ProducerOptions(3, 5, Duration.ofSeconds(5), Duration.ofSeconds(1)))) {
            try {
                var attempts = new java.util.ArrayList<Future<?>>();
                for (int i = 0; i < 1000; i++) attempts.add(callers.submit(() -> {
                    try {
                        accepted.add(sender.send(() -> () -> {
                            peak.accumulateAndGet(running.incrementAndGet(), Math::max);
                            try { hold(release); return success(); }
                            finally { running.decrementAndGet(); }
                        }));
                    } catch (SendRejectedException error) {
                        assertEquals(SendRejectedException.Reason.CAPACITY, error.reason());
                    }
                    assertTrue(sender.stats().outstanding() <= 8);
                }));
                for (var attempt : attempts) attempt.get(3, TimeUnit.SECONDS);
                assertFalse(accepted.isEmpty()); assertTrue(accepted.size() <= 8);
                assertEquals(1000, sender.stats().accepted() + sender.stats().rejected());
                release.countDown();
                for (var result : accepted) assertEquals(success(), result.get(3, TimeUnit.SECONDS));
                await(() -> sender.stats().outstanding() == 0);
                assertTrue(peak.get() <= 3);
                assertEquals(accepted.size(), sender.stats().succeeded());
            } finally { release.countDown(); }
        } finally { callers.shutdownNow(); }
    }

    @Test void slowResultCallbackBackpressuresAdmissionWithoutBlockingWorkersOrDeadlineThread() throws Exception {
        CountDownLatch finishFirst = new CountDownLatch(1), callbackEntered = new CountDownLatch(1);
        CountDownLatch releaseCallback = new CountDownLatch(1), thirdEntered = new CountDownLatch(1), releaseThird = new CountDownLatch(1);
        try (AsyncSender sender = new AsyncSender(new ProducerOptions(1, 2, Duration.ofMillis(500), Duration.ZERO))) {
            try {
                var first = sender.send(() -> () -> { hold(finishFirst); return success(); });
                first.whenComplete((value, error) -> { callbackEntered.countDown(); hold(releaseCallback); });
                finishFirst.countDown();
                assertTrue(callbackEntered.await(2, TimeUnit.SECONDS));
                var second = sender.send(() -> AsyncSenderTest::success);
                await(() -> sender.stats().succeeded() == 2);
                assertFalse(second.isDone());
                var third = sender.send(() -> () -> { thirdEntered.countDown(); hold(releaseThird); return success(); });
                assertTrue(thirdEntered.await(2, TimeUnit.SECONDS));
                await(() -> sender.stats().timedOut() == 1);
                assertEquals(3, sender.stats().outstanding());
                assertThrows(SendRejectedException.class, () -> sender.send(() -> AsyncSenderTest::success));
                releaseCallback.countDown();
                assertEquals(success(), second.get(2, TimeUnit.SECONDS));
                assertTrue(assertInstanceOf(SendTimeoutException.class, failure(third)).outcomeUnknown());
                releaseThird.countDown();
                await(() -> sender.stats().outstanding() == 0);
            } finally { finishFirst.countDown(); releaseCallback.countDown(); releaseThird.countDown(); }
        }
    }

    @Test void callbackCanCloseSenderWithoutWaitingForItsOwnNotificationThread() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        try (AsyncSender sender = new AsyncSender(options(Duration.ofSeconds(5), Duration.ofSeconds(3)))) {
            var result = sender.send(() -> () -> { hold(release); return success(); });
            var closedByCallback = result.thenRun(sender::close);
            release.countDown();
            closedByCallback.get(1, TimeUnit.SECONDS);
            assertTrue(sender.stats().closed());
        } finally { release.countDown(); }
    }
}
