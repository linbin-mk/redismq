package com.mk.mq;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.RedisNoScriptException;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** Redis 代理仅验证客户端线程、快照及脚本调用契约；真实 Redis 行为另由集成用例验证。 */
class RedisMqAsyncTest {
    @Test void snapshotsPayloadAndRecoversMissingScriptsOffCallingThread() throws Exception {
        Thread caller = Thread.currentThread();
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        List<String> bodies = new CopyOnWriteArrayList<>();
        AtomicInteger fallbacks = new AtomicInteger();
        RScript script = (RScript) Proxy.newProxyInstance(RScript.class.getClassLoader(), new Class<?>[]{RScript.class},
                (proxy, method, arguments) -> {
                    assertNotSame(caller, Thread.currentThread());
                    if (method.getName().equals("evalSha")) throw new RedisNoScriptException("NOSCRIPT");
                    if (method.getName().equals("eval")) {
                        fallbacks.incrementAndGet();
                        if (arguments[2] == RScript.ReturnType.INTEGER) {
                            entered.countDown(); assertTrue(release.await(3, TimeUnit.SECONDS)); return 1L;
                        }
                        Object[] params = (Object[]) arguments[4];
                        bodies.add((String) params[3]);
                        return "{\"id\":\"" + params[2] + "\",\"duplicate\":false}";
                    }
                    throw new AssertionError(method.getName());
                });
        RedissonClient redis = client(script);
        try (RedisMq mq = new RedisMq(redis, "async-test")) {
            List<String> payload = new ArrayList<>(List.of("before"));
            SendOptions options = SendOptions.immediate().withMessageId("stable-id");
            var future = mq.sendAsync("tasks", payload, options);
            try {
                assertTrue(entered.await(2, TimeUnit.SECONDS));
                payload.set(0, "after"); assertFalse(future.isDone());
                release.countDown();
                assertEquals(new SendResult("stable-id", false), future.get(3, TimeUnit.SECONDS));
                assertEquals(List.of("[\"before\"]"), bodies);
                assertEquals(2, fallbacks.get());
            } finally { release.countDown(); }
        }
    }

    @Test void validationAndSerializationFailSynchronouslyWithoutRedisAndReleaseCapacity() {
        RedissonClient redis = client(null);
        try (RedisMq mq = new RedisMq(redis, "async-test", new MqOptions(10, 8, Duration.ofHours(1)),
                new ObjectMapper(), new ProducerOptions(1, 1, Duration.ofSeconds(1), Duration.ZERO))) {
            assertThrows(IllegalArgumentException.class, () -> mq.sendAsync("bad:name", "x"));
            assertThrows(NullPointerException.class, () -> mq.sendAsync("tasks", null));
            assertThrows(IllegalArgumentException.class, () -> mq.sendAsync("tasks", "x".repeat(20)));
            assertEquals(0, mq.asyncSendStats().outstanding());
            assertEquals(0, mq.asyncSendStats().accepted());
            mq.close();
            assertThrows(SendRejectedException.class, () -> mq.sendAsync("tasks", Map.of()));
        }
    }

    @Test void closingMqLeavesSynchronousSendingAndExternalClientOwnedByCaller() {
        AtomicBoolean closed = new AtomicBoolean();
        RScript script = (RScript) Proxy.newProxyInstance(RScript.class.getClassLoader(), new Class<?>[]{RScript.class},
                (proxy, method, args) -> args[2] == RScript.ReturnType.INTEGER ? 1L : "{\"id\":\"sync\",\"duplicate\":false}");
        RedissonClient redis = (RedissonClient) Proxy.newProxyInstance(RedissonClient.class.getClassLoader(),
                new Class<?>[]{RedissonClient.class}, (proxy, method, args) -> {
                    if (method.getName().equals("shutdown")) { closed.set(true); return null; }
                    if (method.getName().equals("getScript")) return script;
                    throw new AssertionError(method.getName());
                });
        RedisMq mq = new RedisMq(redis, "async-test");
        mq.close();
        assertEquals("sync", mq.send("tasks", "payload").messageId());
        assertFalse(closed.get());
    }

    private static RedissonClient client(RScript script) {
        return (RedissonClient) Proxy.newProxyInstance(RedissonClient.class.getClassLoader(),
                new Class<?>[]{RedissonClient.class}, (proxy, method, arguments) -> {
                    if (method.getName().equals("getScript") && script != null) return script;
                    throw new AssertionError("不应调用 Redis: " + method.getName());
                });
    }
}
