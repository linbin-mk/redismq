package com.mk.mq.spring;

import com.mk.mq.RedisMq;
import com.mk.mq.SendRejectedException;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.redisson.api.RScript;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RedisMqAutoConfigurationTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(RedisMqAutoConfiguration.class));

    private static RedissonClient client() {
        return (RedissonClient) Proxy.newProxyInstance(RedissonClient.class.getClassLoader(), new Class<?>[]{RedissonClient.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("toString")) return "测试用客户端";
                    throw new AssertionError("无监听器时不应调用 Redis：" + method.getName());
                });
    }

    @Test
    void reusesExistingClientWithoutConnectingOrClosingIt() {
        RedissonClient redis = client();
        runner.withBean(RedissonClient.class, () -> redis, definition -> definition.setDestroyMethodName(""))
                .withPropertyValues("spring.application.name=sample-service", "spring.profiles.active=test")
                .run(context -> {
                    assertThat(context).hasSingleBean(RedisMq.class).hasSingleBean(RedisMqListenerRegistry.class);
                    assertThat(context.getBean(RedissonClient.class)).isSameAs(redis);
                    assertThat(context.getBean(RedisMq.class).storageKeys("demo").get(0)).contains("sample-service:test:demo");
                });
    }

    @Test
    void backsOffWithoutClientOrWhenDisabled() {
        runner.run(context -> assertThat(context).doesNotHaveBean(RedisMq.class));
        runner.withBean(RedissonClient.class, RedisMqAutoConfigurationTest::client, definition -> definition.setDestroyMethodName(""))
                .withPropertyValues("redis-mq.enabled=false")
                .run(context -> assertThat(context).doesNotHaveBean(RedisMq.class));
    }

    @Test
    void supportsProducerOnlyMode() {
        runner.withBean(RedissonClient.class, RedisMqAutoConfigurationTest::client, definition -> definition.setDestroyMethodName(""))
                .withPropertyValues("redis-mq.listeners-enabled=false")
                .run(context -> assertThat(context).hasSingleBean(RedisMq.class)
                        .hasSingleBean(RedisMqProducerLifecycle.class).doesNotHaveBean(RedisMqListenerRegistry.class));
    }

    @Test
    void bindsProducerOptionsAndClosesProducerOnlyClientOnContextStop() {
        RedisMq[] captured = new RedisMq[1];
        runner.withBean(RedissonClient.class, RedisMqAutoConfigurationTest::client, definition -> definition.setDestroyMethodName(""))
                .withPropertyValues("redis-mq.listeners-enabled=false", "redis-mq.producer.concurrency=2",
                        "redis-mq.producer.max-pending=3", "redis-mq.producer.send-timeout=150ms",
                        "redis-mq.producer.shutdown-timeout=0ms")
                .run(context -> {
                    captured[0] = context.getBean(RedisMq.class);
                    var properties = context.getBean(RedisMqProperties.class).getProducer().options();
                    assertThat(properties.concurrency()).isEqualTo(2);
                    assertThat(properties.maxPending()).isEqualTo(3);
                    assertThat(properties.sendTimeout()).hasMillis(150);
                    assertThat(properties.shutdownTimeout()).hasMillis(0);
                    assertThat(context.getBean(RedisMqProducerLifecycle.class).isRunning()).isTrue();
                });
        assertThat(captured[0].asyncSendStats().closed()).isTrue();
        assertThatThrownBy(() -> captured[0].sendAsync("demo", "payload")).isInstanceOf(SendRejectedException.class);
    }

    @Test
    void rejectsInvalidProducerConfigurationWithoutConnectingToRedis() {
        runner.withBean(RedissonClient.class, RedisMqAutoConfigurationTest::client, definition -> definition.setDestroyMethodName(""))
                .withPropertyValues("redis-mq.producer.concurrency=0")
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void producerOnlyShutdownDrainsBeforeExternalRedisClientIsDestroyed() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        List<String> order = new CopyOnWriteArrayList<>();
        RScript script = (RScript) Proxy.newProxyInstance(RScript.class.getClassLoader(), new Class<?>[]{RScript.class},
                (proxy, method, args) -> {
                    if (args[2] == RScript.ReturnType.INTEGER) return 1L;
                    entered.countDown();
                    if (!release.await(3, TimeUnit.SECONDS)) throw new AssertionError("发送未获释放");
                    order.add("sent");
                    return "{\"id\":\"task\",\"duplicate\":false}";
                });
        RedissonClient redis = (RedissonClient) Proxy.newProxyInstance(RedissonClient.class.getClassLoader(),
                new Class<?>[]{RedissonClient.class}, (proxy, method, args) -> {
                    if (method.getName().equals("getScript")) return script;
                    if (method.getName().equals("shutdown")) { order.add("redis-closed"); return null; }
                    throw new AssertionError(method.getName());
                });
        var releases = Executors.newSingleThreadScheduledExecutor();
        java.util.concurrent.atomic.AtomicReference<CompletableFuture<com.mk.mq.SendResult>> result = new java.util.concurrent.atomic.AtomicReference<>();
        try {
            runner.withBean(RedissonClient.class, () -> redis, definition -> definition.setDestroyMethodName("shutdown"))
                    .withPropertyValues("redis-mq.listeners-enabled=false", "redis-mq.producer.shutdown-timeout=2s")
                    .run(context -> {
                        result.set(context.getBean(RedisMq.class).sendAsync("demo", "payload"));
                        assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
                        releases.schedule(release::countDown, 100, TimeUnit.MILLISECONDS);
                    });
            assertThat(result.get().get(1, TimeUnit.SECONDS).messageId()).isEqualTo("task");
            assertThat(order).containsExactly("sent", "redis-closed");
        } finally { release.countDown(); releases.shutdownNow(); }
    }

    @Test
    void rejectsInvalidListenerBeforeConsuming() {
        runner.withBean(RedissonClient.class, RedisMqAutoConfigurationTest::client, definition -> definition.setDestroyMethodName(""))
                .withBean(InvalidListener.class)
                .run(context -> assertThat(context).hasFailed());
    }

    static class InvalidListener {
        @RedisMqListener(queue = "demo")
        public String receive(String value) { return value; }
    }
}
