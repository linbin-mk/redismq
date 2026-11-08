package com.mk.mq.spring;

import com.mk.mq.RedisMq;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.lang.reflect.Proxy;

import static org.assertj.core.api.Assertions.assertThat;

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
                .run(context -> assertThat(context).hasSingleBean(RedisMq.class).doesNotHaveBean(RedisMqListenerRegistry.class));
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
