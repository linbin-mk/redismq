package com.mk.mq.spring;

import com.mk.mq.RedisMq;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

@AutoConfiguration
@ConditionalOnClass(RedissonClient.class)
@ConditionalOnBean(RedissonClient.class)
@ConditionalOnProperty(prefix = "redis-mq", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(RedisMqProperties.class)
public class RedisMqAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    public RedisMq redisMq(RedissonClient redis, RedisMqProperties properties, ObjectProvider<ObjectMapper> mappers, Environment environment) {
        String namespace = properties.getNamespace();
        if (namespace == null || namespace.isBlank()) {
            String profiles = String.join("-", environment.getActiveProfiles());
            namespace = environment.getProperty("spring.application.name", "application") + ":" + (profiles.isBlank() ? "default" : profiles);
        }
        ObjectMapper mapper = mappers.getIfUnique(() -> new ObjectMapper().findAndRegisterModules());
        return new RedisMq(redis, namespace, properties.mqOptions(), mapper, properties.getProducer().options());
    }

    @Bean
    @ConditionalOnMissingBean
    public RedisMqProducerLifecycle redisMqProducerLifecycle(RedisMq mq) {
        return new RedisMqProducerLifecycle(mq);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "redis-mq", name = "listeners-enabled", havingValue = "true", matchIfMissing = true)
    public RedisMqListenerRegistry redisMqListenerRegistry(RedisMq mq, RedisMqProperties properties,
                                                          ConfigurableListableBeanFactory beans, Environment environment) {
        return new RedisMqListenerRegistry(mq, properties, beans, environment);
    }
}
