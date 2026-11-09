package com.mk.mq.dashboard;

import com.mk.mq.RedisMq;
import org.springframework.beans.factory.ObjectProvider;
import com.mk.mq.spring.RedisMqAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/** 只注册看板接口；认证、授权及其他安全策略由宿主应用负责。 */
@AutoConfiguration(after = RedisMqAutoConfiguration.class)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@ConditionalOnBean(RedisMq.class)
@ConditionalOnProperty(prefix = "redis-mq.dashboard", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(DashboardProperties.class)
public class RedisMqDashboardAutoConfiguration {
    @Bean
    public DashboardController redisMqDashboardController(RedisMq mq, DashboardProperties properties, ObjectProvider<DashboardHistory> history) {
        properties.validate();
        properties.getQueues().forEach(mq::registerQueue);
        return new DashboardController(mq, history::getIfAvailable);
    }
}
