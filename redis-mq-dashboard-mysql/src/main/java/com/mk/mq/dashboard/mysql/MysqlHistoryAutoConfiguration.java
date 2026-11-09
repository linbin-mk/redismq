package com.mk.mq.dashboard.mysql;

import com.mk.mq.RedisMq;
import com.mk.mq.dashboard.DashboardHistory;
import com.mk.mq.dashboard.RedisMqDashboardAutoConfiguration;
import com.mk.mq.spring.RedisMqAutoConfiguration;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@AutoConfiguration(after = {RedisMqAutoConfiguration.class, RedisMqDashboardAutoConfiguration.class})
@ConditionalOnBean(RedisMq.class)
@ConditionalOnMissingBean(DashboardHistory.class)
@ConditionalOnProperty(prefix = "redis-mq.dashboard.persistence", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(PersistenceProperties.class)
public class MysqlHistoryAutoConfiguration {
    @Bean(destroyMethod = "close")
    public HistoryDataSource redisMqHistoryDataSource(PersistenceProperties properties, BeanFactory beans) {
        properties.validate();
        return new HistoryDataSource(properties, beans);
    }
    @Bean(initMethod = "start", destroyMethod = "close")
    public MysqlHistory redisMqMysqlHistory(RedisMq mq, PersistenceProperties properties, HistoryDataSource source) {
        return new MysqlHistory(mq, properties, source);
    }
}
