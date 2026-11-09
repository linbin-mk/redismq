package com.mk.mq.dashboard.mysql;

import com.mk.mq.RedisMq;
import com.mk.mq.dashboard.DashboardHistory;
import com.mk.mq.spring.RedisMqAutoConfiguration;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class HistoryAutoConfigurationTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(RedisMqAutoConfiguration.class, MysqlHistoryAutoConfiguration.class))
            .withBean(RedissonClient.class, () -> (RedissonClient) Proxy.newProxyInstance(RedissonClient.class.getClassLoader(), new Class<?>[]{RedissonClient.class},
                    (proxy, method, args) -> { if (method.getName().equals("toString")) return "隔离客户端"; throw new AssertionError("未启用历史不应访问 Redis"); }), definition -> definition.setDestroyMethodName(""))
            .withPropertyValues("redis-mq.listeners-enabled=false");
    @Test void optionalModuleStaysOffAndDoesNotCreateOrReplaceBusinessDataSource() {
        runner.run(context -> assertThat(context).hasSingleBean(RedisMq.class).doesNotHaveBean(DashboardHistory.class)
                .doesNotHaveBean(HistoryDataSource.class).doesNotHaveBean(DataSource.class));
    }
    @Test void enabledHistoryRequiresExplicitSourceAndDatasourceConfiguration() {
        runner.withPropertyValues("redis-mq.dashboard.persistence.enabled=true").run(context -> assertThat(context).hasFailed());
    }
    @Test void independentHistoryPoolDoesNotBecomeTheBusinessDataSource() {
        DataSource existing = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, args) -> { if (method.getName().equals("toString")) return "业务数据源"; throw new AssertionError("不应使用业务连接"); });
        runner.withBean("business", DataSource.class, () -> existing)
                .withPropertyValues("redis-mq.dashboard.persistence.enabled=true", "redis-mq.dashboard.persistence.source-id=isolated",
                        "redis-mq.dashboard.persistence.datasource.url=jdbc:mysql://127.0.0.1:1/unavailable",
                        "redis-mq.dashboard.persistence.datasource.username=test")
                .run(context -> {
                    assertThat(context).hasSingleBean(DataSource.class).hasSingleBean(DashboardHistory.class);
                    assertSame(existing, context.getBean(DataSource.class));
                });
    }
    @Test void borrowingAnExistingDataSourceNeverClosesOrReconfiguresIt() {
        AtomicBoolean touched = new AtomicBoolean();
        DataSource existing = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, args) -> { touched.set(true); throw new AssertionError("不应调用宿主数据源方法：" + method.getName()); });
        StaticListableBeanFactory beans = new StaticListableBeanFactory(); beans.addBean("business", existing);
        var p = new PersistenceProperties(); p.getDatasource().setBeanName("business");
        try (var ignored = new HistoryDataSource(p, beans)) { assertFalse(touched.get()); }
        assertFalse(touched.get());
    }
}
