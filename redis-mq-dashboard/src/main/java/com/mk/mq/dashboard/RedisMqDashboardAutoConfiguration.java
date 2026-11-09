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
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpMethod;
import org.springframework.security.authentication.ProviderManager;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;

/** 独立过滤链只匹配看板路径，宿主其他接口的认证策略保持由宿主管理。 */
@AutoConfiguration(after = RedisMqAutoConfiguration.class)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@ConditionalOnBean(RedisMq.class)
@ConditionalOnProperty(prefix = "redis-mq.dashboard", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(DashboardProperties.class)
@EnableWebSecurity
public class RedisMqDashboardAutoConfiguration {
    @Bean
    @Order(-100)
    public SecurityFilterChain redisMqDashboardSecurity(HttpSecurity http, DashboardProperties properties) throws Exception {
        properties.validate();
        var encoder = new BCryptPasswordEncoder();
        var users = new InMemoryUserDetailsManager(User.withUsername(properties.getUsername())
                .password(encoder.encode(properties.getPassword())).roles("MQ_MONITOR").build());
        var provider = new DaoAuthenticationProvider(users);
        provider.setPasswordEncoder(encoder);
        http.securityMatcher(DashboardController.BASE, DashboardController.BASE + "/**")
                .authenticationManager(new ProviderManager(provider))
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(authorize -> authorize.requestMatchers(HttpMethod.GET, DashboardController.BASE, DashboardController.BASE + "/**")
                        .hasRole("MQ_MONITOR").anyRequest().denyAll())
                .httpBasic(basic -> basic.realmName("Redis MQ Monitor"))
                .requestCache(cache -> cache.disable())
                .headers(headers -> headers.contentSecurityPolicy(policy -> policy.policyDirectives(
                        "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'")));
        return http.build();
    }

    @Bean
    public DashboardController redisMqDashboardController(RedisMq mq, DashboardProperties properties, ObjectProvider<DashboardHistory> history) {
        properties.validate();
        properties.getQueues().forEach(mq::registerQueue);
        return new DashboardController(mq, history::getIfAvailable);
    }
}
