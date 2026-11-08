package com.mk.mq.dashboard;

import com.mk.mq.RedisMq;
import com.mk.mq.spring.RedisMqAutoConfiguration;
import org.junit.jupiter.api.Test;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.core.annotation.Order;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.httpBasic;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest(classes = DashboardTest.Application.class, properties = {
        "redis-mq.dashboard.enabled=true", "redis-mq.listeners-enabled=false", "redis-mq.namespace=dashboard:test"})
@AutoConfigureMockMvc
class DashboardTest {
    private static final String PASSWORD = UUID.randomUUID().toString();
    private static final AtomicInteger writes = new AtomicInteger();
    @Autowired private MockMvc mvc;
    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) { registry.add("redis-mq.dashboard.password", () -> PASSWORD); }

    @Test
    void requiresIndependentAuthenticationForPageAndApi() throws Exception {
        mvc.perform(get("/redis-mq/dashboard/")).andExpect(status().isUnauthorized());
        mvc.perform(get("/redis-mq/dashboard/api/overview")).andExpect(status().isUnauthorized());
        mvc.perform(get("/redis-mq/dashboard/api/overview").with(httpBasic("monitor", UUID.randomUUID().toString())))
                .andExpect(status().isUnauthorized());
    }
    @Test
    void servesSelfContainedResourcesAndSecurityHeaders() throws Exception {
        mvc.perform(get("/redis-mq/dashboard").with(httpBasic("monitor", PASSWORD)))
                .andExpect(status().isFound()).andExpect(header().string("Location", "/redis-mq/dashboard/"));
        var response = mvc.perform(get("/redis-mq/dashboard/").with(httpBasic("monitor", PASSWORD)))
                .andExpect(status().isOk()).andExpect(header().string("Content-Security-Policy", org.hamcrest.Matchers.containsString("script-src 'self'")))
                .andReturn().getResponse();
        assertThat(response.getContentAsString(StandardCharsets.UTF_8)).contains("消息运行概览");
        mvc.perform(get("/redis-mq/dashboard/dashboard.js").with(httpBasic("monitor", PASSWORD)))
                .andExpect(status().isOk()).andExpect(content().string(org.hamcrest.Matchers.containsString("textContent")));
        mvc.perform(get("/redis-mq/dashboard/dashboard.css").with(httpBasic("monitor", PASSWORD))).andExpect(status().isOk());
    }
    @Test
    void overviewUsesReadOnlyQueriesAndReportsPartialFailure() throws Exception {
        int before = writes.get();
        mvc.perform(get("/redis-mq/dashboard/api/overview").with(httpBasic("monitor", PASSWORD)))
                .andExpect(status().isOk()).andExpect(jsonPath("$.namespace").value("dashboard:test"))
                .andExpect(jsonPath("$.totalQueues").value(2)).andExpect(jsonPath("$.queues[0].stats.ready").value(2))
                .andExpect(jsonPath("$.queues[1].stats").isEmpty()).andExpect(jsonPath("$.queues[1].error").isNotEmpty());
        assertThat(writes.get()).isEqualTo(before);
    }
    @Test
    void listHidesPayloadAndReceiptWhileDetailsExposeOnlyPublicSnapshot() throws Exception {
        mvc.perform(get("/redis-mq/dashboard/api/queues/demo/messages").with(httpBasic("monitor", PASSWORD)))
                .andExpect(status().isOk()).andExpect(jsonPath("$[0].id").value("message-1"))
                .andExpect(jsonPath("$[0].payload").doesNotExist()).andExpect(jsonPath("$[0].receipt").doesNotExist());
        mvc.perform(get("/redis-mq/dashboard/api/queues/demo/messages/message-1").with(httpBasic("monitor", PASSWORD)))
                .andExpect(status().isOk()).andExpect(jsonPath("$.payload").value("{\"内容\":\"<script>测试</script>\"}"))
                .andExpect(jsonPath("$.receipt").doesNotExist());
    }
    @Test
    void rejectsInvalidPaginationAndStateAndHandlesGoneMessage() throws Exception {
        mvc.perform(get("/redis-mq/dashboard/api/overview?count=101").with(httpBasic("monitor", PASSWORD))).andExpect(status().isBadRequest());
        mvc.perform(get("/redis-mq/dashboard/api/queues/demo/messages?state=unknown").with(httpBasic("monitor", PASSWORD))).andExpect(status().isBadRequest());
        mvc.perform(get("/redis-mq/dashboard/api/queues/demo/messages/gone").with(httpBasic("monitor", PASSWORD))).andExpect(status().isNotFound());
    }
    @Test
    void doesNotOfferMutationEndpointsOrChangeHostSecurity() throws Exception {
        int before = writes.get();
        mvc.perform(post("/redis-mq/dashboard/api/queues/demo/replay").with(httpBasic("monitor", PASSWORD)).with(csrf()))
                .andExpect(status().isForbidden());
        mvc.perform(get("/business/public")).andExpect(status().isOk());
        mvc.perform(get("/business/private").with(httpBasic("monitor", PASSWORD))).andExpect(status().isForbidden());
        assertThat(writes.get()).isEqualTo(before);
    }
    @Test
    void staysDisabledByDefaultAndFailsClosedWithoutPassword() {
        var runner = new WebApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(RedisMqAutoConfiguration.class, RedisMqDashboardAutoConfiguration.class))
                .withBean(RedissonClient.class, DashboardTest::client, definition -> definition.setDestroyMethodName(""))
                .withPropertyValues("redis-mq.listeners-enabled=false");
        runner.run(context -> assertThat(context).doesNotHaveBean(DashboardController.class));
        runner.withPropertyValues("redis-mq.dashboard.enabled=true").run(context -> assertThat(context).hasFailed());
    }

    private static RedissonClient client() {
        return (RedissonClient) Proxy.newProxyInstance(RedissonClient.class.getClassLoader(), new Class<?>[]{RedissonClient.class}, (proxy, method, args) -> {
            if (method.getName().equals("getScoredSortedSet")) return Proxy.newProxyInstance(RScoredSortedSet.class.getClassLoader(), new Class<?>[]{RScoredSortedSet.class}, (p, m, a) -> switch (m.getName()) {
                case "valueRange" -> List.of("demo", "unavailable");
                case "size" -> 2;
                case "add" -> { writes.incrementAndGet(); yield true; }
                default -> throw new AssertionError("未预期的目录操作：" + m.getName());
            });
            if (method.getName().equals("getScript")) return Proxy.newProxyInstance(RScript.class.getClassLoader(), new Class<?>[]{RScript.class}, (p, m, a) -> {
                if (!m.getName().equals("eval") && !m.getName().equals("evalSha")) throw new AssertionError("未预期的脚本调用");
                assertThat(a[0]).isEqualTo(RScript.Mode.READ_ONLY);
                var keys = (List<?>) a[3];
                if (keys.get(0).toString().contains(":unavailable}")) throw new IllegalStateException("模拟单队列读取失败");
                Object[] parameters = (Object[]) a[4];
                return switch (parameters[0].toString()) {
                    case "stats" -> List.of(2L, 8L, 1L, 0L, 1L, 12L, 1024L, 12L);
                    case "list" -> "[" + message() + "]";
                    case "get" -> parameters[1].equals("gone") ? List.of() : List.of(message(), "{\"内容\":\"<script>测试</script>\"}");
                    default -> throw new AssertionError("看板不允许写操作：" + parameters[0]);
                };
            });
            if (method.getName().equals("toString")) return "隔离测试客户端";
            throw new AssertionError("未预期的客户端调用：" + method.getName());
        });
    }
    private static String message() {
        return "{\"version\":1,\"id\":\"message-1\",\"payload\":\"{\\\"内容\\\":\\\"<script>测试</script>\\\"}\",\"state\":\"SCHEDULED\",\"deliveryCount\":0,\"maxDeliveries\":5,\"createdAt\":1794100000000,\"availableAt\":1794121600000,\"leaseUntil\":0,\"lastError\":\"\",\"ready\":false}";
    }
    @SpringBootConfiguration
    @EnableAutoConfiguration
    static class Application {
        @Bean(destroyMethod = "") RedissonClient redis() { return client(); }
        @Bean HostController hostController() { return new HostController(); }
        @Bean @Order(0) SecurityFilterChain hostSecurity(HttpSecurity http) throws Exception {
            return http.securityMatcher("/business/**").authorizeHttpRequests(requests -> requests.requestMatchers("/business/public").permitAll().anyRequest().denyAll()).build();
        }
    }
    @RestController
    static class HostController {
        @GetMapping("/business/public") String example() { return "宿主公开接口"; }
    }
}
