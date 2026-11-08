package com.mk.mq.dashboard;

import org.springframework.boot.context.properties.ConfigurationProperties;
import java.util.List;

@ConfigurationProperties("redis-mq.dashboard")
public class DashboardProperties {
    private boolean enabled;
    private String username = "monitor";
    private String password;
    private List<String> queues = List.of();
    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean value) { enabled = value; }
    public String getUsername() { return username; }
    public void setUsername(String value) { username = value; }
    public String getPassword() { return password; }
    public void setPassword(String value) { password = value; }
    public List<String> getQueues() { return queues; }
    public void setQueues(List<String> value) { queues = List.copyOf(value); }
    void validate() {
        if (username == null || username.isBlank() || username.contains(":")) throw new IllegalArgumentException("看板管理员账号不能为空且不能包含冒号");
        if (password == null || password.length() < 16) throw new IllegalArgumentException("启用 MQ 看板需要通过外部配置提供至少十六位的独立管理员口令");
        if (queues.size() > 100) throw new IllegalArgumentException("看板预登记队列不能超过一百个");
    }
}
