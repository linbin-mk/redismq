package com.mk.mq.dashboard;

import org.springframework.boot.context.properties.ConfigurationProperties;
import java.util.List;

@ConfigurationProperties("redis-mq.dashboard")
public class DashboardProperties {
    private boolean enabled;
    private List<String> queues = List.of();
    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean value) { enabled = value; }
    public List<String> getQueues() { return queues; }
    public void setQueues(List<String> value) { queues = List.copyOf(value); }
    void validate() {
        if (queues.size() > 100) throw new IllegalArgumentException("看板预登记队列不能超过一百个");
    }
}
