package com.mk.mq.spring;

import com.mk.mq.ConsumerOptions;
import com.mk.mq.MqOptions;
import com.mk.mq.ProducerOptions;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties("redis-mq")
public class RedisMqProperties {
    private boolean enabled = true;
    private boolean listenersEnabled = true;
    private String namespace;
    private int maxQueueSize = 100000;
    private int maxMessageBytes = 65536;
    private long maxQueueBytes = 64L * 1024 * 1024;
    private int maxDedupEntries = 200000;
    private int maxRegisteredQueues = 1024;
    private Duration idempotencyRetention = Duration.ofHours(24);
    private int concurrency = 4;
    private Duration visibility = Duration.ofSeconds(60);
    private Duration maxProcessingTime = Duration.ofMinutes(5);
    private Duration pollInterval = Duration.ofMillis(500);
    private Duration shutdownTimeout = Duration.ofSeconds(30);
    private final Producer producer = new Producer();

    public Producer getProducer() { return producer; }

    public static class Producer {
        private int concurrency = 4;
        private int maxPending = 256;
        private Duration sendTimeout = Duration.ofSeconds(5);
        private Duration shutdownTimeout = Duration.ofSeconds(30);

        public ProducerOptions options() { return new ProducerOptions(concurrency, maxPending, sendTimeout, shutdownTimeout); }
        public int getConcurrency() { return concurrency; }
        public void setConcurrency(int value) { concurrency = value; }
        public int getMaxPending() { return maxPending; }
        public void setMaxPending(int value) { maxPending = value; }
        public Duration getSendTimeout() { return sendTimeout; }
        public void setSendTimeout(Duration value) { sendTimeout = value; }
        public Duration getShutdownTimeout() { return shutdownTimeout; }
        public void setShutdownTimeout(Duration value) { shutdownTimeout = value; }
    }

    public MqOptions mqOptions() { return new MqOptions(maxQueueSize, maxMessageBytes, idempotencyRetention, maxQueueBytes, maxDedupEntries, maxRegisteredQueues); }
    public ConsumerOptions consumerOptions(int overrideConcurrency) {
        return new ConsumerOptions(overrideConcurrency == 0 ? concurrency : overrideConcurrency, visibility, maxProcessingTime, pollInterval, shutdownTimeout);
    }
    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public boolean isListenersEnabled() { return listenersEnabled; }
    public void setListenersEnabled(boolean listenersEnabled) { this.listenersEnabled = listenersEnabled; }
    public String getNamespace() { return namespace; }
    public void setNamespace(String namespace) { this.namespace = namespace; }
    public int getMaxQueueSize() { return maxQueueSize; }
    public void setMaxQueueSize(int value) { maxQueueSize = value; }
    public int getMaxMessageBytes() { return maxMessageBytes; }
    public void setMaxMessageBytes(int value) { maxMessageBytes = value; }
    public int getMaxRegisteredQueues() { return maxRegisteredQueues; }
    public void setMaxRegisteredQueues(int value) { maxRegisteredQueues = value; }
    public long getMaxQueueBytes() { return maxQueueBytes; }
    public void setMaxQueueBytes(long value) { maxQueueBytes = value; }
    public int getMaxDedupEntries() { return maxDedupEntries; }
    public void setMaxDedupEntries(int value) { maxDedupEntries = value; }
    public Duration getIdempotencyRetention() { return idempotencyRetention; }
    public void setIdempotencyRetention(Duration value) { idempotencyRetention = value; }
    public int getConcurrency() { return concurrency; }
    public void setConcurrency(int value) { concurrency = value; }
    public Duration getVisibility() { return visibility; }
    public void setVisibility(Duration value) { visibility = value; }
    public Duration getMaxProcessingTime() { return maxProcessingTime; }
    public void setMaxProcessingTime(Duration value) { maxProcessingTime = value; }
    public Duration getPollInterval() { return pollInterval; }
    public void setPollInterval(Duration value) { pollInterval = value; }
    public Duration getShutdownTimeout() { return shutdownTimeout; }
    public void setShutdownTimeout(Duration value) { shutdownTimeout = value; }
}
