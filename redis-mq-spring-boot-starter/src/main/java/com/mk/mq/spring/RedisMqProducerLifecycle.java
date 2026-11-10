package com.mk.mq.spring;

import com.mk.mq.RedisMq;
import org.springframework.context.SmartLifecycle;

/** 消费者先停止，随后排空生产端；即使关闭监听器也注册此生命周期。 */
public final class RedisMqProducerLifecycle implements SmartLifecycle {
    private final RedisMq mq;
    private volatile boolean running;

    RedisMqProducerLifecycle(RedisMq mq) { this.mq = mq; }

    @Override public void start() { running = true; }
    @Override public void stop() { running = false; mq.close(); }
    @Override public boolean isRunning() { return running; }
    @Override public int getPhase() { return Integer.MAX_VALUE - 200; }
}
