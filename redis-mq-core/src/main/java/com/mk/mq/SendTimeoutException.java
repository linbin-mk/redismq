package com.mk.mq;

import java.util.concurrent.TimeoutException;

/** 异步发送期限已过；outcomeUnknown 为 true 时，消息仍可能已入队或稍后入队。 */
public final class SendTimeoutException extends TimeoutException {
    private final boolean outcomeUnknown;

    public SendTimeoutException(boolean outcomeUnknown) {
        super(outcomeUnknown ? "异步发送超时，Redis 入队结果未知" : "异步发送排队超时，未发送到 Redis");
        this.outcomeUnknown = outcomeUnknown;
    }

    public boolean outcomeUnknown() { return outcomeUnknown; }
}
