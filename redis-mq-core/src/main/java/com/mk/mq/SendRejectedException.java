package com.mk.mq;

import java.util.concurrent.RejectedExecutionException;

/** 本地未受理：该次请求没有交给 Redis，可以由调用方决定是否重试。 */
public final class SendRejectedException extends RejectedExecutionException {
    public enum Reason { CAPACITY, CLOSED }
    private final Reason reason;

    public SendRejectedException(Reason reason) {
        super(reason == Reason.CAPACITY ? "本地异步发送容量已满" : "异步发送器已关闭");
        this.reason = reason;
    }

    public Reason reason() { return reason; }
}
