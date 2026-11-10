package com.mk.mq;

/** 关闭等待已到期；outcomeUnknown 为 true 时不能据此判断消息未入队。 */
public final class SendShutdownException extends IllegalStateException {
    private final boolean outcomeUnknown;

    public SendShutdownException(boolean outcomeUnknown) {
        super(outcomeUnknown ? "发送器关闭，Redis 入队结果未知" : "发送器关闭，任务未发送到 Redis");
        this.outcomeUnknown = outcomeUnknown;
    }

    public boolean outcomeUnknown() { return outcomeUnknown; }
}
