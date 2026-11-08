package com.mk.mq;

/** 不可恢复的业务错误，直接进入死信。 */
public class RejectMessageException extends RuntimeException {
    public RejectMessageException(String message) { super(message); }
}
