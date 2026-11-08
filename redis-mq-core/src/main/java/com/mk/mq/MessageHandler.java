package com.mk.mq;

@FunctionalInterface
public interface MessageHandler<T> {
    void handle(T payload, MessageContext context) throws Exception;
}
