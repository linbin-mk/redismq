package com.mk.mq;

public record SendResult(String messageId, boolean duplicate) { }
