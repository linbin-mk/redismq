package com.mk.mq;

/** 凭证仅属于本次领取；重投后旧凭证失效。 */
public record Delivery(MessageSnapshot message, String receipt) { }
