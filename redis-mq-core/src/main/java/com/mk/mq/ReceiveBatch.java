package com.mk.mq;

import java.util.List;

public record ReceiveBatch(List<Delivery> deliveries, long nextCheckAfterMillis) {
    public ReceiveBatch { deliveries = List.copyOf(deliveries); }
}
