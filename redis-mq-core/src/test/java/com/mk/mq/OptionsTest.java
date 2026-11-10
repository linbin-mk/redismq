package com.mk.mq;

import org.junit.jupiter.api.Test;
import java.time.Duration;
import static org.junit.jupiter.api.Assertions.*;

class OptionsTest {
    @Test void validatesProducerBoundsAndTimeouts() {
        assertThrows(IllegalArgumentException.class, () -> new ProducerOptions(0, 1, Duration.ofSeconds(1), Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> new ProducerOptions(257, 1, Duration.ofSeconds(1), Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> new ProducerOptions(1, 0, Duration.ofSeconds(1), Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> new ProducerOptions(1, 100001, Duration.ofSeconds(1), Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> new ProducerOptions(1, 1, Duration.ZERO, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> new ProducerOptions(1, 1, Duration.ofNanos(1), Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> new ProducerOptions(1, 1, Duration.ofSeconds(1), Duration.ofMillis(-1)));
    }
    @Test void rejectsInvalidCapacityAndOversizedMessageConfiguration() {
        assertThrows(IllegalArgumentException.class, () -> new MqOptions(0, 64, Duration.ofHours(1)));
        assertThrows(IllegalArgumentException.class, () -> new MqOptions(10, 65537, Duration.ofHours(1)));
        assertThrows(IllegalArgumentException.class, () -> new MqOptions(10, 64, Duration.ofHours(1), 63, 10));
        assertThrows(IllegalArgumentException.class, () -> new MqOptions(10, 64, Duration.ofHours(1), 1024, 0));
        assertThrows(IllegalArgumentException.class, () -> new MqOptions(10, 64, Duration.ofHours(1), 1024, 10, 0));
    }
    @Test void rejectsFractionalMillisecondsOverflowAndInvalidRetryPolicy() {
        assertThrows(IllegalArgumentException.class, () -> SendOptions.delayed(Duration.ofNanos(1)));
        assertThrows(IllegalArgumentException.class, () -> SendOptions.delayed(Duration.ofSeconds(Long.MAX_VALUE)));
        assertThrows(IllegalArgumentException.class, () -> SendOptions.delayed(Duration.ofMillis(-1)));
        assertThrows(IllegalArgumentException.class, () -> SendOptions.immediate().withRetry(0, Duration.ofSeconds(1), Duration.ofSeconds(2)));
        assertThrows(IllegalArgumentException.class, () -> SendOptions.immediate().withRetry(2, Duration.ofSeconds(2), Duration.ofSeconds(1)));
    }
    @Test void queueNamesCannotAliasAcrossNamespaceBoundaries() {
        assertEquals("worker-job", Checks.queue("worker-job"));
        assertEquals("app:prod", Checks.name("app:prod", "命名空间"));
        assertThrows(IllegalArgumentException.class, () -> Checks.queue("prod:worker-job"));
        assertThrows(IllegalArgumentException.class, () -> Checks.queue("worker{job}"));
        assertThrows(IllegalArgumentException.class, () -> Checks.queue("x".repeat(161)));
    }
}
