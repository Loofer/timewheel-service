package io.github.timewheel.engine;

import java.time.Duration;

public record TimewheelSettings(Duration tickDuration, int ticksPerWheel, int maxCycle) {
    public TimewheelSettings {
        if (tickDuration == null || tickDuration.isZero() || tickDuration.isNegative()) {
            throw new IllegalArgumentException("tickDuration must be greater than zero");
        }
        if (tickDuration.toMillis() <= 0) {
            throw new IllegalArgumentException("tickDuration must be at least one millisecond");
        }
        if (ticksPerWheel <= 0) {
            throw new IllegalArgumentException("ticksPerWheel must be greater than zero");
        }
        if (maxCycle <= 0) {
            throw new IllegalArgumentException("maxCycle must be greater than zero");
        }
        try {
            Math.multiplyExact(Math.multiplyExact(tickDuration.toMillis(), ticksPerWheel), maxCycle);
        } catch (ArithmeticException ex) {
            throw new IllegalArgumentException("maxDelayMillis would overflow", ex);
        }
    }

    public long maxDelayMillis() {
        return Math.multiplyExact(Math.multiplyExact(tickDuration.toMillis(), ticksPerWheel), maxCycle);
    }

    public void validateDelay(long delayMillis) {
        long maxDelayMillis = maxDelayMillis();
        if (delayMillis > maxDelayMillis) {
            throw new SchedulingException(
                    "DELAY_OUT_OF_RANGE",
                    "delayMillis exceeds max wheel range of " + maxDelayMillis + " ms");
        }
    }
}
