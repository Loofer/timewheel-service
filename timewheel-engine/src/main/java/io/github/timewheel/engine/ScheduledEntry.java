package io.github.timewheel.engine;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record ScheduledEntry(
        String entryId,
        DelayedMessage message,
        long remainingDelayMillis,
        Instant acceptedAt) {

    public ScheduledEntry {
        if (entryId == null || entryId.isBlank()) {
            throw new IllegalArgumentException("entryId is required");
        }
        Objects.requireNonNull(message, "message is required");
        Objects.requireNonNull(acceptedAt, "acceptedAt is required");
    }

    public static ScheduledEntry create(DelayedMessage message, Instant acceptedAt) {
        return new ScheduledEntry(UUID.randomUUID().toString(), message, message.delayMillis(), acceptedAt);
    }
}
