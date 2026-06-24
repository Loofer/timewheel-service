package io.github.timewheel.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ScheduledEntryTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void createBuildsEntryFromAcceptedMessage() {
        DelayedMessage message = new DelayedMessage(
                "trace-1",
                "orders.ready",
                "key-1",
                OBJECT_MAPPER.createObjectNode().put("orderId", "A-1"),
                Map.of("source", "test"),
                500L);
        Instant acceptedAt = Instant.parse("2026-06-24T01:00:00Z");

        ScheduledEntry entry = ScheduledEntry.create(message, acceptedAt);

        assertThat(entry.entryId()).isNotBlank();
        assertThat(entry.message()).isSameAs(message);
        assertThat(entry.acceptedAt()).isEqualTo(acceptedAt);
        assertThat(entry.remainingDelayMillis()).isEqualTo(500L);
    }
}
