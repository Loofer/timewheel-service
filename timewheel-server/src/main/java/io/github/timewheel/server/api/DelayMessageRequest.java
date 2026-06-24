package io.github.timewheel.server.api;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.timewheel.engine.DelayedMessage;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.Map;

public record DelayMessageRequest(
        String id,
        @NotBlank String targetTopic,
        String key,
        @NotNull JsonNode payload,
        Map<String, String> headers,
        long delayMillis) {

    public DelayedMessage toMessage() {
        return new DelayedMessage(id, targetTopic, key, payload, headers, delayMillis);
    }
}
