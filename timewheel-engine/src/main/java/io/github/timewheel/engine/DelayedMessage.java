package io.github.timewheel.engine;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public final class DelayedMessage {

    private final String id;
    private final String targetTopic;
    private final String key;
    private final JsonNode payload;
    private final Map<String, String> headers;
    private final long delayMillis;

    @JsonCreator
    public DelayedMessage(
            @JsonProperty("id") String id,
            @JsonProperty("targetTopic") String targetTopic,
            @JsonProperty("key") String key,
            @JsonProperty("payload") JsonNode payload,
            @JsonProperty("headers") Map<String, String> headers,
            @JsonProperty("delayMillis") long delayMillis) {
        if (targetTopic == null || targetTopic.isBlank()) {
            throw new IllegalArgumentException("targetTopic is required");
        }
        if (payload == null || !payload.isObject()) {
            throw new IllegalArgumentException("payload must be a JSON object");
        }
        this.id = id;
        this.targetTopic = targetTopic;
        this.key = key;
        this.payload = payload.deepCopy();
        this.headers = headers == null ? Map.of() : Map.copyOf(headers);
        this.delayMillis = delayMillis;
    }

    @JsonIgnore
    public Optional<String> id() {
        return optionalNonBlank(id);
    }

    @JsonProperty("id")
    public String idValue() {
        return id;
    }

    @JsonProperty("targetTopic")
    public String targetTopic() {
        return targetTopic;
    }

    @JsonIgnore
    public Optional<String> key() {
        return optionalNonBlank(key);
    }

    @JsonProperty("key")
    public String keyValue() {
        return key;
    }

    @JsonProperty("payload")
    public JsonNode payload() {
        return payload.deepCopy();
    }

    @JsonProperty("headers")
    public Map<String, String> headers() {
        return headers;
    }

    @JsonProperty("delayMillis")
    public long delayMillis() {
        return delayMillis;
    }

    @JsonIgnore
    public boolean immediate() {
        return delayMillis <= 0;
    }

    private static Optional<String> optionalNonBlank(String value) {
        return value == null || value.isBlank() ? Optional.empty() : Optional.of(value);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof DelayedMessage that)) {
            return false;
        }
        return delayMillis == that.delayMillis
                && Objects.equals(id, that.id)
                && Objects.equals(targetTopic, that.targetTopic)
                && Objects.equals(key, that.key)
                && Objects.equals(payload, that.payload)
                && Objects.equals(headers, that.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, targetTopic, key, payload, headers, delayMillis);
    }

    @Override
    public String toString() {
        return "DelayedMessage["
                + "id=" + id
                + ", targetTopic=" + targetTopic
                + ", key=" + key
                + ", payload=" + payload
                + ", headers=" + headers
                + ", delayMillis=" + delayMillis
                + ']';
    }
}
