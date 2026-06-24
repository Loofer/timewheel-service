package io.github.timewheel.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DelayedMessageTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void acceptsJsonObjectPayloadAndStringHeaders() {
        JsonNode payload = OBJECT_MAPPER.createObjectNode().put("orderId", "A-1");
        DelayedMessage message = new DelayedMessage(
                "trace-1",
                "orders.ready",
                "order-key",
                payload,
                Map.of("source", "test"),
                250L);

        assertThat(message.id()).contains("trace-1");
        assertThat(message.targetTopic()).isEqualTo("orders.ready");
        assertThat(message.key()).contains("order-key");
        assertThat(message.payload()).isEqualTo(payload);
        assertThat(message.headers()).containsEntry("source", "test");
        assertThat(message.delayMillis()).isEqualTo(250L);
    }

    @Test
    void idAndKeyFilterNullAndBlankValues() {
        JsonNode payload = OBJECT_MAPPER.createObjectNode();

        assertThat(new DelayedMessage(null, "topic", null, payload, Map.of(), 1L).id()).isEmpty();
        assertThat(new DelayedMessage("  ", "topic", " \t", payload, Map.of(), 1L).id()).isEmpty();
        assertThat(new DelayedMessage("id", "topic", " \t", payload, Map.of(), 1L).key()).isEmpty();
    }

    @Test
    void rejectsBlankTargetTopic() {
        JsonNode payload = OBJECT_MAPPER.createObjectNode();

        assertThatThrownBy(() -> new DelayedMessage("id", " ", "key", payload, Map.of(), 1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("targetTopic");
    }

    @Test
    void rejectsNullOrNonObjectPayload() throws Exception {
        JsonNode arrayPayload = OBJECT_MAPPER.readTree("[1]");

        assertThatThrownBy(() -> new DelayedMessage("id", "topic", "key", null, Map.of(), 1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("payload");
        assertThatThrownBy(() -> new DelayedMessage("id", "topic", "key", arrayPayload, Map.of(), 1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("payload");
    }

    @Test
    void nullHeadersBecomeEmptyImmutableMap() {
        DelayedMessage message = new DelayedMessage(
                "id",
                "topic",
                "key",
                OBJECT_MAPPER.createObjectNode(),
                null,
                1L);

        assertThat(message.headers()).isEmpty();
        assertThatThrownBy(() -> message.headers().put("x", "y"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void headersAreDefensivelyCopiedAndImmutable() {
        Map<String, String> headers = new HashMap<>();
        headers.put("source", "original");

        DelayedMessage message = new DelayedMessage(
                "id",
                "topic",
                "key",
                OBJECT_MAPPER.createObjectNode(),
                headers,
                1L);
        headers.put("source", "changed");

        assertThat(message.headers()).containsEntry("source", "original");
        assertThatThrownBy(() -> message.headers().put("other", "value"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void rejectsNullHeaderNamesOrValues() {
        Map<String, String> nullName = new HashMap<>();
        nullName.put(null, "value");
        Map<String, String> nullValue = new HashMap<>();
        nullValue.put("name", null);

        assertThatThrownBy(() -> new DelayedMessage(
                "id",
                "topic",
                "key",
                OBJECT_MAPPER.createObjectNode(),
                nullName,
                1L))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new DelayedMessage(
                "id",
                "topic",
                "key",
                OBJECT_MAPPER.createObjectNode(),
                nullValue,
                1L))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void immediateIsTrueWhenDelayIsZeroOrNegative() {
        JsonNode payload = OBJECT_MAPPER.createObjectNode();

        assertThat(new DelayedMessage("id", "topic", "key", payload, Map.of(), 0L).immediate()).isTrue();
        assertThat(new DelayedMessage("id", "topic", "key", payload, Map.of(), -1L).immediate()).isTrue();
        assertThat(new DelayedMessage("id", "topic", "key", payload, Map.of(), 1L).immediate()).isFalse();
    }

    @Test
    void serializesAllMessageFieldsWithStableJsonNames() {
        DelayedMessage message = new DelayedMessage(
                "trace-1",
                "orders.ready",
                "order-key",
                OBJECT_MAPPER.createObjectNode().put("orderId", "A-1"),
                Map.of("source", "test"),
                250L);

        JsonNode json = OBJECT_MAPPER.valueToTree(message);

        assertThat(json.get("id").asText()).isEqualTo("trace-1");
        assertThat(json.get("targetTopic").asText()).isEqualTo("orders.ready");
        assertThat(json.get("key").asText()).isEqualTo("order-key");
        assertThat(json.get("payload").get("orderId").asText()).isEqualTo("A-1");
        assertThat(json.get("headers").get("source").asText()).isEqualTo("test");
        assertThat(json.get("delayMillis").asLong()).isEqualTo(250L);
        assertThat(json.has("rawId")).isFalse();
        assertThat(json.has("rawKey")).isFalse();
        assertThat(json.has("idValue")).isFalse();
        assertThat(json.has("keyValue")).isFalse();
        assertThat(json.has("immediate")).isFalse();
        assertThat(json.has("empty")).isFalse();
        assertThat(json.has("present")).isFalse();
    }

    @Test
    void deserializesMessageFieldsWithStableJsonNames() throws Exception {
        DelayedMessage message = OBJECT_MAPPER.readValue("""
                {
                  "id": "trace-1",
                  "targetTopic": "orders.ready",
                  "key": "order-key",
                  "payload": {"orderId": "A-1"},
                  "headers": {"source": "test"},
                  "delayMillis": 250
                }
                """, DelayedMessage.class);

        assertThat(message.id()).contains("trace-1");
        assertThat(message.targetTopic()).isEqualTo("orders.ready");
        assertThat(message.key()).contains("order-key");
        assertThat(message.payload().get("orderId").asText()).isEqualTo("A-1");
        assertThat(message.headers()).containsEntry("source", "test");
        assertThat(message.delayMillis()).isEqualTo(250L);
    }

    @Test
    void deserializationRejectsMissingRequiredFields() {
        assertThatThrownBy(() -> OBJECT_MAPPER.readValue("""
                {
                  "payload": {"orderId": "A-1"},
                  "delayMillis": 250
                }
                """, DelayedMessage.class))
                .isInstanceOf(ValueInstantiationException.class)
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage("targetTopic is required");

        assertThatThrownBy(() -> OBJECT_MAPPER.readValue("""
                {
                  "targetTopic": "orders.ready",
                  "delayMillis": 250
                }
                """, DelayedMessage.class))
                .isInstanceOf(ValueInstantiationException.class)
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage("payload must be a JSON object");
    }

    @Test
    void payloadIsDefensivelyCopied() {
        ObjectNode payload = OBJECT_MAPPER.createObjectNode().put("orderId", "A-1");

        DelayedMessage message = new DelayedMessage("id", "topic", "key", payload, Map.of(), 1L);
        payload.put("orderId", "changed");
        ObjectNode exposedPayload = (ObjectNode) message.payload();
        exposedPayload.put("orderId", "mutated");

        assertThat(message.payload().get("orderId").asText()).isEqualTo("A-1");
    }
}
