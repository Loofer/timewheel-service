package io.github.timewheel.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.timewheel.engine.DelayedMessage;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DlqMessageTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Instant FAILED_AT = Instant.parse("2026-06-24T02:00:00Z");

    @Test
    void validationFactorySetsValidationStageAndPreservesOriginalMessage() {
        DelayedMessage originalMessage = message();

        DlqMessage dlqMessage = DlqMessage.validation(
                "INVALID_PAYLOAD",
                "payload is missing orderId",
                FAILED_AT,
                "timewheel-api",
                originalMessage);

        assertThat(dlqMessage.errorCode()).isEqualTo("INVALID_PAYLOAD");
        assertThat(dlqMessage.errorMessage()).isEqualTo("payload is missing orderId");
        assertThat(dlqMessage.failedStage()).isEqualTo("VALIDATION");
        assertThat(dlqMessage.failedAt()).isEqualTo(FAILED_AT);
        assertThat(dlqMessage.source()).isEqualTo("timewheel-api");
        assertThat(dlqMessage.originalMessage()).isEqualTo(originalMessage);
    }

    @Test
    void schedulingFactorySetsSchedulingStage() {
        DlqMessage dlqMessage = DlqMessage.scheduling(
                "REDIS_WRITE_FAILED",
                "failed to store message",
                FAILED_AT,
                "timewheel-redis",
                message());

        assertThat(dlqMessage.failedStage()).isEqualTo("SCHEDULING");
    }

    @Test
    void publishingFactorySetsPublishingStage() {
        DlqMessage dlqMessage = DlqMessage.publishing(
                "KAFKA_SEND_FAILED",
                "failed to publish message",
                FAILED_AT,
                "timewheel-kafka",
                message());

        assertThat(dlqMessage.failedStage()).isEqualTo("PUBLISHING");
    }

    @Test
    void rejectsMissingStableMetadata() {
        DelayedMessage originalMessage = message();

        assertThatThrownBy(() -> new DlqMessage(
                null,
                "message",
                "PUBLISHING",
                FAILED_AT,
                "source",
                originalMessage))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("errorCode");
        assertThatThrownBy(() -> new DlqMessage(
                "CODE",
                "message",
                null,
                FAILED_AT,
                "source",
                originalMessage))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("failedStage");
        assertThatThrownBy(() -> new DlqMessage(
                "CODE",
                "message",
                "PUBLISHING",
                null,
                "source",
                originalMessage))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("failedAt");
        assertThatThrownBy(() -> new DlqMessage(
                "CODE",
                "message",
                "PUBLISHING",
                FAILED_AT,
                null,
                originalMessage))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("source");
    }

    private static DelayedMessage message() {
        return new DelayedMessage(
                "trace-1",
                "orders.ready",
                "order-key",
                OBJECT_MAPPER.createObjectNode().put("orderId", "A-1"),
                Map.of("tenant", "north"),
                500L);
    }
}
