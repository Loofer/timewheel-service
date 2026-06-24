package io.github.timewheel.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.timewheel.engine.DelayedMessage;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class KafkaDlqPublisherTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void rejectsNullConstructorDependencies() {
        KafkaTemplate<String, String> kafkaTemplate = kafkaTemplate();

        assertThatThrownBy(() -> new KafkaDlqPublisher(null, OBJECT_MAPPER, "timewheel.dlq"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("kafkaTemplate");
        assertThatThrownBy(() -> new KafkaDlqPublisher(kafkaTemplate, null, "timewheel.dlq"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("objectMapper");
        assertThatThrownBy(() -> new KafkaDlqPublisher(kafkaTemplate, OBJECT_MAPPER, " "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("deadLetterTopic");
    }

    @Test
    void publishesDlqMessageAsJsonToDeadLetterTopic() throws Exception {
        KafkaTemplate<String, String> kafkaTemplate = kafkaTemplate();
        KafkaDlqPublisher publisher = new KafkaDlqPublisher(kafkaTemplate, OBJECT_MAPPER, "timewheel.delay.dlq");
        DlqMessage message = DlqMessage.scheduling(
                "DELAY_OUT_OF_RANGE",
                "delayMillis exceeds range",
                Instant.parse("2026-06-24T03:00:00Z"),
                "KAFKA_INPUT",
                message());

        publisher.publish(message);

        ArgumentCaptor<ProducerRecord<String, String>> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(recordCaptor.capture());
        ProducerRecord<String, String> record = recordCaptor.getValue();
        assertThat(record.topic()).isEqualTo("timewheel.delay.dlq");
        assertThat(record.key()).isEqualTo("DELAY_OUT_OF_RANGE");
        JsonNode body = OBJECT_MAPPER.readTree(record.value());
        assertThat(body.get("errorCode").asText()).isEqualTo("DELAY_OUT_OF_RANGE");
        assertThat(body.get("failedStage").asText()).isEqualTo("SCHEDULING");
        assertThat(body.get("source").asText()).isEqualTo("KAFKA_INPUT");
        assertThat(body.at("/originalMessage/targetTopic").asText()).isEqualTo("orders.ready");
    }

    @Test
    void rejectsNullDlqMessageClearly() {
        KafkaDlqPublisher publisher = new KafkaDlqPublisher(kafkaTemplate(), OBJECT_MAPPER, "timewheel.delay.dlq");

        assertThatThrownBy(() -> publisher.publish(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("message");
    }

    @SuppressWarnings("unchecked")
    private static KafkaTemplate<String, String> kafkaTemplate() {
        return mock(KafkaTemplate.class);
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
