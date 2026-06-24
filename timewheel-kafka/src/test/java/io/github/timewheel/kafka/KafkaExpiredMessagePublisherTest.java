package io.github.timewheel.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.timewheel.engine.DelayedMessage;
import io.github.timewheel.engine.DelayedMessageHeaders;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

class KafkaExpiredMessagePublisherTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void rejectsNullConstructorDependencies() {
        KafkaTemplate<String, String> kafkaTemplate = kafkaTemplate();

        assertThatThrownBy(() -> new KafkaExpiredMessagePublisher(null, OBJECT_MAPPER))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("kafkaTemplate");
        assertThatThrownBy(() -> new KafkaExpiredMessagePublisher(kafkaTemplate, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("objectMapper");
    }

    @Test
    void publishesPayloadOnlyToDynamicTargetTopicWithKeyAndHeaders() {
        KafkaTemplate<String, String> kafkaTemplate = kafkaTemplate();
        KafkaExpiredMessagePublisher publisher = new KafkaExpiredMessagePublisher(kafkaTemplate, OBJECT_MAPPER);
        DelayedMessage message = new DelayedMessage(
                "trace-1",
                "orders.ready",
                "order-key",
                OBJECT_MAPPER.createObjectNode().put("orderId", "A-1"),
                Map.of(
                        "tenant", "north",
                        "trace", "trace-1"),
                0L);

        publisher.publish(message);

        ArgumentCaptor<ProducerRecord<String, String>> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(recordCaptor.capture());
        ProducerRecord<String, String> record = recordCaptor.getValue();
        assertThat(record.topic()).isEqualTo("orders.ready");
        assertThat(record.key()).isEqualTo("order-key");
        assertThat(record.value()).isEqualTo("{\"orderId\":\"A-1\"}");
        assertThat(DelayedMessageHeaders.fromUtf8(record.headers().lastHeader("tenant").value())).isEqualTo("north");
        assertThat(DelayedMessageHeaders.fromUtf8(record.headers().lastHeader("trace").value())).isEqualTo("trace-1");
    }

    @Test
    void publishesNullKeyWhenMessageKeyIsBlank() {
        KafkaTemplate<String, String> kafkaTemplate = kafkaTemplate();
        KafkaExpiredMessagePublisher publisher = new KafkaExpiredMessagePublisher(kafkaTemplate, OBJECT_MAPPER);
        DelayedMessage message = new DelayedMessage(
                "trace-1",
                "orders.ready",
                " ",
                OBJECT_MAPPER.createObjectNode().put("orderId", "A-1"),
                Map.of(),
                0L);

        publisher.publish(message);

        ArgumentCaptor<ProducerRecord<String, String>> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(recordCaptor.capture());
        assertThat(recordCaptor.getValue().key()).isNull();
    }

    @Test
    void rejectsNullMessageClearly() {
        KafkaExpiredMessagePublisher publisher = new KafkaExpiredMessagePublisher(kafkaTemplate(), OBJECT_MAPPER);

        assertThatThrownBy(() -> publisher.publish(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("message");
    }

    @Test
    void wrapsPayloadSerializationFailureClearly() throws Exception {
        KafkaTemplate<String, String> kafkaTemplate = kafkaTemplate();
        ObjectMapper objectMapper = mock(ObjectMapper.class);
        DelayedMessage message = new DelayedMessage(
                "trace-1",
                "orders.ready",
                "order-key",
                OBJECT_MAPPER.createObjectNode().put("orderId", "A-1"),
                Map.of(),
                0L);
        JsonProcessingException failure = new JsonProcessingException("boom") {
        };
        when(objectMapper.writeValueAsString(message.payload())).thenThrow(failure);
        KafkaExpiredMessagePublisher publisher = new KafkaExpiredMessagePublisher(kafkaTemplate, objectMapper);

        assertThatThrownBy(() -> publisher.publish(message))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to serialize delayed message payload")
                .hasCause(failure);
    }

    @SuppressWarnings("unchecked")
    private static KafkaTemplate<String, String> kafkaTemplate() {
        return mock(KafkaTemplate.class);
    }
}
