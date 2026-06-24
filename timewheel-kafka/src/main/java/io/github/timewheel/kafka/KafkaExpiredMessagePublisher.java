package io.github.timewheel.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.timewheel.engine.DelayedMessage;
import io.github.timewheel.engine.DelayedMessageHeaders;
import io.github.timewheel.engine.ExpiredMessagePublisher;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Objects;

public class KafkaExpiredMessagePublisher implements ExpiredMessagePublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public KafkaExpiredMessagePublisher(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = Objects.requireNonNull(kafkaTemplate, "kafkaTemplate");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
    }

    @Override
    public void publish(DelayedMessage message) {
        Objects.requireNonNull(message, "message");
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    message.targetTopic(),
                    message.key().orElse(null),
                    objectMapper.writeValueAsString(message.payload()));
            message.headers().forEach((name, value) -> record.headers().add(name, DelayedMessageHeaders.toUtf8(value)));
            kafkaTemplate.send(record);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to serialize delayed message payload", ex);
        }
    }
}
