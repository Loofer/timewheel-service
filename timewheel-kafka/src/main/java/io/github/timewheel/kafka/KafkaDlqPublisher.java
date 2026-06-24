package io.github.timewheel.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Objects;

public class KafkaDlqPublisher implements DlqPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String deadLetterTopic;

    public KafkaDlqPublisher(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            String deadLetterTopic) {
        this.kafkaTemplate = Objects.requireNonNull(kafkaTemplate, "kafkaTemplate");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper").copy().findAndRegisterModules();
        if (deadLetterTopic == null || deadLetterTopic.isBlank()) {
            throw new IllegalArgumentException("deadLetterTopic must not be blank");
        }
        this.deadLetterTopic = deadLetterTopic;
    }

    @Override
    public void publish(DlqMessage message) {
        Objects.requireNonNull(message, "message");
        try {
            kafkaTemplate.send(new ProducerRecord<>(
                    deadLetterTopic,
                    message.errorCode(),
                    objectMapper.writeValueAsString(message)));
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to serialize DLQ message", ex);
        }
    }
}
