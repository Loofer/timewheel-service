package io.github.timewheel.kafka;

import io.github.timewheel.engine.DelayScheduler;
import io.github.timewheel.engine.DelayedMessage;
import io.github.timewheel.engine.SchedulingException;
import org.springframework.kafka.annotation.KafkaListener;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;

public class KafkaDelayMessageListener {

    static final String SOURCE = "KAFKA_INPUT";

    private final DelayScheduler scheduler;
    private final DlqPublisher dlqPublisher;
    private final Clock clock;

    public KafkaDelayMessageListener(DelayScheduler scheduler, DlqPublisher dlqPublisher, Clock clock) {
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.dlqPublisher = Objects.requireNonNull(dlqPublisher, "dlqPublisher");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @KafkaListener(
            topics = "${timewheel.kafka.input-topic}",
            groupId = "${timewheel.kafka.consumer-group:timewheel-service}")
    public void onDelayMessage(DelayedMessage message) {
        if (message == null) {
            dlqPublisher.publish(DlqMessage.validation(
                    "NULL_MESSAGE",
                    "Kafka input message must not be null",
                    Instant.now(clock),
                    SOURCE,
                    null));
            return;
        }
        try {
            scheduler.submit(message);
        } catch (SchedulingException ex) {
            dlqPublisher.publish(DlqMessage.scheduling(
                    ex.errorCode(),
                    ex.getMessage(),
                    Instant.now(clock),
                    SOURCE,
                    message));
        }
    }
}
