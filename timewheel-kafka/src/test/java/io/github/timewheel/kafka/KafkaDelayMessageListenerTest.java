package io.github.timewheel.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.timewheel.engine.DelayScheduler;
import io.github.timewheel.engine.DelayedMessage;
import io.github.timewheel.engine.SchedulingException;
import io.github.timewheel.engine.SchedulingResult;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaDelayMessageListenerTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-06-24T03:00:00Z"), ZoneOffset.UTC);

    @Test
    void schedulesValidKafkaInputMessage() {
        DelayScheduler scheduler = mock(DelayScheduler.class);
        DlqPublisher dlqPublisher = mock(DlqPublisher.class);
        DelayedMessage message = message();
        when(scheduler.submit(message)).thenReturn(SchedulingResult.SCHEDULED);
        KafkaDelayMessageListener listener = new KafkaDelayMessageListener(scheduler, dlqPublisher, CLOCK);

        listener.onDelayMessage(message);

        verify(scheduler).submit(message);
        verify(dlqPublisher, never()).publish(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void sendsSchedulingExceptionToDlq() {
        DelayScheduler scheduler = mock(DelayScheduler.class);
        DlqPublisher dlqPublisher = mock(DlqPublisher.class);
        DelayedMessage message = message();
        when(scheduler.submit(message)).thenThrow(new SchedulingException(
                "DELAY_OUT_OF_RANGE",
                "delayMillis exceeds configured max wheel range"));
        KafkaDelayMessageListener listener = new KafkaDelayMessageListener(scheduler, dlqPublisher, CLOCK);

        listener.onDelayMessage(message);

        ArgumentCaptor<DlqMessage> dlqMessage = ArgumentCaptor.forClass(DlqMessage.class);
        verify(dlqPublisher).publish(dlqMessage.capture());
        assertThat(dlqMessage.getValue().errorCode()).isEqualTo("DELAY_OUT_OF_RANGE");
        assertThat(dlqMessage.getValue().errorMessage()).isEqualTo("delayMillis exceeds configured max wheel range");
        assertThat(dlqMessage.getValue().failedStage()).isEqualTo("SCHEDULING");
        assertThat(dlqMessage.getValue().failedAt()).isEqualTo(Instant.now(CLOCK));
        assertThat(dlqMessage.getValue().source()).isEqualTo("KAFKA_INPUT");
        assertThat(dlqMessage.getValue().originalMessage()).isEqualTo(message);
    }

    @Test
    void sendsNullInputToValidationDlq() {
        DelayScheduler scheduler = mock(DelayScheduler.class);
        DlqPublisher dlqPublisher = mock(DlqPublisher.class);
        KafkaDelayMessageListener listener = new KafkaDelayMessageListener(scheduler, dlqPublisher, CLOCK);

        listener.onDelayMessage(null);

        verify(scheduler, never()).submit(org.mockito.ArgumentMatchers.any());
        ArgumentCaptor<DlqMessage> dlqMessage = ArgumentCaptor.forClass(DlqMessage.class);
        verify(dlqPublisher).publish(dlqMessage.capture());
        assertThat(dlqMessage.getValue().errorCode()).isEqualTo("NULL_MESSAGE");
        assertThat(dlqMessage.getValue().failedStage()).isEqualTo("VALIDATION");
        assertThat(dlqMessage.getValue().failedAt()).isEqualTo(Instant.now(CLOCK));
        assertThat(dlqMessage.getValue().source()).isEqualTo("KAFKA_INPUT");
        assertThat(dlqMessage.getValue().originalMessage()).isNull();
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
