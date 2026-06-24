package io.github.timewheel.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.timewheel.engine.DelayedMessage;
import io.github.timewheel.engine.ExpiredMessagePublisher;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class RetryingExpiredMessagePublisherTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-06-24T04:00:00Z"), ZoneOffset.UTC);

    @Test
    void rejectsInvalidConstructorArguments() {
        ExpiredMessagePublisher delegate = mock(ExpiredMessagePublisher.class);
        DlqPublisher dlqPublisher = mock(DlqPublisher.class);

        assertThatThrownBy(() -> new RetryingExpiredMessagePublisher(null, dlqPublisher, CLOCK, 3, "EXPIRATION_OUTPUT"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("delegate");
        assertThatThrownBy(() -> new RetryingExpiredMessagePublisher(delegate, null, CLOCK, 3, "EXPIRATION_OUTPUT"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("dlqPublisher");
        assertThatThrownBy(() -> new RetryingExpiredMessagePublisher(delegate, dlqPublisher, null, 3, "EXPIRATION_OUTPUT"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("clock");
        assertThatThrownBy(() -> new RetryingExpiredMessagePublisher(delegate, dlqPublisher, CLOCK, 0, "EXPIRATION_OUTPUT"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxAttempts");
        assertThatThrownBy(() -> new RetryingExpiredMessagePublisher(delegate, dlqPublisher, CLOCK, 3, " "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("source");
    }

    @Test
    void retriesUntilPublishSucceeds() {
        ExpiredMessagePublisher delegate = mock(ExpiredMessagePublisher.class);
        DlqPublisher dlqPublisher = mock(DlqPublisher.class);
        DelayedMessage message = message();
        doThrow(new IllegalStateException("first failure"))
                .doThrow(new IllegalStateException("second failure"))
                .doNothing()
                .when(delegate).publish(message);
        RetryingExpiredMessagePublisher publisher = new RetryingExpiredMessagePublisher(
                delegate, dlqPublisher, CLOCK, 3, "EXPIRATION_OUTPUT");

        publisher.publish(message);

        verify(delegate, times(3)).publish(message);
        verify(dlqPublisher, never()).publish(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void sendsPublishingDlqAfterRetryExhaustion() {
        ExpiredMessagePublisher delegate = mock(ExpiredMessagePublisher.class);
        DlqPublisher dlqPublisher = mock(DlqPublisher.class);
        DelayedMessage message = message();
        doThrow(new IllegalStateException("kafka unavailable"))
                .when(delegate).publish(message);
        RetryingExpiredMessagePublisher publisher = new RetryingExpiredMessagePublisher(
                delegate, dlqPublisher, CLOCK, 2, "EXPIRATION_OUTPUT");

        publisher.publish(message);

        verify(delegate, times(2)).publish(message);
        ArgumentCaptor<DlqMessage> dlqMessage = ArgumentCaptor.forClass(DlqMessage.class);
        verify(dlqPublisher).publish(dlqMessage.capture());
        assertThat(dlqMessage.getValue().errorCode()).isEqualTo("PUBLISH_FAILED");
        assertThat(dlqMessage.getValue().errorMessage()).isEqualTo("kafka unavailable");
        assertThat(dlqMessage.getValue().failedStage()).isEqualTo("PUBLISHING");
        assertThat(dlqMessage.getValue().failedAt()).isEqualTo(Instant.now(CLOCK));
        assertThat(dlqMessage.getValue().source()).isEqualTo("EXPIRATION_OUTPUT");
        assertThat(dlqMessage.getValue().originalMessage()).isEqualTo(message);
    }

    @Test
    void rejectsNullMessagesClearly() {
        RetryingExpiredMessagePublisher publisher = new RetryingExpiredMessagePublisher(
                mock(ExpiredMessagePublisher.class),
                mock(DlqPublisher.class),
                CLOCK,
                3,
                "EXPIRATION_OUTPUT");

        assertThatThrownBy(() -> publisher.publish(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("message");
    }

    private static DelayedMessage message() {
        return new DelayedMessage(
                "trace-1",
                "orders.ready",
                "order-key",
                OBJECT_MAPPER.createObjectNode().put("orderId", "A-1"),
                Map.of("tenant", "north"),
                0L);
    }
}
