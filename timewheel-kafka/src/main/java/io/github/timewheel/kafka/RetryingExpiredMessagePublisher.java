package io.github.timewheel.kafka;

import io.github.timewheel.engine.DelayedMessage;
import io.github.timewheel.engine.ExpiredMessagePublisher;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;

public class RetryingExpiredMessagePublisher implements ExpiredMessagePublisher {

    public static final String PUBLISH_FAILED = "PUBLISH_FAILED";

    private final ExpiredMessagePublisher delegate;
    private final DlqPublisher dlqPublisher;
    private final Clock clock;
    private final int maxAttempts;
    private final String source;

    public RetryingExpiredMessagePublisher(
            ExpiredMessagePublisher delegate,
            DlqPublisher dlqPublisher,
            Clock clock,
            int maxAttempts,
            String source) {
        this.delegate = Objects.requireNonNull(delegate, "delegate is required");
        this.dlqPublisher = Objects.requireNonNull(dlqPublisher, "dlqPublisher is required");
        this.clock = Objects.requireNonNull(clock, "clock is required");
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1");
        }
        if (source == null || source.isBlank()) {
            throw new IllegalArgumentException("source is required");
        }
        this.maxAttempts = maxAttempts;
        this.source = source;
    }

    @Override
    public void publish(DelayedMessage message) {
        Objects.requireNonNull(message, "message is required");
        RuntimeException lastFailure = null;
        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            try {
                delegate.publish(message);
                return;
            } catch (RuntimeException failure) {
                lastFailure = failure;
            }
        }
        dlqPublisher.publish(DlqMessage.publishing(
                PUBLISH_FAILED,
                errorMessage(lastFailure),
                Instant.now(clock),
                source,
                message));
    }

    private static String errorMessage(RuntimeException failure) {
        if (failure == null) {
            return "publish failed";
        }
        return failure.getMessage() == null ? failure.getClass().getName() : failure.getMessage();
    }
}
