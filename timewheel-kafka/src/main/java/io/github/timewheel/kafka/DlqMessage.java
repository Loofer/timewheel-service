package io.github.timewheel.kafka;

import io.github.timewheel.engine.DelayedMessage;

import java.time.Instant;
import java.util.Objects;

public record DlqMessage(
        String errorCode,
        String errorMessage,
        String failedStage,
        Instant failedAt,
        String source,
        DelayedMessage originalMessage) {

    public DlqMessage {
        Objects.requireNonNull(errorCode, "errorCode");
        Objects.requireNonNull(failedStage, "failedStage");
        Objects.requireNonNull(failedAt, "failedAt");
        Objects.requireNonNull(source, "source");
    }

    public static DlqMessage validation(
            String errorCode,
            String errorMessage,
            Instant failedAt,
            String source,
            DelayedMessage originalMessage) {
        return new DlqMessage(errorCode, errorMessage, "VALIDATION", failedAt, source, originalMessage);
    }

    public static DlqMessage scheduling(
            String errorCode,
            String errorMessage,
            Instant failedAt,
            String source,
            DelayedMessage originalMessage) {
        return new DlqMessage(errorCode, errorMessage, "SCHEDULING", failedAt, source, originalMessage);
    }

    public static DlqMessage publishing(
            String errorCode,
            String errorMessage,
            Instant failedAt,
            String source,
            DelayedMessage originalMessage) {
        return new DlqMessage(errorCode, errorMessage, "PUBLISHING", failedAt, source, originalMessage);
    }
}
