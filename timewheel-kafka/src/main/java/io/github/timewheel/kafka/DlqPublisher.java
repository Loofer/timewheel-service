package io.github.timewheel.kafka;

public interface DlqPublisher {

    void publish(DlqMessage message);
}
