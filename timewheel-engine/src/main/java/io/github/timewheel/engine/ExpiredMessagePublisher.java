package io.github.timewheel.engine;

public interface ExpiredMessagePublisher {

    void publish(DelayedMessage message);
}
