package io.github.timewheel.engine;

public interface DelayScheduler {

    SchedulingResult submit(DelayedMessage message);
}
