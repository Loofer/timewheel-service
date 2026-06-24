package io.github.timewheel.server.worker;

import io.github.timewheel.redis.RedisTimewheelWorker;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.context.SmartLifecycle;

public class TimewheelWorkerLifecycle implements SmartLifecycle {

    private final RedisTimewheelWorker worker;
    private final Duration tickDuration;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private ScheduledExecutorService executor;
    private ScheduledFuture<?> scheduledTick;

    public TimewheelWorkerLifecycle(RedisTimewheelWorker worker, Duration tickDuration) {
        this.worker = Objects.requireNonNull(worker, "worker is required");
        this.tickDuration = Objects.requireNonNull(tickDuration, "tickDuration is required");
        if (tickDuration.isZero() || tickDuration.isNegative()) {
            throw new IllegalArgumentException("tickDuration must be positive");
        }
    }

    @Override
    public synchronized void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        long tickMillis = tickDuration.toMillis();
        executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "timewheel-worker");
            thread.setDaemon(true);
            return thread;
        });
        scheduledTick = executor.scheduleWithFixedDelay(this::tickSafely, tickMillis, tickMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        if (scheduledTick != null) {
            scheduledTick.cancel(true);
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    private void tickSafely() {
        try {
            worker.tickOnce();
        } catch (RuntimeException ignored) {
            // Keep the scheduling loop alive; health exposes persistent failures.
        }
    }
}
