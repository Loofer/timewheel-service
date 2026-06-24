package io.github.timewheel.server.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import io.github.timewheel.redis.RedisTimewheelWorker;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class TimewheelWorkerLifecycleTest {

    @Test
    void rejectsInvalidConstructorArguments() {
        RedisTimewheelWorker worker = mock(RedisTimewheelWorker.class);

        assertThatThrownBy(() -> new TimewheelWorkerLifecycle(null, Duration.ofSeconds(1)))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("worker");
        assertThatThrownBy(() -> new TimewheelWorkerLifecycle(worker, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("tickDuration");
        assertThatThrownBy(() -> new TimewheelWorkerLifecycle(worker, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tickDuration");
    }

    @Test
    void startsWorkerOnScheduleAndStopsIt() {
        RedisTimewheelWorker worker = mock(RedisTimewheelWorker.class);
        TimewheelWorkerLifecycle lifecycle = new TimewheelWorkerLifecycle(worker, Duration.ofMillis(10));

        lifecycle.start();

        assertThat(lifecycle.isRunning()).isTrue();
        verify(worker, timeout(200).atLeastOnce()).tickOnce();

        lifecycle.stop();

        assertThat(lifecycle.isRunning()).isFalse();
        verify(worker, atLeastOnce()).tickOnce();
    }
}
