package io.github.timewheel.redis;

import io.github.timewheel.engine.TimewheelSettings;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class RedisDelaySchedulerPlacementTest {

    @Test
    void mapsDelayToExpectedCycleAndTick() {
        TimewheelSettings settings = new TimewheelSettings(Duration.ofSeconds(10), 100, 2);

        RedisDelayScheduler.Placement placement = RedisDelayScheduler.place(60_000, 0, 0, settings);

        assertThat(placement.cycle()).isEqualTo(0);
        assertThat(placement.tick()).isEqualTo(6);
    }

    @Test
    void wrapsTickIntoNextCycle() {
        TimewheelSettings settings = new TimewheelSettings(Duration.ofSeconds(10), 100, 2);

        RedisDelayScheduler.Placement placement = RedisDelayScheduler.place(20_000, 0, 99, settings);

        assertThat(placement.cycle()).isEqualTo(1);
        assertThat(placement.tick()).isEqualTo(1);
    }

    @Test
    void roundsPartialTicksUp() {
        TimewheelSettings settings = new TimewheelSettings(Duration.ofSeconds(10), 100, 2);

        RedisDelayScheduler.Placement placement = RedisDelayScheduler.place(10_001, 0, 0, settings);

        assertThat(placement.cycle()).isEqualTo(0);
        assertThat(placement.tick()).isEqualTo(2);
    }
}
