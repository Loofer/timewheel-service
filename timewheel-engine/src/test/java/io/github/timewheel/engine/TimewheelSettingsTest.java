package io.github.timewheel.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class TimewheelSettingsTest {

    @Test
    void calculatesMaxDelayMillisFromWheelRange() {
        TimewheelSettings settings = new TimewheelSettings(Duration.ofMillis(100), 60, 5);

        assertThat(settings.maxDelayMillis()).isEqualTo(30_000L);
    }

    @Test
    void exposesWheelSettingsForPlacementAndConfiguration() {
        TimewheelSettings settings = new TimewheelSettings(Duration.ofMillis(100), 60, 5);

        assertThat(settings.tickDuration()).isEqualTo(Duration.ofMillis(100));
        assertThat(settings.ticksPerWheel()).isEqualTo(60);
        assertThat(settings.maxCycle()).isEqualTo(5);
    }

    @Test
    void rejectsInvalidTickDuration() {
        assertThatThrownBy(() -> new TimewheelSettings(null, 60, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tickDuration");
        assertThatThrownBy(() -> new TimewheelSettings(Duration.ZERO, 60, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tickDuration");
        assertThatThrownBy(() -> new TimewheelSettings(Duration.ofMillis(-1), 60, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tickDuration");
        assertThatThrownBy(() -> new TimewheelSettings(Duration.ofNanos(1), 60, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tickDuration");
    }

    @Test
    void rejectsInvalidTicksPerWheel() {
        assertThatThrownBy(() -> new TimewheelSettings(Duration.ofMillis(100), 0, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ticksPerWheel");
        assertThatThrownBy(() -> new TimewheelSettings(Duration.ofMillis(100), -1, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ticksPerWheel");
    }

    @Test
    void rejectsInvalidMaxCycle() {
        assertThatThrownBy(() -> new TimewheelSettings(Duration.ofMillis(100), 60, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxCycle");
        assertThatThrownBy(() -> new TimewheelSettings(Duration.ofMillis(100), 60, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxCycle");
    }

    @Test
    void validatesInRangeDelayIncludingImmediateValues() {
        TimewheelSettings settings = new TimewheelSettings(Duration.ofMillis(100), 60, 5);

        assertThatCode(() -> settings.validateDelay(-1L)).doesNotThrowAnyException();
        assertThatCode(() -> settings.validateDelay(0L)).doesNotThrowAnyException();
        assertThatCode(() -> settings.validateDelay(1L)).doesNotThrowAnyException();
        assertThatCode(() -> settings.validateDelay(30_000L)).doesNotThrowAnyException();
    }

    @Test
    void rejectsOverRangeDelayWithStableErrorCode() {
        TimewheelSettings settings = new TimewheelSettings(Duration.ofMillis(100), 60, 5);

        assertThatThrownBy(() -> settings.validateDelay(30_001L))
                .isInstanceOfSatisfying(SchedulingException.class, exception ->
                        assertThat(exception.errorCode()).isEqualTo("DELAY_OUT_OF_RANGE"))
                .hasMessageContaining("max wheel range");
    }

    @Test
    void rejectsSettingsWhenMaxDelayMillisWouldOverflow() {
        assertThatThrownBy(() -> new TimewheelSettings(Duration.ofMillis(Long.MAX_VALUE), 2, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxDelayMillis");
    }
}
