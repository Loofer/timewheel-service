package io.github.timewheel.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class SchedulingExceptionTest {

    @Test
    void exposesStableErrorCode() {
        SchedulingException exception = new SchedulingException("DELAY_OUT_OF_RANGE", "too far");

        assertThat(exception.errorCode()).isEqualTo("DELAY_OUT_OF_RANGE");
        assertThat(exception).hasMessage("too far");
    }

    @Test
    void rejectsBlankErrorCode() {
        assertThatThrownBy(() -> new SchedulingException(" ", "bad"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("errorCode");
    }
}
