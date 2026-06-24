package io.github.timewheel.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class RedisTimewheelPropertiesTest {

    @Test
    void exposesOriginalBootstrapDefaults() {
        RedisTimewheelProperties properties = new RedisTimewheelProperties();

        assertThat(properties.getKeyPrefix()).isEqualTo("timewheel-service");
        assertThat(properties.getTickDuration()).isEqualTo(Duration.ofSeconds(10));
        assertThat(properties.getTicksPerWheel()).isEqualTo(100);
        assertThat(properties.getMaxCycle()).isEqualTo(2);
    }

    @Test
    void allowsConfigurationValuesToBeSet() {
        RedisTimewheelProperties properties = new RedisTimewheelProperties();

        properties.setKeyPrefix("orders");
        properties.setTickDuration(Duration.ofMillis(500));
        properties.setTicksPerWheel(60);
        properties.setMaxCycle(4);

        assertThat(properties.getKeyPrefix()).isEqualTo("orders");
        assertThat(properties.getTickDuration()).isEqualTo(Duration.ofMillis(500));
        assertThat(properties.getTicksPerWheel()).isEqualTo(60);
        assertThat(properties.getMaxCycle()).isEqualTo(4);
    }
}
