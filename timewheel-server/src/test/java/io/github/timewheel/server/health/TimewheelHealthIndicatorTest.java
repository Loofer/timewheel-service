package io.github.timewheel.server.health;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.timewheel.redis.RedisWheelKeys;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.kafka.core.KafkaTemplate;

class TimewheelHealthIndicatorTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-06-24T05:00:00Z"), ZoneOffset.UTC);

    private RedissonClient redissonClient;
    private RedisWheelKeys keys;
    private RBucket<Object> tickTime;
    private KafkaTemplate<String, String> kafkaTemplate;

    @BeforeEach
    void setUp() {
        redissonClient = mock(RedissonClient.class);
        keys = new RedisWheelKeys("orders");
        tickTime = bucket(keys.tickTime());
        kafkaTemplate = kafkaTemplate();
        when(kafkaTemplate.metrics()).thenReturn(Map.of());
    }

    @Test
    void rejectsNullConstructorDependencies() {
        assertThatThrownBy(() -> new TimewheelHealthIndicator(null, keys, kafkaTemplate, CLOCK, Duration.ofSeconds(60)))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("redissonClient");
        assertThatThrownBy(() -> new TimewheelHealthIndicator(redissonClient, null, kafkaTemplate, CLOCK, Duration.ofSeconds(60)))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("keys");
        assertThatThrownBy(() -> new TimewheelHealthIndicator(redissonClient, keys, null, CLOCK, Duration.ofSeconds(60)))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("kafkaTemplate");
        assertThatThrownBy(() -> new TimewheelHealthIndicator(redissonClient, keys, kafkaTemplate, null, Duration.ofSeconds(60)))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("clock");
        assertThatThrownBy(() -> new TimewheelHealthIndicator(redissonClient, keys, kafkaTemplate, CLOCK, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxTickIdle");
    }

    @Test
    void reportsUpWhenTickProgressIsFreshAndKafkaMetricsAreReadable() {
        when(tickTime.get()).thenReturn(CLOCK.millis() - 10_000);
        TimewheelHealthIndicator indicator = new TimewheelHealthIndicator(
                redissonClient, keys, kafkaTemplate, CLOCK, Duration.ofSeconds(60));

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails()).containsEntry("tickIdleMillis", 10_000L);
        assertThat(health.getDetails()).containsEntry("kafkaProducer", "up");
    }

    @Test
    void reportsDownWhenTickProgressIsStale() {
        when(tickTime.get()).thenReturn(CLOCK.millis() - 61_000);
        TimewheelHealthIndicator indicator = new TimewheelHealthIndicator(
                redissonClient, keys, kafkaTemplate, CLOCK, Duration.ofSeconds(60));

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails()).containsEntry("reason", "tick progress stale");
        assertThat(health.getDetails()).containsEntry("tickIdleMillis", 61_000L);
    }

    @Test
    void reportsDownWhenRedisReadFails() {
        when(tickTime.get()).thenThrow(new IllegalStateException("redis unavailable"));
        TimewheelHealthIndicator indicator = new TimewheelHealthIndicator(
                redissonClient, keys, kafkaTemplate, CLOCK, Duration.ofSeconds(60));

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails()).containsEntry("reason", "redis unavailable");
    }

    @Test
    void reportsDownWhenKafkaMetricsFail() {
        when(tickTime.get()).thenReturn(CLOCK.millis() - 10_000);
        when(kafkaTemplate.metrics()).thenThrow(new IllegalStateException("kafka unavailable"));
        TimewheelHealthIndicator indicator = new TimewheelHealthIndicator(
                redissonClient, keys, kafkaTemplate, CLOCK, Duration.ofSeconds(60));

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails()).containsEntry("reason", "kafka unavailable");
    }

    @SuppressWarnings("unchecked")
    private RBucket<Object> bucket(String key) {
        RBucket<Object> bucket = mock(RBucket.class);
        when(redissonClient.getBucket(key)).thenReturn(bucket);
        return bucket;
    }

    @SuppressWarnings("unchecked")
    private static KafkaTemplate<String, String> kafkaTemplate() {
        return mock(KafkaTemplate.class);
    }
}
