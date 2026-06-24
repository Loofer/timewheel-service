package io.github.timewheel.server.health;

import io.github.timewheel.redis.RedisWheelKeys;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.core.KafkaTemplate;

public class TimewheelHealthIndicator implements HealthIndicator {

    private final RedissonClient redissonClient;
    private final RedisWheelKeys keys;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final Clock clock;
    private final Duration maxTickIdle;

    public TimewheelHealthIndicator(
            RedissonClient redissonClient,
            RedisWheelKeys keys,
            KafkaTemplate<String, String> kafkaTemplate,
            Clock clock,
            Duration maxTickIdle) {
        this.redissonClient = Objects.requireNonNull(redissonClient, "redissonClient is required");
        this.keys = Objects.requireNonNull(keys, "keys is required");
        this.kafkaTemplate = Objects.requireNonNull(kafkaTemplate, "kafkaTemplate is required");
        this.clock = Objects.requireNonNull(clock, "clock is required");
        this.maxTickIdle = Objects.requireNonNull(maxTickIdle, "maxTickIdle is required");
        if (maxTickIdle.isZero() || maxTickIdle.isNegative()) {
            throw new IllegalArgumentException("maxTickIdle must be positive");
        }
    }

    @Override
    public Health health() {
        try {
            Long tickTime = tickTime();
            if (tickTime == null) {
                return Health.down()
                        .withDetail("reason", "tick progress unavailable")
                        .build();
            }
            long tickIdleMillis = clock.millis() - tickTime;
            if (tickIdleMillis > maxTickIdle.toMillis()) {
                return Health.down()
                        .withDetail("reason", "tick progress stale")
                        .withDetail("tickIdleMillis", tickIdleMillis)
                        .withDetail("maxTickIdleMillis", maxTickIdle.toMillis())
                        .build();
            }
            kafkaTemplate.metrics();
            return Health.up()
                    .withDetail("tickIdleMillis", tickIdleMillis)
                    .withDetail("maxTickIdleMillis", maxTickIdle.toMillis())
                    .withDetail("kafkaProducer", "up")
                    .build();
        } catch (RuntimeException failure) {
            return Health.down()
                    .withDetail("reason", reason(failure))
                    .withException(failure)
                    .build();
        }
    }

    private Long tickTime() {
        RBucket<Object> bucket = redissonClient.getBucket(keys.tickTime());
        Object value = bucket.get();
        return value == null ? null : Long.parseLong(value.toString());
    }

    private static String reason(RuntimeException failure) {
        return failure.getMessage() == null ? failure.getClass().getName() : failure.getMessage();
    }
}
