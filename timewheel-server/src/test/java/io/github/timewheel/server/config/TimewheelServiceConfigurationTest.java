package io.github.timewheel.server.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.timewheel.engine.DelayScheduler;
import io.github.timewheel.engine.ExpiredMessagePublisher;
import io.github.timewheel.engine.TimewheelSettings;
import io.github.timewheel.kafka.DlqPublisher;
import io.github.timewheel.kafka.KafkaDelayMessageListener;
import io.github.timewheel.kafka.RetryingExpiredMessagePublisher;
import io.github.timewheel.redis.RedisDelayScheduler;
import io.github.timewheel.redis.RedisTimewheelWorker;
import io.github.timewheel.redis.RedisWheelKeys;
import io.github.timewheel.server.health.TimewheelHealthIndicator;
import io.github.timewheel.server.worker.TimewheelWorkerLifecycle;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class TimewheelServiceConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(TimewheelServiceConfiguration.class)
            .withBean(RedissonClient.class, () -> mock(RedissonClient.class))
            .withBean(KafkaTemplate.class, () -> mock(KafkaTemplate.class))
            .withBean(ObjectMapper.class, ObjectMapper::new);

    @Test
    void createsServiceBeansFromTimewheelProperties() {
        contextRunner
                .withPropertyValues(
                        "timewheel.tick-duration=5s",
                        "timewheel.ticks-per-wheel=12",
                        "timewheel.max-cycle=3",
                        "timewheel.redis.key-prefix=orders-delay",
                        "timewheel.kafka.publish-max-attempts=4",
                        "timewheel.health.max-tick-idle=30s",
                        "timewheel.kafka.dead-letter-topic=orders-delay.dlq")
                .run(context -> {
                    assertThat(context).hasSingleBean(TimewheelSettings.class);
                    assertThat(context).hasSingleBean(RedisWheelKeys.class);
                    assertThat(context).hasSingleBean(ExpiredMessagePublisher.class);
                    assertThat(context).hasSingleBean(DlqPublisher.class);
                    assertThat(context).hasSingleBean(KafkaDelayMessageListener.class);
                    assertThat(context).hasSingleBean(DelayScheduler.class);
                    assertThat(context).hasSingleBean(RedisTimewheelWorker.class);
                    assertThat(context).hasSingleBean(TimewheelWorkerLifecycle.class);
                    assertThat(context).hasSingleBean(HealthIndicator.class);

                    assertThat(context.getBean(TimewheelSettings.class))
                            .isEqualTo(new TimewheelSettings(Duration.ofSeconds(5), 12, 3));
                    assertThat(context.getBean(RedisWheelKeys.class).currentTick())
                            .isEqualTo("orders-delay:timewheel:current-tick");
                    assertThat(context.getBean(ExpiredMessagePublisher.class))
                            .isInstanceOf(RetryingExpiredMessagePublisher.class);
                    assertThat(context.getBean(DelayScheduler.class))
                            .isInstanceOf(RedisDelayScheduler.class);
                    assertThat(context.getBean(HealthIndicator.class))
                            .isInstanceOf(TimewheelHealthIndicator.class);
                });
    }
}
