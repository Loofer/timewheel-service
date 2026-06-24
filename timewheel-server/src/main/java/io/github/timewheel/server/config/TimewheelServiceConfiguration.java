package io.github.timewheel.server.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.timewheel.engine.DelayScheduler;
import io.github.timewheel.engine.ExpiredMessagePublisher;
import io.github.timewheel.engine.TimewheelSettings;
import io.github.timewheel.kafka.DlqPublisher;
import io.github.timewheel.kafka.KafkaDelayMessageListener;
import io.github.timewheel.kafka.KafkaDlqPublisher;
import io.github.timewheel.kafka.KafkaExpiredMessagePublisher;
import io.github.timewheel.kafka.RetryingExpiredMessagePublisher;
import io.github.timewheel.redis.RedisDelayScheduler;
import io.github.timewheel.redis.RedisTimewheelWorker;
import io.github.timewheel.redis.RedisWheelKeys;
import io.github.timewheel.server.health.TimewheelHealthIndicator;
import io.github.timewheel.server.worker.TimewheelWorkerLifecycle;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.redisson.api.RedissonClient;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Clock;
import java.time.Duration;

@Configuration
@EnableConfigurationProperties
public class TimewheelServiceConfiguration {

    @Bean
    @ConfigurationProperties(prefix = "timewheel")
    TimewheelProperties timewheelProperties() {
        return new TimewheelProperties();
    }

    @Bean
    TimewheelSettings timewheelSettings(TimewheelProperties properties) {
        return new TimewheelSettings(
                properties.getTickDuration(),
                properties.getTicksPerWheel(),
                properties.getMaxCycle());
    }

    @Bean
    RedisWheelKeys redisWheelKeys(TimewheelProperties properties) {
        return new RedisWheelKeys(properties.getRedis().getKeyPrefix());
    }

    @Bean
    ExpiredMessagePublisher expiredMessagePublisher(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            DlqPublisher dlqPublisher,
            TimewheelProperties properties,
            Clock clock) {
        ExpiredMessagePublisher kafkaPublisher = new KafkaExpiredMessagePublisher(kafkaTemplate, objectMapper);
        return new RetryingExpiredMessagePublisher(
                kafkaPublisher,
                dlqPublisher,
                clock,
                properties.getKafka().getPublishMaxAttempts(),
                "EXPIRATION_OUTPUT");
    }

    @Bean
    DlqPublisher dlqPublisher(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            TimewheelProperties properties) {
        return new KafkaDlqPublisher(kafkaTemplate, objectMapper, properties.getKafka().getDeadLetterTopic());
    }

    @Bean
    KafkaDelayMessageListener kafkaDelayMessageListener(
            DelayScheduler scheduler,
            DlqPublisher dlqPublisher,
            Clock clock) {
        return new KafkaDelayMessageListener(scheduler, dlqPublisher, clock);
    }

    @Bean
    DelayScheduler delayScheduler(
            RedissonClient redissonClient,
            ExpiredMessagePublisher publisher,
            RedisWheelKeys keys,
            TimewheelSettings settings,
            Clock clock) {
        return new RedisDelayScheduler(redissonClient, publisher, keys, settings, clock);
    }

    @Bean
    RedisTimewheelWorker redisTimewheelWorker(
            RedissonClient redissonClient,
            ExpiredMessagePublisher publisher,
            RedisWheelKeys keys,
            TimewheelSettings settings,
            TimewheelProperties properties,
            Clock clock) {
        return new RedisTimewheelWorker(redissonClient, publisher, keys, settings, clock, nodeId(properties));
    }

    @Bean
    TimewheelWorkerLifecycle timewheelWorkerLifecycle(
            RedisTimewheelWorker worker,
            TimewheelSettings settings) {
        return new TimewheelWorkerLifecycle(worker, settings.tickDuration());
    }

    @Bean
    HealthIndicator timewheelHealthIndicator(
            RedissonClient redissonClient,
            RedisWheelKeys keys,
            KafkaTemplate<String, String> kafkaTemplate,
            TimewheelProperties properties,
            Clock clock) {
        return new TimewheelHealthIndicator(
                redissonClient,
                keys,
                kafkaTemplate,
                clock,
                properties.getHealth().getMaxTickIdle());
    }

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    public static class TimewheelProperties {
        private Duration tickDuration = Duration.ofSeconds(10);
        private int ticksPerWheel = 100;
        private int maxCycle = 2;
        private String nodeId;
        private Redis redis = new Redis();
        private Kafka kafka = new Kafka();
        private Health health = new Health();

        public Duration getTickDuration() {
            return tickDuration;
        }

        public void setTickDuration(Duration tickDuration) {
            this.tickDuration = tickDuration;
        }

        public int getTicksPerWheel() {
            return ticksPerWheel;
        }

        public void setTicksPerWheel(int ticksPerWheel) {
            this.ticksPerWheel = ticksPerWheel;
        }

        public int getMaxCycle() {
            return maxCycle;
        }

        public void setMaxCycle(int maxCycle) {
            this.maxCycle = maxCycle;
        }

        public String getNodeId() {
            return nodeId;
        }

        public void setNodeId(String nodeId) {
            this.nodeId = nodeId;
        }

        public Redis getRedis() {
            return redis;
        }

        public void setRedis(Redis redis) {
            this.redis = redis;
        }

        public Kafka getKafka() {
            return kafka;
        }

        public void setKafka(Kafka kafka) {
            this.kafka = kafka;
        }

        public Health getHealth() {
            return health;
        }

        public void setHealth(Health health) {
            this.health = health;
        }
    }

    public static class Redis {
        private String keyPrefix = "timewheel-service";

        public String getKeyPrefix() {
            return keyPrefix;
        }

        public void setKeyPrefix(String keyPrefix) {
            this.keyPrefix = keyPrefix;
        }
    }

    public static class Kafka {
        private String inputTopic = "timewheel.delay.input";
        private String consumerGroup = "timewheel-service";
        private String deadLetterTopic = "timewheel.delay.dlq";
        private int publishMaxAttempts = 3;

        public String getInputTopic() {
            return inputTopic;
        }

        public void setInputTopic(String inputTopic) {
            this.inputTopic = inputTopic;
        }

        public String getConsumerGroup() {
            return consumerGroup;
        }

        public void setConsumerGroup(String consumerGroup) {
            this.consumerGroup = consumerGroup;
        }

        public String getDeadLetterTopic() {
            return deadLetterTopic;
        }

        public void setDeadLetterTopic(String deadLetterTopic) {
            this.deadLetterTopic = deadLetterTopic;
        }

        public int getPublishMaxAttempts() {
            return publishMaxAttempts;
        }

        public void setPublishMaxAttempts(int publishMaxAttempts) {
            this.publishMaxAttempts = publishMaxAttempts;
        }
    }

    public static class Health {
        private Duration maxTickIdle = Duration.ofSeconds(60);

        public Duration getMaxTickIdle() {
            return maxTickIdle;
        }

        public void setMaxTickIdle(Duration maxTickIdle) {
            this.maxTickIdle = maxTickIdle;
        }
    }

    private static String nodeId(TimewheelProperties properties) {
        if (properties.getNodeId() != null && !properties.getNodeId().isBlank()) {
            return properties.getNodeId().trim();
        }
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException ignored) {
            return "unknown-node";
        }
    }
}
