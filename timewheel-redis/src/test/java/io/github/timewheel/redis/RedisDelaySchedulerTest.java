package io.github.timewheel.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.timewheel.engine.DelayedMessage;
import io.github.timewheel.engine.ExpiredMessagePublisher;
import io.github.timewheel.engine.ScheduledEntry;
import io.github.timewheel.engine.SchedulingResult;
import io.github.timewheel.engine.TimewheelSettings;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.redisson.api.RBucket;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;

class RedisDelaySchedulerTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-06-24T01:00:00Z"), ZoneOffset.UTC);
    private static final TimewheelSettings SETTINGS = new TimewheelSettings(Duration.ofSeconds(10), 100, 2);

    private RedissonClient redissonClient;
    private ExpiredMessagePublisher publisher;
    private RedisWheelKeys keys;

    @BeforeEach
    void setUp() {
        redissonClient = mock(RedissonClient.class);
        publisher = mock(ExpiredMessagePublisher.class);
        keys = new RedisWheelKeys("orders");
    }

    @Test
    void rejectsNullConstructorDependencies() {
        assertThatThrownBy(() -> new RedisDelayScheduler(null, publisher, keys, SETTINGS, CLOCK))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("redissonClient");
        assertThatThrownBy(() -> new RedisDelayScheduler(redissonClient, null, keys, SETTINGS, CLOCK))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("publisher");
        assertThatThrownBy(() -> new RedisDelayScheduler(redissonClient, publisher, null, SETTINGS, CLOCK))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("keys");
        assertThatThrownBy(() -> new RedisDelayScheduler(redissonClient, publisher, keys, null, CLOCK))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("settings");
        assertThatThrownBy(() -> new RedisDelayScheduler(redissonClient, publisher, keys, SETTINGS, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("clock");
    }

    @Test
    void publishesImmediateMessagesWithoutRedisWrites() {
        RedisDelayScheduler scheduler = new RedisDelayScheduler(
                redissonClient, publisher, keys, SETTINGS, CLOCK);
        DelayedMessage message = message(0L);

        SchedulingResult result = scheduler.submit(message);

        assertThat(result).isEqualTo(SchedulingResult.PUBLISHED_IMMEDIATELY);
        verify(publisher).publish(message);
        verify(redissonClient, never()).getMap(any(String.class));
    }

    @Test
    void storesDelayedMessageInCalculatedSlotAndIndicator() {
        @SuppressWarnings("unchecked")
        RMap<String, ScheduledEntry> slot = mock(RMap.class);
        @SuppressWarnings("unchecked")
        RMap<String, String> entrySlot = mock(RMap.class);
        @SuppressWarnings("unchecked")
        RBucket<Object> currentCycle = mock(RBucket.class);
        @SuppressWarnings("unchecked")
        RBucket<Object> currentTick = mock(RBucket.class);
        when(redissonClient.getBucket(keys.currentCycle())).thenReturn(currentCycle);
        when(redissonClient.getBucket(keys.currentTick())).thenReturn(currentTick);
        when(currentCycle.get()).thenReturn(1L);
        when(currentTick.get()).thenReturn(99);
        when(redissonClient.<String, ScheduledEntry>getMap(keys.slot(0, 1))).thenReturn(slot);
        when(redissonClient.<String, String>getMap(keys.entrySlot())).thenReturn(entrySlot);
        RedisDelayScheduler scheduler = new RedisDelayScheduler(redissonClient, publisher, keys, SETTINGS, CLOCK);
        DelayedMessage message = message(20_000L);

        SchedulingResult result = scheduler.submit(message);

        assertThat(result).isEqualTo(SchedulingResult.SCHEDULED);
        ArgumentCaptor<String> slotEntryId = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ScheduledEntry> scheduledEntry = ArgumentCaptor.forClass(ScheduledEntry.class);
        verify(slot).put(slotEntryId.capture(), scheduledEntry.capture());
        ArgumentCaptor<String> indicatorEntryId = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> indicatorSlot = ArgumentCaptor.forClass(String.class);
        verify(entrySlot).put(indicatorEntryId.capture(), indicatorSlot.capture());
        assertThat(scheduledEntry.getValue().entryId()).isEqualTo(slotEntryId.getValue());
        assertThat(indicatorEntryId.getValue()).isEqualTo(slotEntryId.getValue());
        assertThat(indicatorSlot.getValue()).isEqualTo("0:1");
        assertThat(scheduledEntry.getValue().message()).isEqualTo(message);
        assertThat(scheduledEntry.getValue().acceptedAt()).isEqualTo(Instant.now(CLOCK));
        assertThat(scheduledEntry.getValue().remainingDelayMillis()).isEqualTo(20_000L);
        verify(publisher, never()).publish(any(DelayedMessage.class));
    }

    @Test
    void rejectsNullMessagesClearly() {
        RedisDelayScheduler scheduler = new RedisDelayScheduler(
                redissonClient,
                publisher,
                keys,
                SETTINGS,
                CLOCK);

        assertThatThrownBy(() -> scheduler.submit(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("message");
    }

    private static DelayedMessage message(long delayMillis) {
        return new DelayedMessage(
                "trace-1",
                "orders.ready",
                "key-1",
                OBJECT_MAPPER.createObjectNode().put("orderId", "A-1"),
                Map.of(),
                delayMillis);
    }
}
