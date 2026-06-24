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
import io.github.timewheel.engine.TimewheelSettings;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RBucket;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;

class RedisTimewheelWorkerTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-06-24T02:00:00Z"), ZoneOffset.UTC);
    private static final TimewheelSettings SETTINGS = new TimewheelSettings(Duration.ofSeconds(10), 100, 2);

    private RedissonClient redissonClient;
    private ExpiredMessagePublisher publisher;
    private RedisWheelKeys keys;
    private RBucket<Object> currentTick;
    private RBucket<Object> currentCycle;
    private RBucket<Object> tickTime;
    private RBucket<Object> lastNode;

    @BeforeEach
    void setUp() {
        redissonClient = mock(RedissonClient.class);
        publisher = mock(ExpiredMessagePublisher.class);
        keys = new RedisWheelKeys("orders");
        currentTick = bucket(keys.currentTick());
        currentCycle = bucket(keys.currentCycle());
        tickTime = bucket(keys.tickTime());
        lastNode = bucket(keys.lastNode());
    }

    @Test
    void rejectsNullConstructorDependencies() {
        assertThatThrownBy(() -> new RedisTimewheelWorker(null, publisher, keys, SETTINGS, CLOCK, "node-a"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("redissonClient");
        assertThatThrownBy(() -> new RedisTimewheelWorker(redissonClient, null, keys, SETTINGS, CLOCK, "node-a"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("publisher");
        assertThatThrownBy(() -> new RedisTimewheelWorker(redissonClient, publisher, null, SETTINGS, CLOCK, "node-a"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("keys");
        assertThatThrownBy(() -> new RedisTimewheelWorker(redissonClient, publisher, keys, null, CLOCK, "node-a"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("settings");
        assertThatThrownBy(() -> new RedisTimewheelWorker(redissonClient, publisher, keys, SETTINGS, null, "node-a"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("clock");
        assertThatThrownBy(() -> new RedisTimewheelWorker(redissonClient, publisher, keys, SETTINGS, CLOCK, " "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nodeId");
    }

    @Test
    void currentNodeDrainsSlotAndAdvancesTick() {
        when(currentCycle.get()).thenReturn(1L);
        when(currentTick.get()).thenReturn(98);
        when(lastNode.get()).thenReturn("node-a");
        when(tickTime.get()).thenReturn(CLOCK.millis() - 1_000);
        ScheduledEntry entry = entry("entry-1");
        RMap<String, ScheduledEntry> slot = slot(1, 98, Map.of("entry-1", entry));
        RMap<String, String> entrySlot = entrySlot(Map.of("entry-1", "1:98"));
        when(slot.remove("entry-1")).thenReturn(entry);
        RedisTimewheelWorker worker = new RedisTimewheelWorker(
                redissonClient, publisher, keys, SETTINGS, CLOCK, "node-a");

        RedisTimewheelWorker.TickResult result = worker.tickOnce();

        assertThat(result.owned()).isTrue();
        assertThat(result.expiredCount()).isEqualTo(1);
        assertThat(result.nextCycle()).isEqualTo(1L);
        assertThat(result.nextTick()).isEqualTo(99);
        verify(slot).remove("entry-1");
        verify(entrySlot).remove("entry-1");
        verify(publisher).publish(entry.message());
        verify(currentTick).set(99);
        verify(currentCycle).set(1L);
        verify(tickTime).set(CLOCK.millis());
        verify(lastNode).set("node-a");
    }

    @Test
    void emptyOwnerCanTakeOwnership() {
        when(currentCycle.get()).thenReturn(0L);
        when(currentTick.get()).thenReturn(0);
        when(lastNode.get()).thenReturn("");
        when(tickTime.get()).thenReturn(null);
        slot(0, 0, Map.of());
        entrySlot(Map.of());
        RedisTimewheelWorker worker = new RedisTimewheelWorker(
                redissonClient, publisher, keys, SETTINGS, CLOCK, "node-a");

        RedisTimewheelWorker.TickResult result = worker.tickOnce();

        assertThat(result.owned()).isTrue();
        assertThat(result.nextTick()).isEqualTo(1);
        verify(lastNode).set("node-a");
    }

    @Test
    void staleDifferentOwnerCanBeReplaced() {
        when(currentCycle.get()).thenReturn(0L);
        when(currentTick.get()).thenReturn(5);
        when(lastNode.get()).thenReturn("node-b");
        when(tickTime.get()).thenReturn(CLOCK.millis() - SETTINGS.tickDuration().toMillis() - 100);
        slot(0, 5, Map.of());
        entrySlot(Map.of());
        RedisTimewheelWorker worker = new RedisTimewheelWorker(
                redissonClient, publisher, keys, SETTINGS, CLOCK, "node-a");

        RedisTimewheelWorker.TickResult result = worker.tickOnce();

        assertThat(result.owned()).isTrue();
        assertThat(result.nextTick()).isEqualTo(6);
        verify(lastNode).set("node-a");
    }

    @Test
    void freshDifferentOwnerDoesNotDrainOrAdvance() {
        when(currentCycle.get()).thenReturn(0L);
        when(currentTick.get()).thenReturn(5);
        when(lastNode.get()).thenReturn("node-b");
        when(tickTime.get()).thenReturn(CLOCK.millis() - 1_000);
        RMap<String, ScheduledEntry> slot = slot(0, 5, Map.of("entry-1", entry("entry-1")));
        RedisTimewheelWorker worker = new RedisTimewheelWorker(
                redissonClient, publisher, keys, SETTINGS, CLOCK, "node-a");

        RedisTimewheelWorker.TickResult result = worker.tickOnce();

        assertThat(result.owned()).isFalse();
        assertThat(result.expiredCount()).isZero();
        verify(slot, never()).remove(any());
        verify(publisher, never()).publish(any());
        verify(currentTick, never()).set(any());
        verify(currentCycle, never()).set(any());
        verify(lastNode, never()).set(any());
    }

    @Test
    void tickWrapIncrementsCycleModuloMaxCycle() {
        when(currentCycle.get()).thenReturn(1L);
        when(currentTick.get()).thenReturn(99);
        when(lastNode.get()).thenReturn("node-a");
        when(tickTime.get()).thenReturn(CLOCK.millis() - 1_000);
        slot(1, 99, Map.of());
        entrySlot(Map.of());
        RedisTimewheelWorker worker = new RedisTimewheelWorker(
                redissonClient, publisher, keys, SETTINGS, CLOCK, "node-a");

        RedisTimewheelWorker.TickResult result = worker.tickOnce();

        assertThat(result.nextTick()).isZero();
        assertThat(result.nextCycle()).isZero();
        verify(currentTick).set(0);
        verify(currentCycle).set(0L);
    }

    @Test
    void leavesIndicatorWhenMetadataNoLongerPointsAtExpiredSlot() {
        when(currentCycle.get()).thenReturn(0L);
        when(currentTick.get()).thenReturn(3);
        when(lastNode.get()).thenReturn("node-a");
        when(tickTime.get()).thenReturn(CLOCK.millis() - 1_000);
        ScheduledEntry entry = entry("entry-1");
        RMap<String, ScheduledEntry> slot = slot(0, 3, Map.of("entry-1", entry));
        RMap<String, String> entrySlot = entrySlot(Map.of("entry-1", "0:4"));
        when(slot.remove("entry-1")).thenReturn(entry);
        RedisTimewheelWorker worker = new RedisTimewheelWorker(
                redissonClient, publisher, keys, SETTINGS, CLOCK, "node-a");

        RedisTimewheelWorker.TickResult result = worker.tickOnce();

        assertThat(result.expiredCount()).isEqualTo(1);
        verify(entrySlot, never()).remove("entry-1");
        verify(publisher).publish(entry.message());
    }

    @SuppressWarnings("unchecked")
    private RBucket<Object> bucket(String key) {
        RBucket<Object> bucket = mock(RBucket.class);
        when(redissonClient.getBucket(key)).thenReturn(bucket);
        return bucket;
    }

    @SuppressWarnings("unchecked")
    private RMap<String, ScheduledEntry> slot(int cycle, int tick, Map<String, ScheduledEntry> entries) {
        RMap<String, ScheduledEntry> slot = mock(RMap.class);
        when(redissonClient.<String, ScheduledEntry>getMap(keys.slot(cycle, tick))).thenReturn(slot);
        when(slot.readAllEntrySet()).thenReturn(Set.copyOf(entries.entrySet()));
        return slot;
    }

    @SuppressWarnings("unchecked")
    private RMap<String, String> entrySlot(Map<String, String> entries) {
        RMap<String, String> entrySlot = mock(RMap.class);
        when(redissonClient.<String, String>getMap(keys.entrySlot())).thenReturn(entrySlot);
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            when(entrySlot.get(entry.getKey())).thenReturn(entry.getValue());
        }
        return entrySlot;
    }

    private static ScheduledEntry entry(String entryId) {
        DelayedMessage message = new DelayedMessage(
                "trace-" + entryId,
                "orders.ready",
                "key-" + entryId,
                OBJECT_MAPPER.createObjectNode().put("entryId", entryId),
                Map.of(),
                10_000L);
        return new ScheduledEntry(entryId, message, 10_000L, CLOCK.instant());
    }
}
