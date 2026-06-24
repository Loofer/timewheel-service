package io.github.timewheel.redis;

import io.github.timewheel.engine.ExpiredMessagePublisher;
import io.github.timewheel.engine.ScheduledEntry;
import io.github.timewheel.engine.TimewheelSettings;
import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.redisson.api.RBucket;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;

public class RedisTimewheelWorker {

    private static final long ORIGINAL_TICK_DRIFT_ALLOWANCE_MILLIS = 50L;

    private final RedissonClient redissonClient;
    private final ExpiredMessagePublisher publisher;
    private final RedisWheelKeys keys;
    private final TimewheelSettings settings;
    private final Clock clock;
    private final String nodeId;

    public RedisTimewheelWorker(
            RedissonClient redissonClient,
            ExpiredMessagePublisher publisher,
            RedisWheelKeys keys,
            TimewheelSettings settings,
            Clock clock,
            String nodeId) {
        this.redissonClient = Objects.requireNonNull(redissonClient, "redissonClient is required");
        this.publisher = Objects.requireNonNull(publisher, "publisher is required");
        this.keys = Objects.requireNonNull(keys, "keys is required");
        this.settings = Objects.requireNonNull(settings, "settings is required");
        this.clock = Objects.requireNonNull(clock, "clock is required");
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("nodeId is required");
        }
        this.nodeId = nodeId;
    }

    public TickResult tickOnce() {
        long currentCycle = currentCycle();
        int currentTick = currentTick();
        long currentTickTime = currentTickTimeMillis();
        String lastNode = lastNode();

        if (!ownsTick(currentTickTime, lastNode)) {
            return new TickResult(false, currentCycle, currentTick, currentCycle, currentTick, 0);
        }

        int expiredCount = drainSlot(currentCycle, currentTick);
        TickCursor next = nextCursor(currentCycle, currentTick);
        writeTickState(next);
        return new TickResult(true, currentCycle, currentTick, next.cycle(), next.tick(), expiredCount);
    }

    private boolean ownsTick(long currentTickTime, String lastNode) {
        if (lastNode == null || lastNode.isBlank()) {
            return true;
        }
        if (nodeId.equals(lastNode)) {
            return true;
        }
        return clock.millis() + ORIGINAL_TICK_DRIFT_ALLOWANCE_MILLIS - currentTickTime
                > settings.tickDuration().toMillis();
    }

    private int drainSlot(long cycle, int tick) {
        RMap<String, ScheduledEntry> slot = redissonClient.getMap(keys.slot(Math.toIntExact(cycle), tick));
        RMap<String, String> entrySlot = redissonClient.getMap(keys.entrySlot());
        Set<Map.Entry<String, ScheduledEntry>> entries = slot.readAllEntrySet();
        int expiredCount = 0;
        String slotMetadata = slotMetadata(cycle, tick);
        for (Map.Entry<String, ScheduledEntry> entry : entries) {
            ScheduledEntry removed = slot.remove(entry.getKey());
            if (removed == null) {
                continue;
            }
            if (slotMetadata.equals(entrySlot.get(entry.getKey()))) {
                entrySlot.remove(entry.getKey());
            }
            publisher.publish(removed.message());
            expiredCount++;
        }
        return expiredCount;
    }

    private TickCursor nextCursor(long currentCycle, int currentTick) {
        int nextTick = currentTick + 1;
        long nextCycle = currentCycle;
        if (nextTick >= settings.ticksPerWheel()) {
            nextTick = 0;
            nextCycle++;
            if (nextCycle >= settings.maxCycle()) {
                nextCycle = 0;
            }
        }
        return new TickCursor(nextCycle, nextTick);
    }

    private void writeTickState(TickCursor next) {
        bucket(keys.currentTick()).set(next.tick());
        bucket(keys.currentCycle()).set(next.cycle());
        bucket(keys.tickTime()).set(clock.millis());
        bucket(keys.lastNode()).set(nodeId);
    }

    private long currentCycle() {
        Object value = bucket(keys.currentCycle()).get();
        return value == null ? 0L : Long.parseLong(value.toString());
    }

    private int currentTick() {
        Object value = bucket(keys.currentTick()).get();
        return value == null ? 0 : Integer.parseInt(value.toString());
    }

    public long currentTickTimeMillis() {
        Object value = bucket(keys.tickTime()).get();
        return value == null ? clock.millis() : Long.parseLong(value.toString());
    }

    private String lastNode() {
        Object value = bucket(keys.lastNode()).get();
        return value == null ? "" : value.toString();
    }

    private RBucket<Object> bucket(String key) {
        return redissonClient.getBucket(key);
    }

    static String slotMetadata(long cycle, int tick) {
        return cycle + ":" + tick;
    }

    private record TickCursor(long cycle, int tick) {
    }

    public record TickResult(
            boolean owned,
            long currentCycle,
            int currentTick,
            long nextCycle,
            int nextTick,
            int expiredCount) {
    }
}
