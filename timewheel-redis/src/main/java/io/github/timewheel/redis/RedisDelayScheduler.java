package io.github.timewheel.redis;

import io.github.timewheel.engine.DelayedMessage;
import io.github.timewheel.engine.DelayScheduler;
import io.github.timewheel.engine.ExpiredMessagePublisher;
import io.github.timewheel.engine.ScheduledEntry;
import io.github.timewheel.engine.SchedulingResult;
import io.github.timewheel.engine.TimewheelSettings;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import org.redisson.api.RBucket;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;

public class RedisDelayScheduler implements DelayScheduler {

    private final RedissonClient redissonClient;
    private final ExpiredMessagePublisher publisher;
    private final RedisWheelKeys keys;
    private final TimewheelSettings settings;
    private final Clock clock;

    public RedisDelayScheduler(
            RedissonClient redissonClient,
            ExpiredMessagePublisher publisher,
            RedisWheelKeys keys,
            TimewheelSettings settings,
            Clock clock) {
        this.redissonClient = Objects.requireNonNull(redissonClient, "redissonClient is required");
        this.publisher = Objects.requireNonNull(publisher, "publisher is required");
        this.keys = Objects.requireNonNull(keys, "keys is required");
        this.settings = Objects.requireNonNull(settings, "settings is required");
        this.clock = Objects.requireNonNull(clock, "clock is required");
    }

    @Override
    public SchedulingResult submit(DelayedMessage message) {
        Objects.requireNonNull(message, "message is required");
        if (message.immediate()) {
            publisher.publish(message);
            return SchedulingResult.PUBLISHED_IMMEDIATELY;
        }

        settings.validateDelay(message.delayMillis());
        ScheduledEntry entry = ScheduledEntry.create(message, Instant.now(clock));
        Placement placement = place(message.delayMillis(), currentCycle(), currentTick(), settings);
        RMap<String, ScheduledEntry> slot = redissonClient.getMap(
                keys.slot(Math.toIntExact(placement.cycle()), placement.tick()));
        RMap<String, String> entrySlot = redissonClient.getMap(keys.entrySlot());
        slot.put(entry.entryId(), entry);
        entrySlot.put(entry.entryId(), placement.toSlotMetadata());
        return SchedulingResult.SCHEDULED;
    }

    public record Placement(long cycle, int tick) {

        private String toSlotMetadata() {
            return cycle + ":" + tick;
        }
    }

    public static Placement place(long delayMillis, long currentCycle, int currentTick, TimewheelSettings settings) {
        Objects.requireNonNull(settings, "settings is required");
        settings.validateDelay(delayMillis);
        long tickMillis = settings.tickDuration().toMillis();
        long ticksNeeded = Math.max(1L, ceilDiv(delayMillis, tickMillis));
        long absoluteTick = Math.addExact(
                Math.addExact(Math.multiplyExact(currentCycle, settings.ticksPerWheel()), currentTick),
                ticksNeeded);
        long cycle = (absoluteTick / settings.ticksPerWheel()) % settings.maxCycle();
        int tick = (int) (absoluteTick % settings.ticksPerWheel());
        return new Placement(cycle, tick);
    }

    private long currentCycle() {
        RBucket<Object> bucket = redissonClient.getBucket(keys.currentCycle());
        Object value = bucket.get();
        return value == null ? 0L : Long.parseLong(value.toString());
    }

    private int currentTick() {
        RBucket<Object> bucket = redissonClient.getBucket(keys.currentTick());
        Object value = bucket.get();
        return value == null ? 0 : Integer.parseInt(value.toString());
    }

    private static long ceilDiv(long dividend, long divisor) {
        return Math.floorDiv(Math.addExact(dividend, divisor - 1), divisor);
    }
}
