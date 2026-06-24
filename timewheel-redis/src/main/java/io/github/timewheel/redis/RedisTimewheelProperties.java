package io.github.timewheel.redis;

import java.time.Duration;

public class RedisTimewheelProperties {

    private String keyPrefix = RedisWheelKeys.DEFAULT_PREFIX;
    private Duration tickDuration = Duration.ofSeconds(10);
    private int ticksPerWheel = 100;
    private int maxCycle = 2;

    public String getKeyPrefix() {
        return keyPrefix;
    }

    public void setKeyPrefix(String keyPrefix) {
        this.keyPrefix = keyPrefix;
    }

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
}
