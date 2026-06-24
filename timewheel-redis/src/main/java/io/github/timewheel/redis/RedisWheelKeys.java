package io.github.timewheel.redis;

public final class RedisWheelKeys {

    static final String DEFAULT_PREFIX = "timewheel-service";

    private final String prefix;

    public RedisWheelKeys(String keyPrefix) {
        this.prefix = normalizePrefix(keyPrefix);
    }

    public String currentTick() {
        return timewheelKey("current-tick");
    }

    public String currentCycle() {
        return timewheelKey("current-cycle");
    }

    public String tickTime() {
        return timewheelKey("tick-time");
    }

    public String lastNode() {
        return timewheelKey("last-node");
    }

    public String slot(int cycle, int tick) {
        return timewheelKey("slot:" + cycle + ':' + tick);
    }

    public String entrySlot() {
        return timewheelKey("entry-slot");
    }

    public String tickLock() {
        return timewheelKey("tick-lock");
    }

    public String tryTickLock() {
        return timewheelKey("try-tick-lock");
    }

    private String timewheelKey(String suffix) {
        return prefix + ":timewheel:" + suffix;
    }

    private static String normalizePrefix(String keyPrefix) {
        if (keyPrefix == null || keyPrefix.isBlank()) {
            return DEFAULT_PREFIX;
        }
        String trimmed = keyPrefix.trim();
        while (trimmed.endsWith(":")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed.isBlank() ? DEFAULT_PREFIX : trimmed;
    }
}
