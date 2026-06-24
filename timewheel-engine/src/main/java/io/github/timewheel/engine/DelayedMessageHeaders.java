package io.github.timewheel.engine;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public final class DelayedMessageHeaders {

    private DelayedMessageHeaders() {
    }

    public static byte[] toUtf8(String value) {
        return value == null ? null : value.getBytes(StandardCharsets.UTF_8);
    }

    public static String fromUtf8(byte[] value) {
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }

    public static Map<String, String> copyOf(Map<String, String> headers) {
        return headers == null ? Map.of() : Map.copyOf(headers);
    }
}
