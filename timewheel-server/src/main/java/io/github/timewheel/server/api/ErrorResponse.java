package io.github.timewheel.server.api;

import java.time.Instant;

public record ErrorResponse(String errorCode, String errorMessage, Instant timestamp) {
}
