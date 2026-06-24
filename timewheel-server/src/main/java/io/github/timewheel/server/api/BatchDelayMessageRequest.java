package io.github.timewheel.server.api;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;

public record BatchDelayMessageRequest(@NotEmpty List<@Valid DelayMessageRequest> messages) {
}
