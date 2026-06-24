package io.github.timewheel.server.api;

import io.github.timewheel.engine.DelayScheduler;
import io.github.timewheel.engine.SchedulingException;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.stream.Collectors;

@RestController
public class DelayMessageController {

    private final DelayScheduler delayScheduler;

    public DelayMessageController(DelayScheduler delayScheduler) {
        this.delayScheduler = delayScheduler;
    }

    @PostMapping("/api/delay-messages")
    public ResponseEntity<Void> submit(@Valid @RequestBody DelayMessageRequest request) {
        delayScheduler.submit(request.toMessage());
        return ResponseEntity.accepted().build();
    }

    @PostMapping("/api/delay-messages:batch")
    public ResponseEntity<Void> submitBatch(@Valid @RequestBody BatchDelayMessageRequest request) {
        request.messages().forEach(message -> delayScheduler.submit(message.toMessage()));
        return ResponseEntity.accepted().build();
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> validationException(MethodArgumentNotValidException ex) {
        String message = ex.getBindingResult()
                .getFieldErrors()
                .stream()
                .map(this::formatFieldError)
                .collect(Collectors.joining("; "));
        return badRequest("VALIDATION_FAILED", message);
    }

    @ExceptionHandler({IllegalArgumentException.class})
    public ResponseEntity<ErrorResponse> illegalArgumentException(IllegalArgumentException ex) {
        return badRequest("VALIDATION_FAILED", ex.getMessage());
    }

    @ExceptionHandler(SchedulingException.class)
    public ResponseEntity<ErrorResponse> schedulingException(SchedulingException ex) {
        return badRequest(ex.errorCode(), ex.getMessage());
    }

    private ResponseEntity<ErrorResponse> badRequest(String errorCode, String errorMessage) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(new ErrorResponse(errorCode, errorMessage, Instant.now()));
    }

    private String formatFieldError(FieldError error) {
        return error.getField() + " " + error.getDefaultMessage();
    }
}
