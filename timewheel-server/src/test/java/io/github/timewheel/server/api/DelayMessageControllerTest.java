package io.github.timewheel.server.api;

import io.github.timewheel.engine.DelayScheduler;
import io.github.timewheel.engine.SchedulingException;
import io.github.timewheel.engine.SchedulingResult;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(DelayMessageController.class)
class DelayMessageControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private DelayScheduler delayScheduler;

    @Test
    void acceptsValidDelayMessage() throws Exception {
        when(delayScheduler.submit(any())).thenReturn(SchedulingResult.SCHEDULED);

        mockMvc.perform(post("/api/delay-messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "id": "trace-1",
                                  "targetTopic": "orders.timeout",
                                  "key": "order-10001",
                                  "payload": {"orderId": "10001"},
                                  "headers": {"source": "api"},
                                  "delayMillis": 60000
                                }
                                """))
                .andExpect(status().isAccepted())
                .andExpect(content().string(""));

        verify(delayScheduler).submit(any());
    }

    @Test
    void acceptsBatchDelayMessages() throws Exception {
        when(delayScheduler.submit(any())).thenReturn(SchedulingResult.SCHEDULED);

        mockMvc.perform(post("/api/delay-messages:batch")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "messages": [
                                    {
                                      "targetTopic": "orders.timeout",
                                      "payload": {"orderId": "10001"},
                                      "delayMillis": 60000
                                    },
                                    {
                                      "targetTopic": "orders.ready",
                                      "payload": {"orderId": "10002"},
                                      "delayMillis": 1
                                    }
                                  ]
                                }
                                """))
                .andExpect(status().isAccepted())
                .andExpect(content().string(""));

        verify(delayScheduler, times(2)).submit(any());
    }

    @Test
    void rejectsInvalidDelayMessageRequest() throws Exception {
        mockMvc.perform(post("/api/delay-messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "targetTopic": " ",
                                  "payload": {"orderId": "10001"},
                                  "delayMillis": 60000
                                }
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errorCode").value("VALIDATION_FAILED"))
                .andExpect(jsonPath("$.errorMessage", containsString("targetTopic")));
    }

    @Test
    void mapsSchedulingExceptionToBadRequest() throws Exception {
        when(delayScheduler.submit(any())).thenThrow(new SchedulingException(
                "DELAY_OUT_OF_RANGE",
                "delayMillis exceeds configured max wheel range"));

        mockMvc.perform(post("/api/delay-messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "targetTopic": "orders.timeout",
                                  "payload": {"orderId": "10001"},
                                  "delayMillis": 999999999
                                }
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errorCode").value("DELAY_OUT_OF_RANGE"))
                .andExpect(jsonPath("$.errorMessage").value("delayMillis exceeds configured max wheel range"));
    }
}
