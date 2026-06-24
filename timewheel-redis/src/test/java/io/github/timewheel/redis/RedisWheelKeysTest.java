package io.github.timewheel.redis;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class RedisWheelKeysTest {

    @Test
    void buildsKeysFromPrefix() {
        RedisWheelKeys keys = new RedisWheelKeys("orders");

        assertThat(keys.currentTick()).isEqualTo("orders:timewheel:current-tick");
        assertThat(keys.currentCycle()).isEqualTo("orders:timewheel:current-cycle");
        assertThat(keys.slot(2, 12)).isEqualTo("orders:timewheel:slot:2:12");
        assertThat(keys.entrySlot()).isEqualTo("orders:timewheel:entry-slot");
        assertThat(keys.tickLock()).isEqualTo("orders:timewheel:tick-lock");
        assertThat(keys.tryTickLock()).isEqualTo("orders:timewheel:try-tick-lock");
    }

    @Test
    void defaultsBlankPrefixAndTrimsTrailingColons() {
        assertThat(new RedisWheelKeys(null).currentTick())
                .isEqualTo("timewheel-service:timewheel:current-tick");
        assertThat(new RedisWheelKeys("   ").currentTick())
                .isEqualTo("timewheel-service:timewheel:current-tick");
        assertThat(new RedisWheelKeys("orders:::").slot(0, 0))
                .isEqualTo("orders:timewheel:slot:0:0");
    }
}
