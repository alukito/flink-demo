package com.flinkdemo.level3;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.junit.jupiter.api.Test;

class AlertDeduplicatorTest {
    @Test
    void configuresOutputDeduplicationStateToExpireAfterEightHours() {
        StateTtlConfig ttl = AlertDeduplicator.stateDescriptor("test-alert-emitted").getTtlConfig();

        assertEquals(8L * 60 * 60 * 1000, ttl.getTimeToLive().toMillis());
        assertEquals(StateTtlConfig.UpdateType.OnCreateAndWrite, ttl.getUpdateType());
        assertEquals(StateTtlConfig.StateVisibility.NeverReturnExpired, ttl.getStateVisibility());
    }
}
