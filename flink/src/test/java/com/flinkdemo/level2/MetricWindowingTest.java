package com.flinkdemo.level2;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.Collection;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.junit.jupiter.api.Test;

class MetricWindowingTest {
    @Test void productionWindowsAreAlignedAndNonOverlapping() throws Exception {
        assertEquals("2026-07-18T07:05:00Z", windowEndAt("2026-07-18T07:01:00Z"));
        assertEquals("2026-07-18T07:05:00Z", windowEndAt("2026-07-18T07:03:00Z"));
        assertEquals("2026-07-18T07:15:00Z", windowEndAt("2026-07-18T07:10:00Z"));
    }

    private String windowEndAt(String processingTime) throws Exception {
        long now = Instant.parse(processingTime).toEpochMilli();
        Collection<TimeWindow> windows = MetricWindowing.windowAssigner(MetricWindowing.WINDOW_SIZE)
            .assignWindows(new Object(), Long.MIN_VALUE, new WindowAssigner.WindowAssignerContext() {
                @Override public long getCurrentProcessingTime() { return now; }
            });

        assertEquals(1, windows.size());
        return Instant.ofEpochMilli(windows.iterator().next().getEnd()).toString();
    }
}
