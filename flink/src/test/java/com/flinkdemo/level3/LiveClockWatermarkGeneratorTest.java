package com.flinkdemo.level3;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.flinkdemo.level2.model.EventEnvelope;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.junit.jupiter.api.Test;

class LiveClockWatermarkGeneratorTest {
    @Test
    void advancesQuietStreamFromClockAfterFirstEvent() {
        ManualClock clock = new ManualClock(10_000L);
        LiveClockWatermarkGenerator generator = new LiveClockWatermarkGenerator(5_000L, clock);
        RecordingOutput output = new RecordingOutput();

        generator.onPeriodicEmit(output);
        generator.onEvent(new EventEnvelope(), 10_000L, output);
        generator.onPeriodicEmit(output);
        clock.set(16_000L);
        generator.onPeriodicEmit(output);

        assertEquals(List.of(5_000L, 11_000L), output.timestamps);
    }

    @Test
    void neverRegressesWhenClockMovesBackward() {
        ManualClock clock = new ManualClock(16_000L);
        LiveClockWatermarkGenerator generator = new LiveClockWatermarkGenerator(5_000L, clock);
        RecordingOutput output = new RecordingOutput();

        generator.onEvent(new EventEnvelope(), 10_000L, output);
        generator.onPeriodicEmit(output);
        clock.set(14_000L);
        generator.onPeriodicEmit(output);

        assertEquals(List.of(11_000L), output.timestamps);
    }

    private static final class ManualClock implements LiveClockWatermarkGenerator.MillisecondClock {
        private long currentTimeMillis;

        private ManualClock(long currentTimeMillis) {
            this.currentTimeMillis = currentTimeMillis;
        }

        @Override
        public long currentTimeMillis() {
            return currentTimeMillis;
        }

        private void set(long currentTimeMillis) {
            this.currentTimeMillis = currentTimeMillis;
        }
    }

    private static final class RecordingOutput implements WatermarkOutput {
        private final List<Long> timestamps = new ArrayList<>();

        @Override
        public void emitWatermark(Watermark watermark) {
            timestamps.add(watermark.getTimestamp());
        }

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {}
    }
}
