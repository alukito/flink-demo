package com.flinkdemo.level3;

import com.flinkdemo.level2.model.EventEnvelope;
import java.io.Serializable;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/** Advances live event time from the wall clock after the first source event. */
final class LiveClockWatermarkGenerator implements WatermarkGenerator<EventEnvelope> {
    private static final long serialVersionUID = 1L;

    @FunctionalInterface
    interface MillisecondClock extends Serializable {
        long currentTimeMillis();
    }

    private final long allowedLatenessMillis;
    private final MillisecondClock clock;
    private boolean sourceStarted;
    private long lastEmittedWatermark = Long.MIN_VALUE;

    LiveClockWatermarkGenerator(long allowedLatenessMillis) {
        this(allowedLatenessMillis, System::currentTimeMillis);
    }

    LiveClockWatermarkGenerator(long allowedLatenessMillis, MillisecondClock clock) {
        if (allowedLatenessMillis < 0) {
            throw new IllegalArgumentException("allowed lateness must not be negative");
        }
        this.allowedLatenessMillis = allowedLatenessMillis;
        this.clock = clock;
    }

    @Override
    public void onEvent(EventEnvelope event, long eventTimestamp, WatermarkOutput output) {
        sourceStarted = true;
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        if (!sourceStarted) {
            return;
        }
        long candidate = clock.currentTimeMillis() - allowedLatenessMillis;
        if (candidate > lastEmittedWatermark) {
            output.emitWatermark(new Watermark(candidate));
            lastEmittedWatermark = candidate;
        }
    }
}
