package com.flinkdemo.level2;

import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public final class MetricWindowing {
    static final Time WINDOW_SIZE = Time.minutes(5);
    static final Time UPDATE_INTERVAL = Time.seconds(5);

    private MetricWindowing() {}

    static <T> AllWindowedStream<T, TimeWindow> windowAll(DataStream<T> source) {
        return windowAll(source, WINDOW_SIZE, UPDATE_INTERVAL);
    }

    static <T> AllWindowedStream<T, TimeWindow> windowAll(
            DataStream<T> source, Time windowSize, Time updateInterval) {
        return source
            .windowAll(windowAssigner(windowSize))
            .trigger(trigger(updateInterval));
    }

    static TumblingProcessingTimeWindows windowAssigner(Time windowSize) {
        return TumblingProcessingTimeWindows.of(windowSize);
    }

    static ContinuousProcessingTimeTrigger<TimeWindow> trigger(Time updateInterval) {
        return ContinuousProcessingTimeTrigger.of(updateInterval);
    }
}
