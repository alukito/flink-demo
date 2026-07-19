package com.flinkdemo.level2.function;

import com.flinkdemo.level2.model.WindowStat;
import java.time.Instant;
import java.util.Collections;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public final class CountWindowResult extends ProcessAllWindowFunction<Long, WindowStat, TimeWindow> {
    private final String metric;
    public CountWindowResult(String metric) { this.metric = metric; }
    @Override public void process(Context context, Iterable<Long> values, Collector<WindowStat> out) {
        out.collect(new WindowStat(metric, "window", Instant.ofEpochMilli(context.window().getEnd()).toString(), values.iterator().next(), Collections.emptyMap()));
    }
}
