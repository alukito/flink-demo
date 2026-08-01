package com.flinkdemo.level2.function;

import com.flinkdemo.level2.DailyTime;
import com.flinkdemo.level2.model.EventEnvelope;
import com.flinkdemo.level2.model.WindowStat;
import java.util.Collections;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public final class DailyAggregateFunction extends KeyedProcessFunction<String, EventEnvelope, WindowStat> {
    private final String metric; private final boolean revenue; private transient ValueState<Long> total;
    public DailyAggregateFunction(String metric, boolean revenue) { this.metric = metric; this.revenue = revenue; }
    @Override public void open(Configuration parameters) { total = getRuntimeContext().getState(new ValueStateDescriptor<>("daily-total", Long.class, 0L)); }
    @Override public void processElement(EventEnvelope event, Context context, Collector<WindowStat> out) throws Exception {
        long increment = revenue ? event.getPayload().path("total_amount").longValue() : 1L;
        long next = total.value() + increment;
        total.update(next);
        String windowEnd = DailyTime.windowEnd(context.getCurrentKey());
        out.collect(new WindowStat(metric, "daily", windowEnd, next, Collections.emptyMap()));
    }
}
