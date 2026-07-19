package com.flinkdemo.level2.function;

import com.flinkdemo.level2.model.EventEnvelope;
import org.apache.flink.api.common.functions.AggregateFunction;

public final class CountAggregate implements AggregateFunction<EventEnvelope, Long, Long> {
    public Long createAccumulator() { return 0L; }
    public Long add(EventEnvelope value, Long accumulator) { return accumulator + 1L; }
    public Long getResult(Long accumulator) { return accumulator; }
    public Long merge(Long left, Long right) { return left + right; }
}
