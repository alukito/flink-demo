package com.flinkdemo.level3;

import com.flinkdemo.level2.model.EventEnvelope;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public final class EventDeduplicator extends KeyedProcessFunction<String, EventEnvelope, EventEnvelope> {
    private static final long serialVersionUID = 1L;
    private transient ValueState<Boolean> seen;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("seen-event-id", Types.BOOLEAN);
        descriptor.enableTimeToLive(
            StateTtlConfig.newBuilder(Time.hours(8))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build());
        seen = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(EventEnvelope event, Context context, Collector<EventEnvelope> out) throws Exception {
        if (event == null || event.getEventId() == null || event.getEventId().isBlank()) {
            throw new IllegalArgumentException("event_id is required for CEP deduplication");
        }
        if (!Boolean.TRUE.equals(seen.value())) {
            seen.update(true);
            out.collect(event);
        }
    }
}
