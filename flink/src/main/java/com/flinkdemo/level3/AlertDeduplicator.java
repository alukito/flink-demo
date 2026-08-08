package com.flinkdemo.level3;

import com.flinkdemo.level3.model.CepAlert;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/** Emits the first alert for an ID and bounds replay-protection state to the eight-hour demo horizon. */
public final class AlertDeduplicator extends KeyedProcessFunction<String, CepAlert, CepAlert> {
    private static final long serialVersionUID = 1L;

    private final String stateName;
    private transient ValueState<Boolean> emitted;

    public AlertDeduplicator(String stateName) {
        this.stateName = stateName;
    }

    @Override
    public void open(Configuration parameters) {
        emitted = getRuntimeContext().getState(stateDescriptor(stateName));
    }

    @Override
    public void processElement(CepAlert alert, Context context, Collector<CepAlert> out) throws Exception {
        if (!Boolean.TRUE.equals(emitted.value())) {
            emitted.update(true);
            out.collect(alert);
        }
    }

    static ValueStateDescriptor<Boolean> stateDescriptor(String stateName) {
        ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>(stateName, Types.BOOLEAN);
        descriptor.enableTimeToLive(
            StateTtlConfig.newBuilder(Time.hours(8))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build());
        return descriptor;
    }
}
