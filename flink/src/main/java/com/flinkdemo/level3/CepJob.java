package com.flinkdemo.level3;

import com.flinkdemo.level2.model.EventEnvelope;
import com.flinkdemo.level3.model.CepAlert;
import com.flinkdemo.level3.pattern.AbandonedCartPattern;
import com.flinkdemo.level3.pattern.DeliveryCompletedPattern;
import com.flinkdemo.level3.pattern.OrderSurgePattern;
import com.flinkdemo.level3.pattern.SlowDeliveryPattern;
import com.flinkdemo.level3.pattern.TrendingProductPattern;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Entrypoint for one independently submitted Level 3 CEP detector. */
public final class CepJob {
    private CepJob() {}

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        CepPattern pattern = CepPattern.fromName(parameters.getRequired("pattern"));
        String brokers = parameters.get("brokers", "kafka:9092");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(Duration.ofSeconds(30).toMillis());
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Duration.ofSeconds(10)));

        DataStream<EventEnvelope> source = env.fromSource(
            CepJobSupport.source(brokers, pattern),
            WatermarkStrategy.noWatermarks(),
            pattern.pattern() + "-source");

        build(source, pattern)
            .sinkTo(CepJobSupport.alertSink(brokers))
            .name(pattern.pattern() + "-alert-sink");
        env.execute(pattern.jobName());
    }

    static DataStream<CepAlert> build(DataStream<EventEnvelope> source, CepPattern pattern) {
        switch (pattern) {
            case ABANDONED_CART:
                return AbandonedCartPattern.build(source);
            case TRENDING_PRODUCT:
                return TrendingProductPattern.build(source);
            case SLOW_DELIVERY:
                return SlowDeliveryPattern.build(source);
            case ORDER_SURGE:
                return OrderSurgePattern.build(source);
            case DELIVERY_COMPLETED:
                return DeliveryCompletedPattern.build(source);
            default:
                throw new IllegalStateException("unhandled CEP pattern: " + pattern);
        }
    }
}
