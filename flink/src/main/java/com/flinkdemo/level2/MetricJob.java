package com.flinkdemo.level2;

import com.flinkdemo.level2.function.CountAggregate;
import com.flinkdemo.level2.function.CountWindowResult;
import com.flinkdemo.level2.function.DailyAggregateFunction;
import com.flinkdemo.level2.function.TopProductWindowFunction;
import com.flinkdemo.level2.model.EventEnvelope;
import com.flinkdemo.level2.model.WindowStat;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public final class MetricJob {
    private MetricJob() {}

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        MetricDefinition definition = MetricDefinition.fromName(parameters.getRequired("metric"));
        String brokers = parameters.get("brokers", "kafka:9092");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(Duration.ofSeconds(30).toMillis());
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Duration.ofSeconds(10)));
        DataStream<EventEnvelope> source = env.fromSource(
            KafkaIO.source(brokers, definition),
            WatermarkStrategy.noWatermarks(),
            definition.metric() + "-source");
        build(source, definition)
            .sinkTo(KafkaIO.sink(brokers))
            .name(definition.metric() + "-kafka-sink");
        env.execute("level2-" + definition.metric());
    }

    public static DataStream<WindowStat> build(
            DataStream<EventEnvelope> source, MetricDefinition definition) {
        DataStream<WindowStat> output = null;
        if (definition.hasWindow()) {
            output = definition.isTopProduct()
                ? MetricWindowing.windowAll(source)
                    .process(new TopProductWindowFunction())
                    .name("top-product-window")
                : MetricWindowing.windowAll(source)
                    .aggregate(new CountAggregate(), new CountWindowResult(definition.metric()))
                    .name(definition.metric() + "-window");
        }
        if (definition.hasDaily()) {
            DataStream<WindowStat> daily = source
                .keyBy(event -> DailyTime.dateKey(event.getTimestamp()))
                .process(new DailyAggregateFunction(definition.metric(), definition.isRevenue()))
                .name(definition.metric() + "-daily");
            output = output == null ? daily : output.union(daily);
        }
        if (output == null) {
            throw new IllegalStateException("metric has no pipeline: " + definition.metric());
        }
        return output;
    }
}
