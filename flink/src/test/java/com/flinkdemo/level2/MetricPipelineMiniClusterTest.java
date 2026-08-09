package com.flinkdemo.level2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flinkdemo.level2.function.CountAggregate;
import com.flinkdemo.level2.function.CountWindowResult;
import com.flinkdemo.level2.model.EventEnvelope;
import com.flinkdemo.level2.model.WindowStat;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalSingleValueProcessAllWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;

public class MetricPipelineMiniClusterTest {
    @ClassRule public static final MiniClusterWithClientResource CLUSTER = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder().setNumberTaskManagers(1).setNumberSlotsPerTaskManager(2).build());

    @Test public void dailyRevenuePipelineRunsInMiniClusterAndKeepsRupiahInteger() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EventEnvelope first = new EventEnvelope("e1", "cart.checkout", "b1", "Buyer 1", "buyer", "2026-07-18T10:00:00Z", mapper.readTree("{\"total_amount\":150000}"));
        EventEnvelope second = new EventEnvelope("e2", "cart.checkout", "b2", "Buyer 2", "buyer", "2026-07-18T10:00:01Z", mapper.readTree("{\"total_amount\":339000}"));
        DataStream<EventEnvelope> input = env.fromCollection(List.of(first, second));
        List<Long> values = MetricJob.build(input, MetricDefinition.REVENUE).executeAndCollect(2).stream().map(stat -> stat.getValue()).collect(Collectors.toList());
        assertTrue(values.contains(489000L));
    }

    @Test public void countWindowsEmitGrowingSnapshotsThenAdvanceAtTheAlignedBoundary() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        List<EventEnvelope> events = List.of(
            new EventEnvelope("e1", "cart.checkout", "b1", "Buyer 1", "buyer", "2026-07-18T10:00:00Z", mapper.readTree("{}")),
            new EventEnvelope("e2", "cart.checkout", "b2", "Buyer 2", "buyer", "2026-07-18T10:00:01Z", mapper.readTree("{}")),
            new EventEnvelope("e3", "cart.checkout", "b3", "Buyer 3", "buyer", "2026-07-18T10:00:02Z", mapper.readTree("{}")));

        try (KeyedOneInputStreamOperatorTestHarness<Byte, EventEnvelope, WindowStat> harness =
                countWindowHarness()) {
            harness.open();
            harness.setProcessingTime(10L);
            harness.processElement(new StreamRecord<>(events.get(0)));
            harness.setProcessingTime(100L);

            assertEquals(List.of(1L), values(harness));
            assertEquals(List.of("1970-01-01T00:00:01Z"), windowEnds(harness));

            harness.processElement(new StreamRecord<>(events.get(1)));
            harness.setProcessingTime(200L);

            assertEquals(List.of(1L, 2L), values(harness));
            assertEquals(
                List.of("1970-01-01T00:00:01Z", "1970-01-01T00:00:01Z"),
                windowEnds(harness));

            harness.setProcessingTime(1_000L);
            harness.processElement(new StreamRecord<>(events.get(2)));
            harness.setProcessingTime(1_100L);

            List<WindowStat> output = harness.extractOutputValues();
            WindowStat nextWindow = output.get(output.size() - 1);
            assertEquals(1L, nextWindow.getValue());
            assertEquals("1970-01-01T00:00:02Z", nextWindow.getWindowEnd());
            assertEquals(
                1_000L,
                Instant.parse(nextWindow.getWindowEnd()).toEpochMilli()
                    - Instant.parse(output.get(0).getWindowEnd()).toEpochMilli());
        }
    }

    private static KeyedOneInputStreamOperatorTestHarness<Byte, EventEnvelope, WindowStat>
            countWindowHarness() throws Exception {
        ExecutionConfig config = new ExecutionConfig();
        NullByteKeySelector<EventEnvelope> keySelector = new NullByteKeySelector<>();
        TumblingProcessingTimeWindows assigner =
            MetricWindowing.windowAssigner(Time.milliseconds(1_000L));
        WindowOperator<Byte, EventEnvelope, Long, WindowStat, TimeWindow> operator =
            new WindowOperator<>(
                assigner,
                assigner.getWindowSerializer(config),
                keySelector,
                BasicTypeInfo.BYTE_TYPE_INFO.createSerializer(config.getSerializerConfig()),
                new AggregatingStateDescriptor<>(
                    "window-contents",
                    new CountAggregate(),
                    BasicTypeInfo.LONG_TYPE_INFO.createSerializer(config.getSerializerConfig())),
                new InternalSingleValueProcessAllWindowFunction<>(
                    new CountWindowResult("tx_count")),
                MetricWindowing.trigger(Time.milliseconds(100L)),
                0L,
                null);
        return new KeyedOneInputStreamOperatorTestHarness<>(
            operator, keySelector, BasicTypeInfo.BYTE_TYPE_INFO);
    }

    private static List<Long> values(
            KeyedOneInputStreamOperatorTestHarness<Byte, EventEnvelope, WindowStat> harness) {
        return harness.extractOutputValues().stream()
            .map(WindowStat::getValue)
            .collect(Collectors.toList());
    }

    private static List<String> windowEnds(
            KeyedOneInputStreamOperatorTestHarness<Byte, EventEnvelope, WindowStat> harness) {
        return harness.extractOutputValues().stream()
            .map(WindowStat::getWindowEnd)
            .collect(Collectors.toList());
    }
}
