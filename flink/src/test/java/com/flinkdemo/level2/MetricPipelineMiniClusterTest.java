package com.flinkdemo.level2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flinkdemo.level2.function.CountAggregate;
import com.flinkdemo.level2.function.CountWindowResult;
import com.flinkdemo.level2.model.EventEnvelope;
import com.flinkdemo.level2.model.WindowStat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.CloseableIterator;
import org.junit.ClassRule;
import org.junit.Test;

public class MetricPipelineMiniClusterTest {
    private static final long TEST_WINDOW_SIZE_MILLIS = 2_400L;
    private static final long TEST_TRIGGER_INTERVAL_MILLIS = 300L;

    @ClassRule public static final MiniClusterWithClientResource CLUSTER = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder().setNumberTaskManagers(1).setNumberSlotsPerTaskManager(2).build());

    @Test public void dailyRevenuePipelineRunsInMiniClusterAndKeepsRupiahInteger() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EventEnvelope first = new EventEnvelope("e1", "cart.checkout", "b1", "buyer", "2026-07-18T10:00:00Z", mapper.readTree("{\"total_amount\":150000}"));
        EventEnvelope second = new EventEnvelope("e2", "cart.checkout", "b2", "buyer", "2026-07-18T10:00:01Z", mapper.readTree("{\"total_amount\":339000}"));
        DataStream<EventEnvelope> input = env.fromCollection(List.of(first, second));
        List<Long> values = MetricJob.build(input, MetricDefinition.REVENUE).executeAndCollect(2).stream().map(stat -> stat.getValue()).collect(Collectors.toList());
        assertTrue(values.contains(489000L));
    }

    @SuppressWarnings("deprecation")
    @Test public void countWindowsEmitGrowingSnapshotsThenAdvanceAtTheAlignedBoundary() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        List<EventEnvelope> events = List.of(
            new EventEnvelope("e1", "cart.checkout", "b1", "buyer", "2026-07-18T10:00:00Z", mapper.readTree("{}")),
            new EventEnvelope("e2", "cart.checkout", "b2", "buyer", "2026-07-18T10:00:01Z", mapper.readTree("{}")),
            new EventEnvelope("e3", "cart.checkout", "b3", "buyer", "2026-07-18T10:00:02Z", mapper.readTree("{}")));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<EventEnvelope> input = env
            .addSource(new AlignedProcessingTimeSource(events, TEST_WINDOW_SIZE_MILLIS))
            .setParallelism(1);

        List<WindowStat> output = new ArrayList<>();
        try (CloseableIterator<WindowStat> iterator = MetricWindowing
                .windowAll(
                    input,
                    Time.milliseconds(TEST_WINDOW_SIZE_MILLIS),
                    Time.milliseconds(TEST_TRIGGER_INTERVAL_MILLIS))
                .aggregate(new CountAggregate(), new CountWindowResult("tx_count"))
                .executeAndCollect("aligned-window-snapshots-test")) {
            while (iterator.hasNext()) output.add(iterator.next());
        }

        List<String> windowEnds = output.stream()
            .map(WindowStat::getWindowEnd)
            .distinct()
            .sorted()
            .collect(Collectors.toList());
        assertEquals(2, windowEnds.size());

        List<Long> firstWindowValues = valuesFor(output, windowEnds.get(0));
        assertTrue(firstWindowValues.contains(1L));
        assertTrue(firstWindowValues.contains(2L));
        assertEquals(2L, firstWindowValues.get(firstWindowValues.size() - 1));
        assertTrue(valuesFor(output, windowEnds.get(1)).contains(1L));
        assertEquals(
            TEST_WINDOW_SIZE_MILLIS,
            Instant.parse(windowEnds.get(1)).toEpochMilli()
                - Instant.parse(windowEnds.get(0)).toEpochMilli());
    }

    private static List<Long> valuesFor(List<WindowStat> output, String windowEnd) {
        return output.stream()
            .filter(stat -> windowEnd.equals(stat.getWindowEnd()))
            .map(WindowStat::getValue)
            .collect(Collectors.toList());
    }

    @SuppressWarnings("deprecation")
    private static final class AlignedProcessingTimeSource implements SourceFunction<EventEnvelope> {
        private final List<EventEnvelope> events;
        private final long windowSizeMillis;
        private volatile boolean running = true;

        private AlignedProcessingTimeSource(List<EventEnvelope> events, long windowSizeMillis) {
            this.events = events;
            this.windowSizeMillis = windowSizeMillis;
        }

        @Override public void run(SourceContext<EventEnvelope> context) throws Exception {
            long firstWindowStart =
                ((System.currentTimeMillis() / windowSizeMillis) + 1L) * windowSizeMillis;
            if (!waitUntil(firstWindowStart + 400L)) return;
            emit(context, events.get(0));
            if (!waitUntil(firstWindowStart + 1_300L)) return;
            emit(context, events.get(1));
            if (!waitUntil(firstWindowStart + windowSizeMillis + 400L)) return;
            emit(context, events.get(2));
            waitUntil(firstWindowStart + windowSizeMillis + 1_300L);
        }

        @Override public void cancel() {
            running = false;
        }

        private void emit(SourceContext<EventEnvelope> context, EventEnvelope event) {
            synchronized (context.getCheckpointLock()) {
                context.collect(event);
            }
        }

        private boolean waitUntil(long deadlineMillis) throws InterruptedException {
            while (running) {
                long remaining = deadlineMillis - System.currentTimeMillis();
                if (remaining <= 0L) return true;
                Thread.sleep(Math.min(remaining, 50L));
            }
            return false;
        }
    }
}
