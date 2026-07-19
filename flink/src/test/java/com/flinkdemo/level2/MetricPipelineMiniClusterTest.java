package com.flinkdemo.level2;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flinkdemo.level2.model.EventEnvelope;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;

public class MetricPipelineMiniClusterTest {
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
}
