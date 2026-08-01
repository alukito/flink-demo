package com.flinkdemo.level2;

import com.flinkdemo.level2.model.EventEnvelope;
import com.flinkdemo.level2.model.WindowStat;
import com.flinkdemo.level2.serde.EventEnvelopeSchema;
import com.flinkdemo.level2.serde.WindowStatSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public final class KafkaIO {
    private KafkaIO() {}

    public static KafkaSource<EventEnvelope> source(String brokers, MetricDefinition definition) {
        return KafkaSource.<EventEnvelope>builder()
            .setBootstrapServers(brokers)
            .setTopics(definition.sourceTopic())
            .setGroupId("flink-level2-" + definition.metric())
            .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
            .setValueOnlyDeserializer(new EventEnvelopeSchema())
            .build();
    }

    public static KafkaSink<WindowStat> sink(String brokers) {
        return KafkaSink.<WindowStat>builder()
            .setBootstrapServers(brokers)
            .setRecordSerializer(new WindowStatSchema("flink.window.stats"))
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
    }
}
