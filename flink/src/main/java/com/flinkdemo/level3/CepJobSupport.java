package com.flinkdemo.level3;

import com.flinkdemo.level2.model.EventEnvelope;
import com.flinkdemo.level2.serde.EventEnvelopeSchema;
import com.flinkdemo.level3.model.CepAlert;
import com.flinkdemo.level3.serde.CepAlertSchema;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public final class CepJobSupport {
    private static final Duration MAX_OUT_OF_ORDERNESS = Duration.ofSeconds(5);

    private CepJobSupport() {}

    public static KafkaSource<EventEnvelope> source(String brokers, CepPattern pattern) {
        return KafkaSource.<EventEnvelope>builder()
            .setBootstrapServers(brokers)
            .setTopics(pattern.sourceTopics())
            .setGroupId(pattern.consumerGroup())
            .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
            .setValueOnlyDeserializer(new EventEnvelopeSchema())
            .build();
    }

    public static SingleOutputStreamOperator<EventEnvelope> eventTime(DataStream<EventEnvelope> stream) {
        return stream
            .map(event -> {
                eventTimestamp(event);
                return event;
            })
            .name("validate-event-timestamp")
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<EventEnvelope>forBoundedOutOfOrderness(MAX_OUT_OF_ORDERNESS)
                    .withTimestampAssigner((event, previousTimestamp) -> eventTimestamp(event)));
    }

    public static KafkaSink<CepAlert> alertSink(String brokers) {
        return KafkaSink.<CepAlert>builder()
            .setBootstrapServers(brokers)
            .setRecordSerializer(new CepAlertSchema("flink.cep.alerts"))
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
    }

    public static long eventTimestamp(EventEnvelope event) {
        if (event == null || event.getTimestamp() == null) {
            throw new IllegalArgumentException("event timestamp is required");
        }
        try {
            return Instant.parse(event.getTimestamp()).toEpochMilli();
        } catch (DateTimeParseException error) {
            throw new IllegalArgumentException("invalid event timestamp: " + event.getTimestamp(), error);
        }
    }
}
