package com.flinkdemo.level2.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flinkdemo.level2.model.WindowStat;
import java.nio.charset.StandardCharsets;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

public final class WindowStatSchema implements KafkaRecordSerializationSchema<WindowStat> {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final String topic;
    public WindowStatSchema(String topic) { this.topic = topic; }
    @Override public ProducerRecord<byte[], byte[]> serialize(WindowStat stat, KafkaSinkContext context, Long timestamp) {
        try {
            byte[] key = (stat.getMetric() + ":" + stat.getScope()).getBytes(StandardCharsets.UTF_8);
            return new ProducerRecord<>(topic, key, MAPPER.writeValueAsBytes(stat));
        } catch (JsonProcessingException error) {
            throw new IllegalArgumentException("cannot serialize window stat", error);
        }
    }
}
