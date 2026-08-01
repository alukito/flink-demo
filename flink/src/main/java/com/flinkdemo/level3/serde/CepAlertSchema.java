package com.flinkdemo.level3.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flinkdemo.level3.model.CepAlert;
import java.nio.charset.StandardCharsets;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

public final class CepAlertSchema implements KafkaRecordSerializationSchema<CepAlert> {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final String topic;

    public CepAlertSchema(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(CepAlert alert, KafkaSinkContext context, Long timestamp) {
        try {
            return new ProducerRecord<>(
                topic,
                alert.getAlertId().getBytes(StandardCharsets.UTF_8),
                MAPPER.writeValueAsBytes(alert));
        } catch (JsonProcessingException error) {
            throw new IllegalArgumentException("cannot serialize CEP alert", error);
        }
    }
}
