package dev.kafka;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Avro-продюсер через Schema Registry (Модуль 7).
 * Аргумент: v2 — схема с полем currency (эволюция).
 * Пишет GenericRecord в топик orders-avro.
 */
public class AvroProducer {

    private static final String SCHEMA_V1 = "{"
            + "\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"dev.kafka\","
            + "\"fields\":["
            + "{\"name\":\"orderId\",\"type\":\"string\"},"
            + "{\"name\":\"customerId\",\"type\":\"string\"},"
            + "{\"name\":\"amount\",\"type\":\"double\"},"
            + "{\"name\":\"status\",\"type\":\"string\"}"
            + "]}";

    private static final String SCHEMA_V2 = "{"
            + "\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"dev.kafka\","
            + "\"fields\":["
            + "{\"name\":\"orderId\",\"type\":\"string\"},"
            + "{\"name\":\"customerId\",\"type\":\"string\"},"
            + "{\"name\":\"amount\",\"type\":\"double\"},"
            + "{\"name\":\"status\",\"type\":\"string\"},"
            + "{\"name\":\"currency\",\"type\":\"string\",\"default\":\"USD\"}"
            + "]}";

    public static void main(String[] args) {
        boolean v2 = args.length > 0 && "v2".equals(args[0]);
        String schemaJson = v2 ? SCHEMA_V2 : SCHEMA_V1;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", "http://localhost:8081");

        Schema schema = new Schema.Parser().parse(schemaJson);

        try (KafkaProducer<String, Object> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= 5; i++) {
                GenericRecord record = new GenericData.Record(schema);
                record.put("orderId", "a" + (1000 + i));
                record.put("customerId", "c" + (i % 3 + 1));
                record.put("amount", i * 10.5);
                record.put("status", i % 2 == 0 ? "PAID" : "NEW");
                if (v2) {
                    record.put("currency", "USD");
                }

                String orderId = record.get("orderId").toString();
                ProducerRecord<String, Object> pr =
                        new ProducerRecord<>("orders-avro", orderId, record);
                producer.send(pr, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.printf("sent orderId=%s partition=%d offset=%d%n",
                                orderId, metadata.partition(), metadata.offset());
                    } else {
                        System.err.println("failed: " + exception.getMessage());
                    }
                });
            }
            producer.flush();
        }
    }
}
