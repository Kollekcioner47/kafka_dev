package dev.kafka;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Avro-консюмер через Schema Registry (Модуль 7).
 * Читает orders-avro как GenericRecord.
 */
public class AvroConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "avro-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("schema.registry.url", "http://localhost:8081");

        try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("orders-avro"));

            while (true) {
                ConsumerRecords<String, GenericRecord> records =
                        consumer.poll(Duration.ofMillis(1000));
                if (records.isEmpty()) {
                    break;
                }
                for (ConsumerRecord<String, GenericRecord> r : records) {
                    GenericRecord v = r.value();
                    System.out.printf("orderId=%s customerId=%s amount=%s status=%s%n",
                            v.get("orderId"), v.get("customerId"), v.get("amount"), v.get("status"));
                }
            }
        }
    }
}
