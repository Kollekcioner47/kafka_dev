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
 *
 * ЧТО: пишет заказы в orders-avro в формате Avro. Значение — GenericRecord
 *      (структура из схемы), ключ — обычная строка.
 *
 * ЗАЧЕМ: Avro — компактный бинарный формат со схемой. Schema Registry хранит
 *      схемы и следит за их совместимостью. KafkaAvroSerializer регистрирует
 *      схему в Registry и пишет в сообщение только её ID (не всю схему) —
 *      экономия места и возможность менять схему без переписывания данных.
 *
 * КАК: аргумент v2 выбирает схему V2 (добавлено поле currency с дефолтом) —
 *      демонстрация эволюции схемы: старый консюмер без этого поля всё равно
 *      прочитает новые сообщения.
 */
public class AvroProducer {

    // Схема V1: базовый заказ. Avro-схема — это JSON, описывающий поля и типы.
    private static final String SCHEMA_V1 = "{"
            + "\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"dev.kafka\","
            + "\"fields\":["
            + "{\"name\":\"orderId\",\"type\":\"string\"},"
            + "{\"name\":\"customerId\",\"type\":\"string\"},"
            + "{\"name\":\"amount\",\"type\":\"double\"},"
            + "{\"name\":\"status\",\"type\":\"string\"}"
            + "]}";

    // Схема V2: добавлено поле currency с default="USD" — это BACKWARD-совместимо:
    // старый консюмер (по V1) читает новые данные, не зная о currency.
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
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092,localhost:9093,localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Значение сериализуем Avro-сериализатором Confluent, а не строкой.
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        // Куда ходить за схемами (сервис Schema Registry из docker-compose).
        props.put("schema.registry.url", "http://localhost:8081");

        // Парсим схему из JSON-строки в объект Avro Schema.
        Schema schema = new Schema.Parser().parse(schemaJson);

        // Значение — Object, потому что это GenericRecord (не кодогенерированный класс).
        try (KafkaProducer<String, Object> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= 5; i++) {
                // GenericRecord: данные кладём по ИМЕНИ поля из схемы — без
                // кодогенерации (в отличие от SpecificRecord).
                GenericRecord record = new GenericData.Record(schema);
                record.put("orderId", "a" + (1000 + i));
                record.put("customerId", "c" + (i % 3 + 1));
                record.put("amount", i * 10.5);
                record.put("status", i % 2 == 0 ? "PAID" : "NEW");
                if (v2) {
                    record.put("currency", "USD"); // поле есть только в V2
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
