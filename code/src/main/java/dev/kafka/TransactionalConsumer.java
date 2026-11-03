package dev.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Транзакционный консюмер (Модуль 5).
 *
 * ЧТО: читает топик tx-orders-out в двух режимах изоляции и показывает разницу
 *      в видимости откаченных транзакций.
 *
 * ЗАЧЕМ: параметр isolation.level управляет тем, ЧТО видит консюмер:
 *   - read_committed   — только закоммиченные транзакции (откаченные скрыты);
 *   - read_uncommitted — всё, включая сообщения, которые потом откатят.
 * Это ключ к exactly-once: консюмер с read_committed не прочитает «мусор»
 * откаченных транзакций.
 *
 * КАК: аргумент read_committed|read_uncommitted. Группа уникальна на режим
 *      (tx-reader-<режим>), чтобы оба прогона читали с начала независимо.
 */
public class TransactionalConsumer {

    public static void main(String[] args) {
        String isolation = args.length > 0 ? args[0] : "read_committed";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092,localhost:9093,localhost:9094");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "tx-reader-" + isolation);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Вот она — ключевая настройка транзакционного чтения.
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolation);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("tx-orders-out"));

            // Читаем, пока не будет три пустых poll подряд (данные закончились).
            int emptyPolls = 0;
            while (emptyPolls < 3) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if (records.isEmpty()) {
                    emptyPolls++;
                    continue;
                }
                emptyPolls = 0;
                for (ConsumerRecord<String, String> r : records) {
                    System.out.printf("[%s] offset=%d key=%s value=%s%n",
                            isolation, r.offset(), r.key(), r.value());
                }
            }
        }
    }
}
