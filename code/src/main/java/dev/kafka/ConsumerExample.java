package dev.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

/**
 * Консюмер (Модуль 3).
 *
 * ЧТО: минимальный консюмер с РУЧНЫМ коммитом offset и обработкой rebalance
 *      через ConsumerRebalanceListener.
 *
 * ЗАЧЕМ: показывает базовый цикл consume: subscribe → poll → обработать →
 *        commitSync. Ручной коммит (вместо автокоммита) даёт контроль над тем,
 *        КОГДА фиксировать прогресс: коммитим только ПОСЛЕ успешной обработки,
 *        иначе при сбое сообщение потеряется (at-most-once) или, наоборот,
 *        перечитается (at-least-once).
 *
 * КАК: аргумент earliest|latest задаёт auto.offset.reset — откуда читать новой
 *      группе, у которой ещё нет закоммиченных офсетов.
 */
public class ConsumerExample {

    public static void main(String[] args) {
        String reset = args.length > 0 ? args[0] : "earliest";

        Properties props = new Properties();
        // Три брокера (bootstrap перечисляет все три на случай отказа одного).
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092,localhost:9093,localhost:9094");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "orders-group");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // ручной коммит
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, reset);
        // Кооперативная стратегия: rebalance перебрасывает партиции порциями,
        // а не «стоп-мир» целиком.
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");

        // KafkaConsumer НЕ потокобезопасен: один экземпляр — один поток.
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // Rebalance-колбэки: вызываются, когда партиции забирают/назначают.
            consumer.subscribe(List.of("orders"), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    System.out.println("Revoked: " + partitions);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    System.out.println("Assigned: " + partitions);
                }
            });

            while (true) {
                // poll() — «сердце» консюмера: тянет пачку сообщений (а также
                // обрабатывает heartbeat и rebalance в фоне).
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if (records.isEmpty()) {
                    break; // для демонстрации: выходим, когда данных больше нет
                }
                for (ConsumerRecord<String, String> r : records) {
                    System.out.printf("partition=%d offset=%d key=%s value=%s%n",
                            r.partition(), r.offset(), r.key(), r.value());
                }
                consumer.commitSync(); // фиксируем обработанное ПОСЛЕ обработки
            }
        }
    }
}
