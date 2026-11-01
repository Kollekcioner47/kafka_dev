package dev.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * НАГЛЯДНЫЙ консюмер (Модуль 3).
 *
 * Печатает, ЧТО ПРОИСХОДИТ на каждом шаге:
 *   1) какие партиции назначены этому консюмеру (rebalance);
 *   2) какие сообщения читаются (partition / offset / key / value);
 *   3) момент коммита offset;
 *   4) таблицу из трёх ключевых чисел по каждой партиции:
 *        position   — до куда дочитали в этом сеансе (растёт при poll);
 *        committed  — что сохранено для рестарта (растёт при commitSync);
 *        end / lag  — сколько всего сообщений и сколько ещё не прочитано.
 *
 * `max.poll.records=5` + пауза между poll() делают рост чисел видимым.
 *
 * Запуск (аргументы: метка консюмера, earliest|latest, имя группы):
 *   mvn exec:java -Dexec.mainClass=dev.kafka.ConsumerVisualizer -Dexec.args="C1 earliest"
 *   mvn exec:java -Dexec.mainClass=dev.kafka.ConsumerVisualizer -Dexec.args="C2 earliest orders-group"
 */
public class ConsumerVisualizer {

    public static void main(String[] args) {
        String label = args.length > 0 ? args[0] : "C1";
        String reset = args.length > 1 ? args[1] : "earliest";
        String group = args.length > 2 ? args[2] : "orders-group";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, label);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // ручной коммит
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, reset);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5");       // маленькие порции
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("orders"), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    banner(label, "REBALANCE: у меня ЗАБИРАЮТ партиции " + partitions);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    banner(label, "REBALANCE: мне НАЗНАЧЕНЫ партиции " + partitions);
                }
            });

            boolean firstPoll = true;
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                if (!records.isEmpty()) {
                    System.out.println("[" + label + "] poll вернул " + records.count()
                            + " сообщ. — обрабатываю...");
                    for (ConsumerRecord<String, String> r : records) {
                        System.out.printf("    [%s] partition=%d offset=%-4d key=%-6s value=%s%n",
                                label, r.partition(), r.offset(), r.key(), r.value());
                    }
                    // Обработка успешна -> фиксируем прочитанное.
                    consumer.commitSync();
                    System.out.println("    [" + label + "] commitSync() — committed догнал position");
                }

                if (firstPoll || !records.isEmpty()) {
                    printOffsetTable(consumer, label);
                    firstPoll = false;
                }

                try {
                    Thread.sleep(400);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    /** Три числа по каждой партиции: position / committed / end (+ lag). */
    private static void printOffsetTable(KafkaConsumer<String, String> consumer, String label) {
        Set<TopicPartition> assignment = consumer.assignment();
        if (assignment.isEmpty()) {
            return;
        }
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignment);
        Map<TopicPartition, OffsetAndMetadata> committedMap = consumer.committed(assignment);

        System.out.println("  ---- [" + label + "] состояние партиций ----");
        System.out.printf("    %-14s %-10s %-11s %-10s %-6s%n",
                "partition", "position", "committed", "end", "lag");
        for (TopicPartition tp : assignment) {
            long position = consumer.position(tp);
            OffsetAndMetadata committed = committedMap.get(tp);
            String committedStr = (committed == null || committed.offset() < 0)
                    ? "(нет)" : String.valueOf(committed.offset());
            long end = endOffsets.getOrDefault(tp, -1L);
            System.out.printf("    %-14s %-10d %-11s %-10d %-6d%n",
                    tp, position, committedStr, end, end - position);
        }
        System.out.println();
    }

    private static void banner(String label, String text) {
        System.out.println();
        System.out.println("================================================");
        System.out.println("  [" + label + "] " + text);
        System.out.println("================================================");
    }
}
