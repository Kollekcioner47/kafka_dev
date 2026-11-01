package dev.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Транзакционный продюсер (Модуль 5).
 * Аргумент: commit (по умолчанию) | abort.
 * Атомарно пишет в tx-orders-out и tx-audit.
 */
public class TransactionalProducer {

    public static void main(String[] args) {
        boolean abort = args.length > 0 && "abort".equals(args[0]);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx-orders"); // обязательный id

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.initTransactions();

            String orderId = "tx-" + System.currentTimeMillis();
            try {
                producer.beginTransaction();
                producer.send(new ProducerRecord<>("tx-orders-out", orderId,
                        "{\"orderId\":\"" + orderId + "\"}"));
                producer.send(new ProducerRecord<>("tx-audit", orderId,
                        "AUDIT:" + orderId));

                if (abort) {
                    System.out.println("aborting transaction " + orderId);
                    producer.abortTransaction();
                } else {
                    producer.commitTransaction();
                    System.out.println("committed " + orderId);
                }
            } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
                // Фатально: продюсер «вытеснен» или последовательность сломана — продолжать нельзя.
                System.err.println("fatal: " + e.getMessage());
            } catch (KafkaException e) {
                producer.abortTransaction();
                System.err.println("aborted due to: " + e.getMessage());
            }
        }
    }
}
