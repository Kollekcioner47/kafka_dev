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
 *
 * ЧТО: атомарно пишет ОДНО И ТО ЖЕ сообщение в два топика (tx-orders-out и
 *      tx-audit): либо оба появятся, либо ни одного.
 *
 * ЗАЧЕМ: обычный продюсер пишет в каждый топик независимо — если запись во
 *      второй топик упадёт, в первом останется «половинчатый» результат.
 *      Транзакция даёт семантику «всё или ничего» на наборе записей.
 *
 * КАК: initTransactions() → beginTransaction() → send(...) × N →
 *      commitTransaction() ИЛИ abortTransaction(). Аргумент commit|abort
 *      выбирает сценарий (см. сравнение read_committed vs read_uncommitted).
 */
public class TransactionalProducer {

    public static void main(String[] args) {
        boolean abort = args.length > 0 && "abort".equals(args[0]);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092,localhost:9093,localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // transactional.id — стабильный уникальный id транзакционного продюсера.
        // Он переживает рестарты и защищает от «зомби»: если старый экземпляр
        // продюсера «воскреснет» после того, как новый взял тот же id, брокер
        // вытеснит старого (ProducerFencedException). Без него транзакции не включатся.
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx-orders");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            // Регистрирует транзакционного продюсера у брокера (один раз на экземпляр).
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
                    producer.abortTransaction(); // откат: оба сообщения не видны read_committed
                } else {
                    producer.commitTransaction(); // атомарно публикуем оба сообщения
                    System.out.println("committed " + orderId);
                }
            } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
                // Эти исключения ФАТАЛЬНЫ: продюсер «вытеснен» зомби-защитой,
                // сломана последовательность или нет прав. Продолжать нельзя —
                // единственный корректный выход — закрыть продюсер.
                System.err.println("fatal: " + e.getMessage());
            } catch (KafkaException e) {
                // Обычная ошибка (сеть, таймаут и т.п.): можно откатить и попробовать позже.
                producer.abortTransaction();
                System.err.println("aborted due to: " + e.getMessage());
            }
        }
    }
}
