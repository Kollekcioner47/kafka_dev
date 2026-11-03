# Модуль 5. Kafka Transactions

> 2 ак.ч · ~1 час теория + ~1 час live coding

## Цели модуля

- Разобрать уровни гарантий доставки и где что теряется/дублируется.
- Понять, как работает Idempotent Producer.
- Разобрать exactly-once семантику (EOS) и роль транзакций.
- Написать транзакционный Producer и Consumer с `read_committed`.

---

## 1. Гарантии в Kafka

| Уровень | Потери | Дубли | Как достигается |
|---|---|---|---|
| **at-most-once** | возможны | нет | `acks=0`, авто-коммит ДО обработки |
| **at-least-once** | нет | возможны | `acks=all` + коммит ПОСЛЕ обработки |
| **exactly-once** | нет | нет | идемпотентный продюсер + транзакции |

Откуда берутся проблемы:

- **Дубли при записи**: продюсер отправил батч, брокер записал, но
  подтверждение потерялось → продюсер ретраит → то же сообщение записано дважды.
- **Дубли при чтении**: консюмер обработал сообщение, но упал до коммита offset →
  после рестарта читает его снова (at-least-once).
- **Потери при чтении**: консюмер закоммитил offset ДО обработки, обработка упала
  → сообщение «проглочено» (at-most-once).

Exactly-once требует устранения дублей на обеих сторонах.

---

## 2. Idempotent Producer

**Идемпотентный продюсер** устраняет дубли при записи. Включается параметром
`enable.idempotence=true` (в Kafka 3.x — дефолт).

Как работает:
1. Брокер выдаёт продюсеру уникальный **PID** (producer id).
2. Каждое сообщение в партицию получает монотонный **sequence number**.
3. Брокер запоминает последний принятый sequence для `(PID, partition)`.
4. Если приходит сообщение с уже принятым sequence → оно отбрасывается как дубль.

Требования:
- `acks=all` (иначе идемпотентность не включается);
- `max.in.flight.requests.per.connection ≤ 5` (порядок последовательностей);
- `retries > 0`.

Ограничение: идемпотентность действует в рамках **одного сеанса продюсера**
(PID). При перезапуске PID меняется, и защита от дублей «через рестарт» не
работает — для этого нужны транзакции.

```java
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
props.put(ProducerConfig.ACKS_CONFIG, "all");
```

---

## 3. Exactly Once семантика (EOS)

Exactly-once в Kafka — это **read-process-write** атомарность: прочитать
сообщение из входного топика, обработать, записать результат — и сделать это
ровно один раз, даже при сбоях. Достигается связкой:

- идемпотентный продюсер (нет дублей записи);
- **транзакции** (атомарность набора записей в 1..N партиций/топиков);
- **read_committed** консюмер (видит только закоммиченные транзакции).

Где используется EOS:
- Kafka Streams (`processing.guarantee=exactly_once_v2`) — Модуль 6;
- Kafka Connect (для интеграций с внешними системами);
- вручную — транзакционный Producer/Consumer (ниже).

---

## 4. Транзакции в Kafka

Транзакция объединяет несколько записей в атомарную единицу: либо все видны,
либо ни одна.

### Транзакционный Producer

Обязательные параметры:
- `transactional.id` — стабильный уникальный id транзакционного продюсера
  (переживает рестарты; гарантирует, что «зомби» не завершит чужую транзакцию);
- `enable.idempotence=true`, `acks=all` — включаются автоматически.

```java
import org.apache.kafka.clients.producer.*;

Properties props = new Properties();
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx-orders"); // обязательный!

KafkaProducer<String, String> producer = new KafkaProducer<>(props);
producer.initTransactions();   // регистрация транзакционного продюсера

try {
    producer.beginTransaction();
    producer.send(new ProducerRecord<>("tx-orders-out", orderId, orderJson));
    producer.send(new ProducerRecord<>("tx-audit", orderId, "AUDIT:" + orderId));
    producer.commitTransaction();      // атомарно публикуем обе записи
} catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
    // нельзя продолжать — продюсер «вытеснен» или последовательность сломана
    producer.close();
} catch (KafkaException e) {
    producer.abortTransaction();      // откат
}
```

Ключевые методы: `initTransactions()`, `beginTransaction()`, `send()`,
`commitTransaction()`, `abortTransaction()`, `sendOffsetsToTransaction()`
(коммит офсетов консюмера в той же транзакции — для read-process-write).

### Транзакционный Consumer (read_committed)

```java
props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
```

`read_committed` скрывает сообщения незавершённых/откаченных транзакций и
читает только до **Last Stable Offset (LSO)**. Дефолт `read_uncommitted` видит
всё, включая сообщения, которые потом будут откачены.

### Как брокер хранит транзакции

- внутренний топик `__transaction_state` (replication factor задаётся на
  брокере — в нашем compose = 3);
- маркеры-контролы (commit/abort markers) в партициях;
- при `abortTransaction` записи помечаются и не читаются `read_committed`.

---

## Live coding: реализация транзакционного Producer и Consumer

Запуск — готовым fat-JAR из `dist/` (без Maven). Исходники — в `code/src/main/java/dev/kafka/TransactionalProducer.java` и
`TransactionalConsumer.java`.

### Шаг 1. Топики

```bash
docker exec kafka1 kafka-topics --bootstrap-server localhost:29092 \
  --create --topic tx-orders-out --partitions 3 --replication-factor 3
docker exec kafka1 kafka-topics --bootstrap-server localhost:29092 \
  --create --topic tx-audit --partitions 3 --replication-factor 3
```

### Шаг 2. Успешная транзакция

```bash
java -jar dist/module05-transactions/transactional-producer.jar commit
```

Продюсер запишет `orderId` в **оба** топика (`tx-orders-out` и `tx-audit`)
атомарно. Проверяем:

```bash
docker exec kafka1 kafka-console-consumer --bootstrap-server localhost:29092 \
  --topic tx-orders-out --from-beginning
docker exec kafka1 kafka-console-consumer --bootstrap-server localhost:29092 \
  --topic tx-audit --from-beginning
```

### Шаг 3. Откат транзакции

```bash
java -jar dist/module05-transactions/transactional-producer.jar abort
```

Продюсер пишет сообщения и вызывает `abortTransaction()`. Теперь сравним два
режима чтения:

```bash
# read_uncommitted — видит и откаченные записи
java -jar dist/module05-transactions/transactional-consumer.jar read_uncommitted

# read_committed — откаченные записи скрыты
java -jar dist/module05-transactions/transactional-consumer.jar read_committed
```

`read_committed` покажет только сообщения из **успешной** транзакции (Шаг 2),
а записи откаченной транзакции не появятся.

### Шаг 4. Атомарность в действии

Проверьте: в Шаге 2 сообщение появилось в обоих топиках одновременно. Если бы
запись во второй топик упала, `abortTransaction()` откатил бы и первый.
Это и есть «все или ничего».

### Шаг 5. Очистка

```bash
docker exec kafka1 kafka-topics --bootstrap-server localhost:29092 --delete --topic tx-orders-out
docker exec kafka1 kafka-topics --bootstrap-server localhost:29092 --delete --topic tx-audit
```

---

## Итоги модуля

- Гарантии: at-most-once / at-least-once / exactly-once.
- Idempotent Producer убирает дубли записи через PID + sequence numbers.
- Транзакции дают атомарность набора записей и exactly-once в связке с
  `read_committed`.
- `transactional.id` + `initTransactions()` — обязательная пара для
  транзакционного продюсера; `read_committed` — для чтения только
  закоммиченных данных.
