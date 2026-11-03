# Модуль 2. Kafka Producer API

> 2 ак.ч · ~1 час теория + ~1 час live coding

## Цели модуля

- Разобрать структуру `ProducerRecord` и её параметры.
- Написать пользовательский класс партицирования (Partitioner).
- Понять, как работает timestamp у сообщений.
- Освоить ключевые параметры конфигурации продюсера.
- Создать `KafkaProducer`, отправить сообщения (sync/async) и обработать `Callback`.

---

## 1. ProducerRecord: структура и параметры

`ProducerRecord` — это «заявка» на запись одного сообщения. Конструктор:

```java
new ProducerRecord<>(
    String topic,           // куда пишем (обязательно)
    Integer partition,      // явная партиция (null = по партиционеру)
    Long timestamp,         // время сообщения (null = время создания)
    K key,                  // ключ (null = нет ключа)
    V value,                // значение (обязательно)
    Iterable<Header> headers // произвольные заголовки
);
```

Типичные варианты:
```java
new ProducerRecord<>("orders", orderId, orderJson);                    // topic + key + value
new ProducerRecord<>("orders", 1, orderId, orderJson);                 // + явная партиция
new ProducerRecord<>("orders", null, ts, orderId, orderJson, headers); // полный набор
```

Куда попадёт сообщение:
- задан `partition` → строго в неё;
- иначе задан `key` → `partition = hash(key) % numPartitions` (по умолчанию
  `DefaultPartitioner` использует murmur2);
- нет ни `partition`, ни `key` → sticky-распределение по батчам.

**Почему важен ключ:** сообщения с одним ключом идут в одну партицию, а значит
сохраняют порядок и могут обрабатываться одним консюмером.

---

## 2. Пользовательский класс партицирования

Чтобы управлять распределением, реализуем интерфейс `Partitioner`:

```java
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import java.util.Map;

public class OrderPartitioner implements Partitioner {

    @Override
    public void configure(Map<String, ?> configs) {
        // сюда приходит конфигурация продюсера (можно читать свои параметры)
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {
        int n = cluster.partitionCountForTopic(topic); // число партиций топика
        if (key == null) {
            return 0; // без ключа — всегда в партицию 0
        }
        // Пример: направляем заказы с чётным hash в чётные партиции
        return Math.abs(key.hashCode()) % n;
    }

    @Override
    public void close() {}
}
```

Подключаем партиционер в конфигурации:

```java
props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, OrderPartitioner.class.getName());
```

Реалистичный сценарий кастомного партиционера: маршрутизация по стране
заказчика (чтобы данные ЕС и РФ физически разделялись), выделение «горячего»
клиента в отдельную партицию, привязка к партиции с учётом совместимости схемы.

> Внутри `configure()` можно получить число партиций или внешние параметры.
> Важно: `cluster.partitionCountForTopic(topic)` возвращает актуальное число
> партиций, а не захардкоженное.

---

## 3. Timestamp сообщений

Каждое сообщение имеет timestamp. Тип задаётся на брокере
(`log.message.timestamp.type`):

| Тип | Поведение |
|---|---|
| `CreateTime` (дефолт) | берётся timestamp, заданный продюсером (или время отправки) |
| `LogAppendTime` | брокер перезаписывает своим временем при записи |

Зачем: временны́е окна в Kafka Streams, retention по времени, дедупликация,
отладка порядка событий. Управление со стороны продюсера:

```java
long ts = System.currentTimeMillis();
new ProducerRecord<>("orders", null, ts, orderId, orderJson);
```

---

## 4. Producer API configuration

Ключевые параметры (`ProducerConfig`):

| Параметр | Дефолт | Назначение |
|---|---|---|
| `bootstrap.servers` | — | список брокеров для подключения |
| `key.serializer` / `value.serializer` | — | сериализаторы ключа/значения |
| `acks` | `all` | сколько реплик подтверждает запись: `0`/`1`/`all` |
| `enable.idempotence` | `true` (3.x) | идемпотентность (см. Модуль 5) |
| `retries` | `2147483647` | повторные попытки при ошибках |
| `delivery.timeout.ms` | `120000` | общий лимит доставки (с retries) |
| `linger.ms` | `0` | сколько ждать накопления батча |
| `batch.size` | `16384` | размер батча в байтах |
| `compression.type` | `none` | `gzip`/`snappy`/`lz4`/`zstd` |
| `max.in.flight.requests.per.connection` | `5` | сколько неподтверждённых запросов |
| `partitioner.class` | `DefaultPartitioner` | класс партиционирования |
| `client.id` | `producer-N` | имя клиента в метриках/логах |

Смысл `acks`:

- `acks=0` — продюсер не ждёт подтверждения (быстро, возможна потеря);
- `acks=1` — подтверждает только лидер (возможна потеря при падении лидера);
- `acks=all` — подтверждают все реплики из ISR (максимальная надёжность).

Несколько связок «почему это важно»:

- **`retries` + `delivery.timeout.ms`** — `retries` задаёт число повторов, а
  `delivery.timeout.ms` — общий бюджет на доставку одного сообщения (включая
  все ретраи). Исчерпали бюджет — продюсер вернёт ошибку в `Callback`.
- **`linger.ms` + `batch.size`** — сколько ждать и сколько копить в батче. Больше
  значения → выше throughput, но выше latency. Для пропускной способности ставят
  `linger.ms=5..100`, `batch.size` — десятки КБ.
- **`max.in.flight.requests.per.connection`** — сколько запросов «в полёте» без
  подтверждения. При `>1` сообщения могут переупорядочиться при ретраях; `=1`
  даёт строгий порядок ценой производительности. С `enable.idempotence=true`
  порядок сохраняется и при `≤5`.

---

## 5. Класс KafkaProducer: создание, отправка, Callback

### Создание

```java
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

Properties props = new Properties();
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
props.put(ProducerConfig.ACKS_CONFIG, "all");

KafkaProducer<String, String> producer = new KafkaProducer<>(props);
```

`KafkaProducer` потокобезопасен — один экземпляр на всё приложение. Он должен
закрываться через `producer.close()` (сбрасывает буферы и завершает в-flight
запросы).

### Синхронная отправка (`.get()`)

```java
ProducerRecord<String, String> record =
    new ProducerRecord<>("orders", orderId, orderJson);
try {
    RecordMetadata meta = producer.send(record).get();
    System.out.printf("sent: topic=%s partition=%d offset=%d ts=%d%n",
        meta.topic(), meta.partition(), meta.offset(), meta.timestamp());
} catch (Exception e) {
    e.printStackTrace();
}
```

`.get()` блокирует поток до подтверждения — просто для обучения/скриптов, в
продакшене обычно используют асинхронную отправку.

### Асинхронная отправка с Callback

```java
producer.send(record, new Callback() {
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            System.out.printf("ok: partition=%d offset=%d%n",
                metadata.partition(), metadata.offset());
        } else {
            System.err.println("failed: " + exception.getMessage());
        }
    }
});
```

`Callback` выполняется в **I/O-потоке продюсера**, поэтому внутри него нельзя
делать долгие операции и блокирующие вызовы. Замер latency/порядок лучше
фиксировать и агрегировать вне callback.

### Завершение

```java
producer.flush();   // дождаться отправки всех накопленных сообщений
producer.close();   // flush + освобождение ресурсов
```

---

## Live coding: реализация Producer с различными конфигурациями

Запуск — готовым fat-JAR из `dist/` (без Maven). Исходники — в `code/src/main/java/dev/kafka/ProducerExample.java` и
`OrderPartitioner.java`.

### Шаг 1. Подготовка

Стенд уже поднят (Модуль 1). Создаём топик и собираем проект:

```bash
docker exec kafka1 kafka-topics --bootstrap-server localhost:29092 \
  --create --topic orders --partitions 4 --replication-factor 3

```

### Шаг 2. Базовый продюсер с Callback

`ProducerExample` отправляет N заказов и печатает партицию/офсет из callback.
Запуск:

```bash
java -jar dist/module02-producer/producer-example.jar
```

Пример вывода (порядок строк может отличаться):

```
ok: partition=1 offset=0 key=o1001
ok: partition=0 offset=0 key=o1002
ok: partition=2 offset=0 key=o1003
```

У разных ключей — разные `partition`; `offset` растёт внутри одной партиции.
Обратите внимание: сообщения с одним `orderId` (ключом) стабильно попадают в
одну партицию.

### Шаг 3. Эксперимент с acks и производительностью

Добавим в `ProducerExample` режим конфигураций. Сравним `acks=0` и `acks=all`,
замерив время отправки 10 000 сообщений:

```java
private static long run(String label, Properties props, int n) {
    try (KafkaProducer<String, String> p = new KafkaProducer<>(props)) {
        long t0 = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            p.send(new ProducerRecord<>("orders", "key-" + (i % 100), "v" + i));
        }
        p.flush();
        long dt = System.currentTimeMillis() - t0;
        System.out.printf("%-10s %5d ms (%d msg/s)%n", label, dt, n * 1000L / Math.max(dt, 1));
        return dt;
    }
}
```

Режим `bench` уже есть в `ProducerExample` — запускаем:

```bash
java -jar dist/module02-producer/producer-example.jar bench
```

Внутри `bench()` сравниваются два набора свойств и запускаются два прогона:

```java
// acks=0 — быстро, но без гарантии
Properties p0 = base(); p0.put(ProducerConfig.ACKS_CONFIG, "0");
// acks=all — надёжно
Properties pAll = base(); pAll.put(ProducerConfig.ACKS_CONFIG, "all");
run("acks=0", p0, 10_000);
run("acks=all", pAll, 10_000);
```

### Шаг 4. Пользовательский партиционер

Режим `partitioner` тоже есть в `ProducerExample` — запускаем:

```bash
java -jar dist/module02-producer/producer-example.jar partitioner
```

Внутри он подключает `OrderPartitioner`:

```java
props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, OrderPartitioner.class.getName());
```

Проверяем в Kafdrop или через CLI, что все сообщения без ключа уходят в
партицию 0, а сообщения с ключами распределяются по нашему правилу:

```bash
docker exec kafka1 kafka-get-offsets --bootstrap-server localhost:29092 --topic orders
```

### Шаг 5. Timestamp

Отправим сообщение с явным timestamp и проверим его:

```java
long ts = System.currentTimeMillis() - 60_000; // минута назад
producer.send(new ProducerRecord<>("orders", null, ts, "old-order", "{\"status\":\"OLD\"}")).get();
```

Прочитаем с меткой времени:

```bash
docker exec kafka1 kafka-console-consumer \
  --bootstrap-server localhost:29092 --topic orders --from-beginning \
  --property print.timestamp=true --property print.key=true
```

### Шаг 6. Очистка

```bash
docker exec kafka1 kafka-topics --bootstrap-server localhost:29092 --delete --topic orders
```

---

## Итоги модуля

- `ProducerRecord` = topic + partition + timestamp + key + value + headers.
- Партиция выбирается по ключу (`murmur2`), либо кастомным `Partitioner`.
- `acks=0/1/all` задают баланс скорость/надёжность.
- `KafkaProducer` создаётся один раз, отправка — sync (`.get()`) или async
  (`Callback`); завершение — `flush()`/`close()`.
