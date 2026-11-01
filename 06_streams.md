# Модуль 6. Kafka Streams

> 2 ак.ч · ~1 час теория + ~1 час live coding

## Цели модуля

- Понять модель Kafka Streams: топология, KStream, KTable, Serde.
- Разобраться в хранении состояния (state store, changelog).
- Научиться объединять потоки данных (join).
- Освоить KTable и GlobalKTable.
- Познакомиться с Processor API.
- Написать приложение потоковой обработки.

---

## 1. Основы Kafka Streams

**Kafka Streams** — клиентская библиотека (JVM) для потоковой обработки поверх
Kafka. Не требует отдельного кластера: приложение — обычный Java-процесс,
читающий из входных топиков и пишущий в выходные.

Ключевые понятия:
- **Topology** — граф обработки: `source → processor → sink`.
- **KStream** — поток записей (append-only), каждая запись независима.
- **KTable** — таблица «последнего значения по ключу» (changelog), обновляется.
- **Serde** — пара serializer/deserializer для ключей и значений.
- **State store** — локальное состояние (RocksDB по умолчанию).

```java
Properties props = new Properties();
props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-streams-app");
props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
```

Минимальная топология:

```java
StreamsBuilder builder = new StreamsBuilder();
KStream<String, String> orders = builder.stream("orders");
orders.filter((k, v) -> v.contains("PAID"))
      .to("orders-paid");

KafkaStreams streams = new KafkaStreams(builder.build(), props);
streams.start();
Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
```

---

## 2. Хранение состояния

**Stateful**-операции (`count`, `aggregate`, `reduce`, `join` с окнами) требуют
состояния. Kafka Streams хранит его:

1. **Локально** — в `state store` (RocksDB) на диске каждого инстанса;
2. **В changelog-топике** — брокер хранит копию состояния, чтобы при
   перебалансировке/рестарте состояние восстановилось.

Параметры:
- `state.dir` — каталог локального состояния;
- `StreamsConfig.REPLICATION_FACTOR_CONFIG` — RF внутренних/changelog топиков
  (в dev-стенде с 3 брокерами = 3);
- `num.stream.threads` — потоки обработки на инстанс.

Пример stateful-агрегации (сумма по ключу):

```java
KTable<String, Long> totals = orders
    .groupBy((k, v) -> v.customerId)          // перегруппировка по ключу
    .aggregate(() -> 0L,                       // инициализатор
               (customerId, order, agg) -> agg + order.amount, // агрегатор
               Materialized.as("customer-totals-store"));      // имя state store
totals.toStream().to("orders-agg", Produced.with(Serdes.String(), Serdes.Long()));
```

> При агрегации к `Long` value-серде state store должен быть `Serdes.Long()`
> (иначе дефолтный `String`-серде упадёт при сериализации). В полном примере
> `StreamsExample` это задано явно через `Materialized.withValueSerde(...)`.

### Окна (windowing)

Для агрегаций по времени:
- **tumbling** — фиксированные непересекающиеся окна (каждые 10 с);
- **hopping** — перекрывающиеся окна (окно 30 с, шаг 10 с);
- **session** — окна по активности (промежуток без событий закрывает сессию);
- **sliding** — скользящее окно для join.

```java
TimeWindows.of(Duration.ofSeconds(30))            // tumbling 30s
TimeWindows.of(Duration.ofSeconds(30)).advanceBy(Duration.ofSeconds(10)); // hopping
```

> Терминология: в Kafka Streams «скользящее» (sliding) окно — это отдельный
> механизм для join, а окно с перекрытием называется **hopping** (не путать).

---

## 3. Объединение данных (join)

| Join | Тип | Ключевая особенность |
|---|---|---|
| KStream–KStream | windowed | оба потока в пределах окна времени |
| KStream–KTable | table lookup | таблица по последнему значению ключа |
| KStream–GlobalKTable | global lookup | таблица реплицирована на ВСЕ инстансы |
| KTable–KTable | table join | совмещение двух changelog-таблиц |

```java
// KStream–KTable: обогащение заказа данными клиента
KStream<String, Order> orders = ...;
KTable<String, Customer> customers = builder.table("customers");
orders.join(customers, (order, customer) -> order.withCustomerName(customer.name()))
      .to("orders-enriched");
```

Правила:
- KStream–KStream требует **окно** (оба потока бесконечны);
- KStream–KTable обогащает по ключу значением таблицы на момент события;
- **GlobalKTable** не требует переразбиения по ключу (см. §4).

---

## 4. KTable и GlobalKTable

**KTable** — представление «последнее значение по ключу». Создаётся из топика
(`builder.table()`), из агрегации или из `groupBy().count()`. Хранит только
текущее состояние по ключу.

**GlobalKTable** — таблица, **полностью реплицируемая на каждый инстанс**
приложения. Создаётся `builder.globalTable()`. Отличия от KTable:

| | KTable | GlobalKTable |
|---|---|---|
| Данные | партиционированы | полная копия на каждом инстансе |
| Чтение при join | по партиции ключа | по любому ключу (lookup) |
| Rebalance | перераспределяется | не перераспределяется |
| Назначение | большие таблицы | маленькие справочники (валюты, страны) |

```java
GlobalKTable<String, String> currencies = builder.globalTable("currencies");

// KStream–GlobalKTable: ключ заказа ≠ ключ справочника — используем keyMapper
orders.leftJoin(currencies,
    (orderKey, order) -> order.currency(),   // какой ключ искать в справочнике
    (order, currency) -> order.withRate(currency));
```

---

## 5. Processor API

**Processor API** — низкоуровневая альтернатива DSL. Даёт полный контроль
(доступ к каждому сообщению, произвольное состояние, `forward()`,
планировщики `Punctuator`).

```java
Topology topology = new Topology();
topology.addSource("Source", "orders")
        .addProcessor("Process", () -> new Processor<String, String>() {
            private ProcessorContext context;
            private KeyValueStore<String, Long> store;

            @Override public void init(ProcessorContext context) {
                this.context = context;
                this.store = context.getStateStore("counts");
            }

            @Override public void process(String key, String value) {
                Long old = store.get(key);
                store.put(key, (old == null ? 0 : old) + 1);
                context.forward(key, store.get(key)); // отправить дальше
            }

            @Override public void close() {}
        }, "Source")
        .addStateStore(Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore("counts"), Serdes.String(), Serdes.Long()), "Process")
        .addSink("Sink", "orders-agg", "Process");
```

Когда нужен Processor API: нестандартная логика, доступ к `context.timestamp()`,
периодические задачи (`schedule`/`Punctuator`), ручное управление состоянием.

---

## Live coding: разработка приложения с Kafka Streams

Исходный код — в `code/src/main/java/dev/kafka/StreamsExample.java`.

Бизнес-кейс: из топика `orders` (JSON) считаем **сумму оплаченных заказов по
клиенту** и обогащаем результат именем клиента из справочника `customers`.

### Шаг 1. Входные данные

```bash
# справочник клиентов (ключ=customerId, значение=имя)
docker exec kafka1 kafka-topics --bootstrap-server localhost:29092 \
  --create --topic customers --partitions 3 --replication-factor 3
docker exec kafka1 kafka-topics --bootstrap-server localhost:29092 \
  --create --topic orders --partitions 4 --replication-factor 3

# наполняем справочник (console-producer, ключ:значение)
docker exec -it kafka1 kafka-console-producer \
  --bootstrap-server localhost:29092 --topic customers \
  --property "parse.key=true" --property "key.separator=:"
# c1:Alice
# c2:Bob
# c3:Carol
# (Ctrl+C)
```

Наполняем `orders` (ключ=orderId, значение=JSON):

```bash
docker exec -it kafka1 kafka-console-producer \
  --bootstrap-server localhost:29092 --topic orders \
  --property "parse.key=true" --property "key.separator=:"
# o1:{"customerId":"c1","amount":100,"status":"PAID"}
# o2:{"customerId":"c1","amount":50,"status":"NEW"}
# o3:{"customerId":"c2","amount":200,"status":"PAID"}
# o4:{"customerId":"c1","amount":30,"status":"PAID"}
# (Ctrl+C)
```

> Значение заказа — **JSON-строка**, а не объект: в `StreamsExample` её разбирает
> Jackson (`ObjectMapper`) через хелперы `field()`/`amount()`. В проде вместо
> «голого» JSON чаще берут Avro через Schema Registry (Модуль 7) — там и схема,
> и типы строже.

### Шаг 2. Топология

```bash
cd code && mvn -q compile
mvn -q exec:java -Dexec.mainClass=dev.kafka.StreamsExample
```

Топология делает:
1. читает `orders` (KStream);
2. `filter` — только `status == "PAID"`;
3. `groupBy(customerId)` + `aggregate` — сумма `amount` по клиенту (KTable);
4. `leftJoin` с GlobalKTable `customers` — добавляет имя клиента;
5. пишет результат `"Alice -> 130"` в топик `orders-agg`.

### Шаг 3. Результат

```bash
docker exec kafka1 kafka-console-consumer \
  --bootstrap-server localhost:29092 --topic orders-agg --from-beginning \
  --property print.key=true
```

Ожидаем (порядок не гарантирован между партициями):

```
c1  Alice -> 130
c2  Bob -> 200
```

Заметьте: заказ `o2` (NEW) отфильтрован, поэтому сумма `c1` = 100 + 30 = 130.

### Шаг 4. Обновление в реальном времени

Допишите ещё один оплаченный заказ для `c1` и понаблюдайте, как KTable выдаёт
новое значение:

```bash
docker exec -it kafka1 kafka-console-producer \
  --bootstrap-server localhost:29092 --topic orders \
  --property "parse.key=true" --property "key.separator=:"
# o5:{"customerId":"c1","amount":20,"status":"PAID"}
# (Ctrl+C)
```

В `orders-agg` появится `c1  Alice -> 150`.

### Шаг 5. State store и восстановление

Остановите приложение (Ctrl+C) и запустите снова — сумма не сбросится: состояние
восстанавливается из changelog-топика. Проверьте внутренние топики:

```bash
docker exec kafka1 kafka-topics --bootstrap-server localhost:29092 --list | grep orders-streams
```

### Шаг 6. Очистка

```bash
docker exec kafka1 kafka-topics --bootstrap-server localhost:29092 --delete --topic orders
docker exec kafka1 kafka-topics --bootstrap-server localhost:29092 --delete --topic customers
docker exec kafka1 kafka-topics --bootstrap-server localhost:29092 --delete --topic orders-agg
# внутренние топики приложения (changelog/repartition)
docker exec kafka1 kafka-topics --bootstrap-server localhost:29092 --list | grep orders-streams | xargs -I{} docker exec kafka1 kafka-topics --bootstrap-server localhost:29092 --delete --topic {}
```

---

## Итоги модуля

- Kafka Streams — клиентская библиотека; топология = source → processor → sink.
- KStream (поток) vs KTable (последнее значение) vs GlobalKTable (полная копия).
- Состояние хранится в RocksDB + changelog-топике; восстанавливается при рестарте.
- Join: KStream–KStream (оконный), KStream–KTable, KStream–GlobalKTable.
- Processor API — низкоуровневый контроль (state store, `forward`, `Punctuator`).
