# Модуль 9. Apache Flink (Flink SQL + Kafka)

> 2 ак.ч · ~1 час теория + ~1 час live coding

## Цели модуля

- Понять, какую задачу решает Flink рядом с Kafka и чем он отличается от Kafka
  Streams и ksqlDB.
- Разобрать модель Flink: DataStream API, Table API, Flink SQL.
- Подключить Kafka как source/sink через Kafka-коннектор.
- Написать Flink SQL: CREATE TABLE, потоковая фильтрация и агрегация.

---

## 1. Зачем Flink рядом с Kafka

Kafka — это **шина/лог**: хранит события и доставляет их. Саму логику обработки
(фильтры, агрегации, join, окна) делают поверх неё:

| Инструмент | Что это | Когда выбирать |
|---|---|---|
| **Kafka Streams** (Модуль 6) | библиотека внутри вашего JVM-приложения | логика в вашем сервисе, нет отдельного кластера |
| **ksqlDB** | SQL-движок от Confluent | ⚠️ фактически заморожен, не для новых проектов |
| **Apache Flink** | отдельный движок потоковой обработки + SQL | сложная stateful-обработка, SQL поверх Kafka, exactly-once |

**Почему не ksqlDB:** Confluent сместил фокус на Flink, а ksqlDB перестал активно
развиваться. Для разработчиков правильный современный выбор — **Flink SQL**
(привычный SQL поверх Kafka, но на полноценном потоковом движке).

Flink даёт то, чего нет в простом консюмере:
- stateful-обработка с восстановлением после сбоя (checkpoints/savepoints);
- окна (tumbling/hopping/session) и event-time;
- **exactly-once** end-to-end (Flink → Kafka с транзакциями);
- SQL/Table API поверх тех же потоков.

---

## 2. Модель Flink

Три уровня API:

| Уровень | Что это | Пример |
|---|---|---|
| **DataStream API** | императивный код на Java/Scala | `stream.filter().keyBy().window().sum()` |
| **Table API** | декларативные таблицы в коде | `table.groupBy($("status")).select(...)` |
| **Flink SQL** | обычный SQL | `SELECT status, SUM(amount) FROM orders GROUP BY status` |

В этом модуле работаем через **Flink SQL** (SQL Client): минимум кода, максимум
наглядности.

Ключевые понятия Flink SQL:
- **CREATE TABLE** — описание источника/приёмника (connector + format);
- **SELECT** в потоковом режиме — не «прочитать и закончить», а **непрерывный**
  запрос, реагирующий на каждое новое событие;
- **INSERT INTO** — запись результата в sink (тот же Kafka, другая таблица);
- **окна** — `TUMBLE` / `HOP` / `SESSION` для агрегаций по времени.

---

## 3. Kafka connector

Flink подключается к Kafka через коннектор `flink-sql-connector-kafka`. В
`WITH`-блоке таблицы задаются:

```sql
CREATE TABLE orders (
  orderId STRING,
  customerId STRING,
  amount DOUBLE,
  status STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders',
  'properties.bootstrap.servers' = 'kafka1:29092,kafka2:29092,kafka3:29092',
  'properties.group.id' = 'flink-orders',
  'format' = 'json',
  'scan.startup.mode' = 'earliest-offset'   -- с какого офсета читать
);
```

Разбор параметров:
- `connector` — тип коннектора (`kafka`);
- `topic` — топик Kafka;
- `properties.*` — обычные свойства Kafka-клиента (bootstrap.servers, group.id);
  **внутри Docker используем INTERNAL-адреса** `kafkaN:29092`, а не `localhost`;
- `format` — сериализация (`json`, `avro`, `protobuf`, `csv`, `raw`);
- `scan.startup.mode` — `earliest-offset` / `latest-offset` / `group-offsets`.

`bootstrap.servers` здесь — INTERNAL-адреса, потому что Flink работает в контейнере
рядом с брокерами (как Schema Registry и REST Proxy). Клиенты на хосте используют
`localhost:9092..9094`.

---

## 4. Развёртывание (Docker)

Flink уже добавлен в `docker-compose.yml`: сервисы `flink-jobmanager` и
`flink-taskmanager` (образ `flink:1.20-scala_2.12`).

Перед первым запуском нужно положить Kafka-коннектор в библиотеку Flink:

```bash
# скачать «fat»-jar коннектора (версия коннектора должна совпадать с версией Flink)
curl -L -o /tmp/flink-sql-connector-kafka.jar \
  https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar

# положить его в lib Flink
docker cp /tmp/flink-sql-connector-kafka.jar flink-jobmanager:/opt/flink/lib/
```

> Важно: в Flink 1.20 SQL Client **не подхватывает** JAR'ы из `lib` автоматически,
> поэтому при запуске передаём его явно флагом `-j` (см. шаг 3 ниже).

Flink Web UI: <http://localhost:18081>.

---

## Live coding: Flink SQL читает Kafka, фильтрует и агрегирует

### Шаг 1. Подготовка

```bash
docker compose up -d          # весь стенд, включая Flink
docker compose ps             # flink-jobmanager / flink-taskmanager должны быть Up
```

### Шаг 2. Данные в Kafka

Создадим топики **явно** — в нашем стенде автосоздание выключено
(`KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"` в `docker-compose.yml`). Поэтому
sink-топики `orders-paid` и `orders-agg-flink`, куда Flink будет писать,
нужно создать заранее — иначе `INSERT INTO` упадёт с ошибкой, что топика нет.

```bash
docker exec kafka1 kafka-topics --bootstrap-server localhost:29092 \
  --create --topic orders --partitions 4 --replication-factor 3
docker exec kafka1 kafka-topics --bootstrap-server localhost:29092 \
  --create --topic orders-paid --partitions 3 --replication-factor 3
docker exec kafka1 kafka-topics --bootstrap-server localhost:29092 \
  --create --topic orders-agg-flink --partitions 3 --replication-factor 3
java -jar dist/module02-producer/producer-example.jar
```

### Шаг 3. SQL Client

```bash
docker exec -it flink-jobmanager ./bin/sql-client.sh -j /opt/flink/lib/flink-sql-connector-kafka.jar
```

Появится приглашение `Flink SQL>`. Все команды ниже вводятся в нём.

### Шаг 4. Таблица-источник из Kafka

```sql
CREATE TABLE orders (
  orderId STRING,
  customerId STRING,
  amount DOUBLE,
  status STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders',
  'properties.bootstrap.servers' = 'kafka1:29092,kafka2:29092,kafka3:29092',
  'properties.group.id' = 'flink-orders',
  'format' = 'json',
  'scan.startup.mode' = 'earliest-offset'
);
```

Проверим, что Flink видит данные:

```sql
SELECT * FROM orders;
```

Потоковый `SELECT` будет печатать строки по мере чтения (для завершения нажмите
`q`). По умолчанию Flink SQL в консоли показывает непрерывный поток.

### Шаг 5. Фильтрация (append-запрос в новый топик)

Создадим sink-таблицу и запишем в неё только оплаченные заказы:

```sql
CREATE TABLE orders_paid (
  orderId STRING,
  amount DOUBLE
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders-paid',
  'properties.bootstrap.servers' = 'kafka1:29092,kafka2:29092,kafka3:29092',
  'format' = 'json'
);

INSERT INTO orders_paid
SELECT orderId, amount FROM orders WHERE status = 'PAID';
```

Проверим результат с хоста:

```bash
docker exec kafka1 kafka-console-consumer --bootstrap-server localhost:29092 \
  --topic orders-paid --from-beginning
```

### Шаг 6. Оконная агрегация по статусу

Посчитаем сумму по статусам за 30-секундные окна:

```sql
CREATE TABLE orders_agg (
  window_start TIMESTAMP(3),
  status STRING,
  total DOUBLE,
  cnt BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders-agg-flink',
  'properties.bootstrap.servers' = 'kafka1:29092,kafka2:29092,kafka3:29092',
  'format' = 'json'
);

INSERT INTO orders_agg
SELECT
  TUMBLE_START(PROCTIME(), INTERVAL '30' SECONDS) AS window_start,
  status,
  SUM(amount) AS total,
  COUNT(*) AS cnt
FROM orders
GROUP BY TUMBLE(PROCTIME(), INTERVAL '30' SECONDS), status;
```

Запустите `ProducerExample` ещё раз (новые заказы) и через ~30 секунд прочитайте
результат:

```bash
docker exec kafka1 kafka-console-consumer --bootstrap-server localhost:29092 \
  --topic orders-agg-flink --from-beginning
```

### Шаг 7. Очистка

```sql
EXIT;
```

```bash
docker exec kafka1 kafka-topics --bootstrap-server localhost:29092 --delete --topic orders-paid
docker exec kafka1 kafka-topics --bootstrap-server localhost:29092 --delete --topic orders-agg-flink
```

---

## Итоги модуля

- Flink — отдельный движок потоковой обработки; Flink SQL заменяет устаревший ksqlDB.
- Flink SQL: `CREATE TABLE` (connector+format) → `SELECT` (непрерывный) → `INSERT INTO`.
- Kafka-коннектор: `properties.bootstrap.servers` внутри Docker — `kafkaN:29092`.
- Агрегации с окнами (`TUMBLE`) дают потоковые итоги по времени.
