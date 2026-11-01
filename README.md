# Apache Kafka для разработчиков

> Автор: **Иван Климарев**

Курс на **18 академических часов** (9 модулей × 2 ак.ч) для разработчиков,
которым нужно уверенно писать продюсеры, консюмеры и приложения поверх
Apache Kafka, а не администрировать кластер.

---


## 1. Цель и аудитория

**Аудитория:** Java-разработчики (и разработчики на других языках), которые
хотят работать с Kafka на уровне клиентского API: писать Producer/Consumer,
управлять топиками через Admin API, строить потоковую обработку на Kafka
Streams, сериализовать данные через Schema Registry, работать через Confluent
REST API и строить потоковую обработку на Apache Flink.

**Предварительные требования:**
- Java (JDK 17 или 21) и Maven;
- базовое понимание распределённых систем (что такое брокер, лидер, реплика);
- Docker / Docker Compose для локального стенда (Модуль 1).

**После курса разработчик умеет:**
- поднимать локальный стенд Kafka + Kafdrop и пользоваться CLI;
- писать идемпотентные и транзакционные продюсеры с `Callback` и
  пользовательским партицированием;
- писать консюмеры с ручным коммитом offset и обработкой rebalance;
- управлять топиками, партициями, группами, ACL и квотами через Admin API;
- строить приложения потоковой обработки на Kafka Streams (state, join,
  KTable/GlobalKTable, Processor API);
- сериализовать данные в Avro через Schema Registry;
- выполнять запросы к Kafka через Confluent REST API;
- строить потоковую обработку на Apache Flink (Flink SQL + Kafka).

---

## 2. Стек курса

| Компонент | Выбор | Примечание |
|---|---|---|
| Язык кода | **Java 17+ + Maven** | Единый стек; Kafka Streams есть только в JVM |
| Kafka | Confluent Platform **7.7** (Kafka 3.7), **3 брокера** | RF=3, распределение лидеров, failover |
| Координация | **ZooKeeper** (классический режим) | Проще для обучения; в Kafka 4.x ZooKeeper удалён (KRaft) |
| UI | **Kafdrop** | Веб-обзор топиков/партиций/групп |
| Schema Registry | Confluent **cp-schema-registry** | Модуль 7 |
| REST Proxy | Confluent **cp-kafka-rest** | Модуль 8 |
| Потоковая обработка | **Apache Flink** (Flink SQL) | Модуль 9 |
| Сборка | Maven (`kafka-clients`, `kafka-streams`, Avro-сериализаторы) | Проект в `code/` |

> **Почему ZooKeeper, а не KRaft?** Для разработчиков классический режим
> ZooKeeper + Kafdrop — самый наглядный и хорошо документированный способ
> локально развернуть кластер и наблюдать его состояние. В Kafka 4.x ZooKeeper
> удалён (остаётся только KRaft); разница разбирается в Модуле 1.

> **Почему Java и можно ли на других языках?** Kafka Streams и Avro/Schema
> Registry идиоматичны в JVM, поэтому основной стек — Java. Но Producer/Consumer/
> Admin API есть и на других языках: **Python** (`confluent-kafka`, `kafka-python`),
> **Rust** (`rdkafka` / `rust-rdkafka`), **Go** (`franz-go`, `segmentio/kafka-go`),
> **C#/.NET** (`Confluent.Kafka`). Полноценного аналога Kafka Streams на
> Python/Rust нет — потоковую обработку там делают через Flink (Модуль 9).

---

## 3. Разбивка 18 ак.ч по модулям

Каждый модуль = **2 ак.ч** (~1 час теория + ~1 час live coding).

| # | Модуль | Файл | Ключевые темы |
|---|---|---|---|
| 1 | Введение в Apache Kafka | `01_intro.md` | настройка Kafka/ZooKeeper/Kafdrop, топики, партиции, репликация, гарантии, API |
| 2 | Kafka Producer API | `02_producer.md` | ProducerRecord, партиционер, timestamp, конфигурация, KafkaProducer + Callback |
| 3 | Kafka Consumer API | `03_consumer.md` | стратегии распределения партиций, rebalance, offset, конфигурация, KafkaConsumer |
| 4 | Admin API | `04_admin_api.md` | топики, партиции, сообщения/консюмеры, ACL и квоты |
| 5 | Kafka Transactions | `05_transactions.md` | гарантии, Idempotent Producer, exactly-once, транзакции |
| 6 | Kafka Streams | `06_streams.md` | основы, состояние, join, KTable/GlobalKTable, Processor API |
| 7 | Kafka Schema Registry | `07_schema_registry.md` | структура сообщений, Avro, сравнение форматов, Schema Registry |
| 8 | Confluent REST API | `08_rest_api.md` | введение, развёртывание, выполнение запросов |
| 9 | Apache Flink | `09_flink.md` | Flink SQL, Kafka connector, окна и агрегации |

---

## 4. Структура репозитория

```
Apache Kafka для разработчиков/
├── README.md                 ← этот файл (программа курса)
├── docker-compose.yml        ← локальный стенд: ZooKeeper, Kafka, Kafdrop,
│                               Schema Registry, REST Proxy, Flink
├── 01_intro.md ... 09_flink.md
└── code/                     ← Maven-проект с примерами для live coding
    ├── pom.xml
    └── src/main/java/dev/kafka/
```

---

## 5. Сквозной бизнес-кейс

Все модули связаны одной доменной моделью — **заказы (`orders`)**:

| Модуль | Топик / объект | Что делаем |
|---|---|---|
| 1 | `orders` | создаём топик, пишем/читаем через CLI |
| 2 | `orders` | продюсер пишет заказы (JSON), свой партиционер по `orderId` |
| 3 | `orders` | консюмер в группе, ручной коммит, обработка rebalance |
| 4 | `orders`, `orders-agg` | Admin API: создаём/чистим топики, увеличиваем партиции, смотрим группы, ACL |
| 5 | `tx-orders-in/out` | транзакционная запись «заказ + событие» |
| 6 | `orders` → `orders-agg` | Streams: агрегация заказов по статусу, join со справочником |
| 7 | `orders-avro` | те же заказы, но в Avro через Schema Registry |
| 8 | `orders-avro` | чтение/запись через Confluent REST API |
| 9 | `orders` → `orders-paid`, `orders-agg-flink` | Flink SQL: фильтрация и оконная агрегация |

---

## 6. Как проходить курс

1. **Модуль 1** — поднять стенд: `docker compose up -d` (см. `docker-compose.yml`),
   проверить Kafdrop на <http://localhost:9000>.
2. Каждый следующий модуль открывается соответствующим `.md`-файлом, который
   содержит теорию и пошаговый live-coding сценарий с кодом.
3. Готовый код — в `code/` (Maven-проект). Команды для запуска приведены в
   каждом модуле.
4. Повторный проход: в конце каждого модуля есть раздел «Очистка», чтобы
   пройти курс заново.
---

## 7. Лицензия

- **Текст курса** (конспекты, модули `01_intro.md`–`09_flink.md`, `README.md`) —
  [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/): можно делиться
  и адаптировать (в т.ч. коммерчески) с указанием авторства; производные — под
  той же лицензией. См. `LICENSE-CONTENT`.
- **Код** (`code/`, `docker-compose.yml`) — [MIT](https://opensource.org/licenses/MIT).
  См. `LICENSE`.
