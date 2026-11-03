# Примеры кода к курсу «Apache Kafka для разработчиков»

Maven-проект с готовыми классами для live coding из модулей 2–7.
Все классы находятся в пакете `dev.kafka`.

## Требования

- JDK 17 или 21;
- Maven 3.8+;
- поднятый стенд `docker compose up -d` (см. `docker-compose.yml` в корне курса).

> **Если Maven Central заблокирован (РКН):** скопируйте `maven-settings.xml`
> из корня курса в `~/.m2/settings.xml` — Maven переключится на зеркало
> Google Cloud Storage.

## Сборка

```bash
cd code
mvn -q compile
```

## Запуск по модулям

```bash
# Модуль 2 — продюсер (обычная отправка с Callback)
mvn -q exec:java -Dexec.mainClass=dev.kafka.ProducerExample
mvn -q exec:java -Dexec.mainClass=dev.kafka.ProducerExample -Dexec.args="partitioner"
mvn -q exec:java -Dexec.mainClass=dev.kafka.ProducerExample -Dexec.args="bench"

# Модуль 3 — консюмер (аргумент: earliest | latest)
mvn -q exec:java -Dexec.mainClass=dev.kafka.ConsumerExample
mvn -q exec:java -Dexec.mainClass=dev.kafka.ConsumerExample -Dexec.args="latest"

# Модуль 3 — НАГЛЯДНЫЙ консюмер (метка, earliest|latest, группа)
# показывает: назначенные партиции, что читается, момент коммита,
# и таблицу position / committed / end / lag по каждой партиции
mvn -q exec:java -Dexec.mainClass=dev.kafka.ConsumerVisualizer -Dexec.args="C1 earliest"
mvn -q exec:java -Dexec.mainClass=dev.kafka.ConsumerVisualizer -Dexec.args="C2 earliest orders-group"

# Модуль 4 — Admin API (режимы ниже)
mvn -q exec:java -Dexec.mainClass=dev.kafka.AdminExample -Dexec.args="create"
mvn -q exec:java -Dexec.mainClass=dev.kafka.AdminExample -Dexec.args="describe"
mvn -q exec:java -Dexec.mainClass=dev.kafka.AdminExample -Dexec.args="partitions"
mvn -q exec:java -Dexec.mainClass=dev.kafka.AdminExample -Dexec.args="groups"
mvn -q exec:java -Dexec.mainClass=dev.kafka.AdminExample -Dexec.args="delete-records"
mvn -q exec:java -Dexec.mainClass=dev.kafka.AdminExample -Dexec.args="reset-offsets"
mvn -q exec:java -Dexec.mainClass=dev.kafka.AdminExample -Dexec.args="quota"
mvn -q exec:java -Dexec.mainClass=dev.kafka.AdminExample -Dexec.args="list-quotas"
mvn -q exec:java -Dexec.mainClass=dev.kafka.AdminExample -Dexec.args="acl"
mvn -q exec:java -Dexec.mainClass=dev.kafka.AdminExample -Dexec.args="list-acls"

# Модуль 5 — транзакции
mvn -q exec:java -Dexec.mainClass=dev.kafka.TransactionalProducer -Dexec.args="commit"
mvn -q exec:java -Dexec.mainClass=dev.kafka.TransactionalProducer -Dexec.args="abort"
mvn -q exec:java -Dexec.mainClass=dev.kafka.TransactionalConsumer -Dexec.args="read_committed"
mvn -q exec:java -Dexec.mainClass=dev.kafka.TransactionalConsumer -Dexec.args="read_uncommitted"

# Модуль 6 — Kafka Streams (работает, пока не остановите Ctrl+C)
mvn -q exec:java -Dexec.mainClass=dev.kafka.StreamsExample

# Модуль 7 — Schema Registry + Avro
mvn -q exec:java -Dexec.mainClass=dev.kafka.AvroProducer
mvn -q exec:java -Dexec.mainClass=dev.kafka.AvroProducer -Dexec.args="v2"
mvn -q exec:java -Dexec.mainClass=dev.kafka.AvroConsumer
```

Модуль 8 (Confluent REST API) не требует Java-кода — только `curl` (примеры в
`08_rest_api.md`).

## Структура

```
code/src/main/java/dev/kafka/
├── ProducerExample.java          # Модуль 2
├── OrderPartitioner.java         # Модуль 2 (пользовательский партиционер)
├── ConsumerExample.java          # Модуль 3 (минимальный)
├── ConsumerVisualizer.java       # Модуль 3 (наглядная демонстрация)
├── AdminExample.java             # Модуль 4
├── TransactionalProducer.java    # Модуль 5
├── TransactionalConsumer.java    # Модуль 5
├── StreamsExample.java           # Модуль 6
├── AvroProducer.java             # Модуль 7
└── AvroConsumer.java             # Модуль 7
```
