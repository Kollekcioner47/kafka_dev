# Модуль 7. Kafka Schema Registry

> 2 ак.ч · ~1 час теория + ~1 час live coding

## Цели модуля

- Понять структуру сообщений и варианты сериализации.
- Освоить формат Avro и его модель эволюции схем.
- Сравнить Avro, Protobuf и JSON.
- Разобрать Schema Registry (subjects, версии, совместимость).
- Написать продюсер/консюмер с Avro-сериализацией через Schema Registry.

---

## 1. Структура и варианты сообщений

Сообщение Kafka — это `key` и `value` в виде **байтов**. Как превратить объект в
байты и обратно — решает клиент. Варианты:

1. **JSON** (`StringSerializer`) — просто, читаемо, но без схемы: громоздко,
   медленно, изменения ломают консюмеры «молча».
2. **Бинарные форматы со схемой** — Avro, Protobuf, JSON Schema: компактно,
   быстро, схема управляется централизованно.
3. **Свои форматы** (Java Serialization и т.п.) — не рекомендуется (хрупкость,
   версионность, безопасность).

Ключевая проблема, которую решает Schema Registry: **эволюция данных**.
Продуценты и консюмеры обновляются независимо, и схема должна меняться так,
чтобы старые и новые версии могли сосуществовать.

---

## 2. Avro формат

**Avro** — компактный бинарный формат сериализации со схемой в JSON-подобном
синтаксисе (`.avsc`). Схема хранится **отдельно от данных** (в Schema Registry),
поэтому сами сообщения маленькие.

Пример схемы `Order`:

```json
{
  "type": "record",
  "name": "Order",
  "namespace": "dev.kafka",
  "fields": [
    { "name": "orderId",    "type": "string" },
    { "name": "customerId", "type": "string" },
    { "name": "amount",     "type": "double" },
    { "name": "status",     "type": "string" }
  ]
}
```

Типы: примитивы (`string`, `int`, `long`, `double`, `boolean`, `bytes`),
сложные (`record`, `enum`, `array`, `map`), `union` (включая `null` для
опциональных полей), логические типы (`timestamp-millis`, `decimal`).

### Эволюция схем

Совместимость бывает:
- **BACKWARD** — новые данные читаются старым консюмером (можно добавлять поля
  с дефолтом, но не удалять обязательные);
- **FORWARD** — старые данные читаются новым консюмером;
- **FULL** — и то, и другое (наиболее безопасный дефолт);
- **NONE** — совместимость не проверяется.

Пример безопасного изменения: добавить поле `currency` с `"default": "USD"`.

---

## 3. Сравнение Avro, Protobuf, JSON

| Критерий | JSON | Avro | Protobuf |
|---|---|---|---|
| Читаемость человеком | да | нет (бинарный) | нет (бинарный) |
| Схема | нет (или отдельно) | да, в Registry | да, в `.proto` |
| Компактность | низкая | высокая | высокая |
| Скорость | низкая | высокая | очень высокая |
| Эволюция | вручную | встроенная (совместимость) | встроенная (нумерация полей) |
| Кодогенерация | нет | опциональна | обязательна |
| Экосистема | везде | Kafka/Hadoop | gRPC/microservices |

Выбор для Kafka:
- нужна простая отладка/быстрый прототип → **JSON**;
- долгоживущие event-driven данные с эволюцией → **Avro** (де-факто стандарт в
  Kafka-экосистеме, тесная связь со Schema Registry);
- уже есть gRPC/строгая типизация и нужна максимальная скорость → **Protobuf**.

---

## 4. Schema Registry

**Schema Registry** — центральный репозиторий схем Avro/Protobuf/JSON Schema для
Kafka. Хранит версии схем и **совместимость** между ними.

Ключевые понятия:
- **Subject** — пространство имён для схемы. По конвенции:
  `<topic>-key` для ключа и `<topic>-value` для значения (например
  `orders-avro-value`).
- **Версия** — каждая новая схема в subject получает номер.
- **Совместимость** — глобальный или per-subject уровень (BACKWARD/FORWARD/FULL/NONE).

### REST API (кратко)

```bash
curl http://localhost:8081/subjects                     # список subject
curl http://localhost:8081/subjects/orders-avro-value/versions/latest
curl http://localhost:8081/config                        # глобальная совместимость
curl -X PUT -H "Content-Type: application/json" \
     --data '{"compatibility": "FULL"}' http://localhost:8081/config
```

### Сериализаторы Confluent

```java
props.put("key.serializer",   "org.apache.kafka.common.serialization.StringSerializer");
props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
props.put("schema.registry.url", "http://localhost:8081");
```

Сериализатор:
1. регистрирует схему в Registry (если новая) и получает **schema id**;
2. пишет сообщение как `[magic byte][schema id][avro bytes]` — в данных только
   id схемы, а не сама схема (экономия места).

Варианты использования Avro в Java:
- **GenericRecord** — схема задаётся строкой/файлом, данные кладутся по имени
  поля; проще для примеров, без кодогенерации;
- **SpecificRecord** — кодогенерация классов из `.avsc` (плагин
  `avro-maven-plugin`); строгая типизация, удобно в крупных проектах.

---

## Live coding: использование Schema Registry с Avro сериализацией

Исходный код — в `code/src/main/java/dev/kafka/AvroProducer.java` и
`AvroConsumer.java` (GenericRecord).

### Шаг 1. Подготовка

Schema Registry уже в compose (порт 8081). Создаём топик:

```bash
docker exec kafka1 kafka-topics --bootstrap-server localhost:29092 \
  --create --topic orders-avro --partitions 3 --replication-factor 3
curl -s http://localhost:8081/subjects   # пока пусто
```

### Шаг 2. Avro-продюсер

```bash
cd code && mvn -q compile
mvn -q exec:java -Dexec.mainClass=dev.kafka.AvroProducer
```

Продюсер пишет заказы в `orders-avro` (value — Avro `Order`, ключ — строка).

### Шаг 3. Проверяем схему в Registry

```bash
curl -s http://localhost:8081/subjects
# ожидаем: ["orders-avro-value"]

curl -s http://localhost:8081/subjects/orders-avro-value/versions/latest
# покажет схему Order с полями orderId/customerId/amount/status
```

### Шаг 4. Avro-консюмер

```bash
mvn -q exec:java -Dexec.mainClass=dev.kafka.AvroConsumer
```

Консюмер читает Avro-байты через `KafkaAvroDeserializer` и выводит поля объекта.

### Шаг 5. Эволюция схемы

Изменим схему, добавив поле `currency` с дефолтом (в `AvroProducer` — второй
режим). Запустим ещё раз:

```bash
mvn -q exec:java -Dexec.mainClass=dev.kafka.AvroProducer -Dexec.args="v2"
curl -s http://localhost:8081/subjects/orders-avro-value/versions   # теперь две версии
mvn -q exec:java -Dexec.mainClass=dev.kafka.AvroConsumer            # старый консюмер читает новые данные
```

Консюмер без поля `currency` корректно читает новые сообщения (BACKWARD/дефолт).
Теперь попробуйте удалить обязательное поле (без дефолта) и посмотрите, как
Registry отклонит несовместимую схему (если совместимость != NONE).

### Шаг 6. Очистка

```bash
docker exec kafka1 kafka-topics --bootstrap-server localhost:29092 --delete --topic orders-avro
curl -X DELETE http://localhost:8081/subjects/orders-avro-value
curl -X DELETE http://localhost:8081/subjects/orders-avro-value?permanent=true
```

---

## Итоги модуля

- Сообщение — байты; формат выбирает клиент (JSON / Avro / Protobuf).
- Avro — компактный бинарный формат со схемой и встроенной эволюцией.
- Schema Registry хранит схемы по subject (`<topic>-value`/`-key`), версии и
  правила совместимости (BACKWARD/FORWARD/FULL/NONE).
- Confluent-сериализаторы пишут в сообщение только schema id; GenericRecord —
  без кодогенерации, SpecificRecord — со строгой типизацией.
