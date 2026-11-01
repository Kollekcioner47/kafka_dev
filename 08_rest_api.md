# Модуль 8. Confluent REST API

> 2 ак.ч · ~1 час теория + ~1 час live coding

## Цели модуля

- Понять назначение и архитектуру Confluent REST Proxy.
- Развернуть REST Proxy (уже есть в compose) и настроить его.
- Выполнять основные запросы: топики, продюсирование, консьюминг.
- Писать/читать JSON и Avro через HTTP.

---

## 1. Введение в Confluent REST API

**Confluent REST Proxy** — HTTP-интерфейс к кластеру Kafka. Позволяет клиентам,
у которых нет нативного Kafka-клиента (браузер, legacy-системы, языки без
библиотек), работать с Kafka по REST.

Что умеет:
- просмотр топиков, партиций, брокеров (metadata);
- продюсирование сообщений (JSON, Avro, Protobuf, binary);
- создание консюмеров и чтение записей (с коммитом офсетов);
- интеграция со Schema Registry (Avro/Protobuf/JSON Schema).

Ограничения: REST добавляет HTTP-оверхед, поэтому не подходит для
высокопроизводительных сценариев (там — нативные клиенты). Зато идеален для
интеграций, тестов, скриптов и языков без Kafka-клиента.

Версии API:
- **v2** — классический (`/topics`, `/consumers`), используется в примерах ниже;
- **v3** — новый, ресурсно-ориентированный (`/v3/clusters/{cluster}/...`),
  появился в Confluent Platform 7.x.

---

## 2. Развертывание Confluent REST API

REST Proxy уже поднят в `docker-compose.yml` (сервис `rest-proxy`, порт **8082**).
Ключевые переменные окружения:

| Переменная | Значение | Назначение |
|---|---|---|
| `KAFKA_REST_BOOTSTRAP_SERVERS` | `kafka:9092` | куда подключается прокси |
| `KAFKA_REST_LISTENERS` | `http://0.0.0.0:8082` | слушающий порт |
| `KAFKA_REST_SCHEMA_REGISTRY_URL` | `http://schema-registry:8081` | для Avro/схем |
| `KAFKA_REST_HOST_NAME` | `rest-proxy` | имя хоста |

Проверка:

```bash
curl -s http://localhost:8082/ | python -m json.tool   # версия API
curl -s http://localhost:8082/brokers                  # список брокеров
```

---

## 3. Выполнение запросов

### Метаданные топиков

```bash
curl -s http://localhost:8082/topics                       # все топики
curl -s http://localhost:8082/topics/orders                # метаданные топика
curl -s http://localhost:8082/topics/orders/partitions     # партиции
curl -s http://localhost:8082/partitions/orders/0          # партиция 0
```

### Продюсирование (JSON)

```bash
curl -s -X POST http://localhost:8082/topics/orders \
  -H "Content-Type: application/vnd.kafka.json.v2+json" \
  -d '{
    "records": [
      { "key": "o101", "value": {"orderId": "o101", "status": "NEW"} },
      { "value": {"orderId": "o102", "status": "PAID"} }
    ]
  }'
```

Ответ содержит `offsets` — партицию и офсет каждого записанного сообщения.

### Консюмирование (создание → подписка → чтение → удаление)

```bash
# 1) создать консюмер в группе
curl -s -X POST http://localhost:8082/consumers/rest-group \
  -H "Content-Type: application/vnd.kafka.v2+json" \
  -d '{"name": "rest-consumer-1", "format": "json", "auto.offset.reset": "earliest"}'
# ответ: {"instance_id": "rest-consumer-1", "base_uri": "http://.../consumers/rest-group/instances/rest-consumer-1"}

# 2) подписаться на топик
curl -s -X POST http://localhost:8082/consumers/rest-group/instances/rest-consumer-1/subscription \
  -H "Content-Type: application/vnd.kafka.v2+json" \
  -d '{"topics": ["orders"]}'

# 3) читать записи
curl -s http://localhost:8082/consumers/rest-group/instances/rest-consumer-1/records \
  -H "Accept: application/vnd.kafka.json.v2+json"

# 4) удалить консюмер (освободить партиции группы)
curl -s -X DELETE http://localhost:8082/consumers/rest-group/instances/rest-consumer-1
```

### Avro через REST Proxy

```bash
curl -s -X POST http://localhost:8082/topics/orders-avro \
  -H "Content-Type: application/vnd.kafka.avro.v2+json" \
  -d '{
    "value_schema": "{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"dev.kafka\",\"fields\":[{\"name\":\"orderId\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"}]}",
    "records": [
      { "value": {"orderId": "o201", "amount": 99.5} }
    ]
  }'
```

Схема автоматически регистрируется в Schema Registry (subject
`orders-avro-value`).

---

## Live coding: взаимодействие с Kafka через REST API

### Шаг 1. Проверка стенда

```bash
docker compose ps
curl -s http://localhost:8082/ | head -c 300
curl -s http://localhost:8082/brokers
```

### Шаг 2. Топик и запись JSON

```bash
docker exec kafka1 kafka-topics --bootstrap-server localhost:29092 \
  --create --topic rest-orders --partitions 3 --replication-factor 3

curl -s -X POST http://localhost:8082/topics/rest-orders \
  -H "Content-Type: application/vnd.kafka.json.v2+json" \
  -d '{"records":[
        {"key":"r1","value":{"orderId":"r1","customerId":"c1","amount":120,"status":"NEW"}},
        {"key":"r2","value":{"orderId":"r2","customerId":"c2","amount":75,"status":"PAID"}}
      ]}'
```

### Шаг 3. Чтение через REST-консюмер

```bash
curl -s -X POST http://localhost:8082/consumers/rest-group \
  -H "Content-Type: application/vnd.kafka.v2+json" \
  -d '{"name":"rc1","format":"json","auto.offset.reset":"earliest"}'

curl -s -X POST http://localhost:8082/consumers/rest-group/instances/rc1/subscription \
  -H "Content-Type: application/vnd.kafka.v2+json" \
  -d '{"topics":["rest-orders"]}'

curl -s http://localhost:8082/consumers/rest-group/instances/rc1/records \
  -H "Accept: application/vnd.kafka.json.v2+json"
```

Убедитесь, что получены оба сообщения с ключами `r1`/`r2`. Проверьте группу в
Kafdrop (вкладка Consumers → `rest-group`).

### Шаг 4. Avro через REST

```bash
docker exec kafka1 kafka-topics --bootstrap-server localhost:29092 \
  --create --topic rest-orders-avro --partitions 3 --replication-factor 3

curl -s -X POST http://localhost:8082/topics/rest-orders-avro \
  -H "Content-Type: application/vnd.kafka.avro.v2+json" \
  -d '{
    "value_schema": "{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"dev.kafka\",\"fields\":[{\"name\":\"orderId\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"}]}",
    "records": [{"value":{"orderId":"o201","amount":99.5}}]
  }'
```

Проверяем схему в Registry:

```bash
curl -s http://localhost:8081/subjects/rest-orders-avro-value/versions/latest
```

### Шаг 5. Очистка

```bash
curl -s -X DELETE http://localhost:8082/consumers/rest-group/instances/rc1
docker exec kafka1 kafka-topics --bootstrap-server localhost:29092 --delete --topic rest-orders
docker exec kafka1 kafka-topics --bootstrap-server localhost:29092 --delete --topic rest-orders-avro
curl -X DELETE http://localhost:8081/subjects/rest-orders-avro-value
curl -X DELETE "http://localhost:8081/subjects/rest-orders-avro-value?permanent=true"
```

---

## Итоги модуля

- REST Proxy — HTTP-интерфейс к Kafka для языков/систем без нативного клиента.
- v2 API: `/topics` (metadata), `POST /topics/{t}` (produce), `/consumers/{g}/...`
  (consume), `/brokers`.
- Форматы: JSON (`vnd.kafka.json.v2+json`), Avro (`vnd.kafka.avro.v2+json`,
  со схемой), binary, Protobuf.
- Avro-записи через REST автоматически регистрируют схему в Schema Registry.
