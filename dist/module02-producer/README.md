# Модуль 2 — Producer (готовый артефакт)

Fat-JAR: Maven и исходники не нужны, достаточно Java 17+.

## Запуск

    java -jar producer-example.jar              # обычная отправка с Callback (send)
    java -jar producer-example.jar bench        # сравнение acks=0 vs acks=all
    java -jar producer-example.jar partitioner  # через OrderPartitioner

Требование: Kafka на `localhost:9092..9094` (поднимите `docker compose up -d`
из корня курса) и топик `orders` (4 партиции, RF=3).

## Состав

- `ProducerExample` — продюсер: режимы send / bench / partitioner;
- `OrderPartitioner` — пользовательский партиционер (без ключа → партиция 0).