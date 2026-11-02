# Модуль 3 — Consumer (готовые артефакты)

Fat-JAR'ы: Maven и исходники не нужны, достаточно Java 17+.

## Запуск

    java -jar consumer-example.jar [earliest|latest]
    java -jar consumer-visualizer.jar <метка> [earliest|latest] [группа]

- `consumer-example` — минимальный консюмер с ручным коммитом;
- `consumer-visualizer` — наглядный: печатает rebalance-баннеры, читаемые
  сообщения и таблицу `position / committed / end / lag` по каждой партиции.

Пример:

    java -jar consumer-visualizer.jar C1 earliest
    java -jar consumer-visualizer.jar C2 earliest orders-group

Требование: Kafka на `localhost:9092..9094` (стек из `docker compose up -d`),
топик `orders` (4 партиции, RF=3).
