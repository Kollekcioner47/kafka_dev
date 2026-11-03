# Модуль 5 — Transactions (готовые артефакты)

Fat-JAR'ы: Maven и исходники не нужны, достаточно Java 17+.

## Запуск

    java -jar transactional-producer.jar [commit|abort]
    java -jar transactional-consumer.jar [read_committed|read_uncommitted]

- `commit` — атомарно пишет в `tx-orders-out` и `tx-audit`;
- `abort` — пишет и откатывает транзакцию (сравните `read_committed` vs
  `read_uncommitted`).

Требование: Kafka на `localhost:9092..9094`, топики `tx-orders-out` и
`tx-audit` (3 партиции, RF=3).
