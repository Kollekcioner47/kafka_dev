# Модуль 4 — Admin API (готовый артефакт)

Fat-JAR: Maven и исходники не нужны, достаточно Java 17+.

## Запуск

    java -jar admin-example.jar <режим>

Режимы:

    create | describe | partitions | groups | delete-records |
    reset-offsets | quota | list-quotas | acl | list-acls

Примечания:

- `acl` требует включённого authorizer на брокере (см. Модуль 4);
- `reset-offsets` — группа должна быть без активных консюмеров.

Требование: Kafka на `localhost:9092..9094` (стек из `docker compose up -d`).
