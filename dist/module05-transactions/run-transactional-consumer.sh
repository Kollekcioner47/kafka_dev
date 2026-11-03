#!/usr/bin/env bash
# Модуль 5 — транзакционный консюмер
exec java -jar "$(dirname "$0")/transactional-consumer.jar" "$@"