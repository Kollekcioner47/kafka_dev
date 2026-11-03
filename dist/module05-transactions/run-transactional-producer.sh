#!/usr/bin/env bash
# Модуль 5 — транзакционный продюсер
exec java -jar "$(dirname "$0")/transactional-producer.jar" "$@"