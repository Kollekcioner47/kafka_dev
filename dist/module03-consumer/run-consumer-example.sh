#!/usr/bin/env bash
# Модуль 3 — минимальный консюмер
exec java -jar "$(dirname "$0")/consumer-example.jar" "$@"