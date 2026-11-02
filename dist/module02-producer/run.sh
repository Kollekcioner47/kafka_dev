#!/usr/bin/env bash
# Модуль 2 — Producer. Режимы: send (по умолчанию) | bench | partitioner
exec java -jar "$(dirname "$0")/producer-example.jar" "$@"