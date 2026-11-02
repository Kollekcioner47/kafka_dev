#!/usr/bin/env bash
# Модуль 3 — наглядный консюмер
exec java -jar "$(dirname "$0")/consumer-visualizer.jar" "$@"