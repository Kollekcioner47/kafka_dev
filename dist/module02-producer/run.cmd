@echo off
REM Модуль 2 — Producer. Режимы: send (по умолчанию) | bench | partitioner
java -jar "%~dp0producer-example.jar" %*