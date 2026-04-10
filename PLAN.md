# План Работ: `oracle-cdc-consumer-no-validation`

## Цель
Сделать изолированный Kafka consumer для CDC-сообщений от `oracle-producer-archivelog-sr-no-validation`:
- только `topic-per-table`;
- только `oneshot` запуск;
- manual commit offsets;
- управляемая политика обработки битых сообщений.

## Этапы

### 1. Каркас проекта
- [x] Создать отдельную папку проекта на уровне `/Users/dmitrijpozdnakov/work/kafka/`.
- [x] Добавить `Dockerfile`, `requirements.txt`, `docker-compose.yaml`.
- [x] Добавить базовую структуру `app/` и `env/`.

### 2. Реализация consumer runtime
- [x] Реализовать Kafka Consumer с `enable.auto.commit=false`.
- [x] Реализовать `oneshot` цикл обработки (`MAX_MESSAGES`, `MAX_EMPTY_POLLS`).
- [x] Поддержать подписку по regex `TOPIC_REGEX` (только topic-per-table).

### 3. Валидация входящих CDC-сообщений
- [x] Проверка `key/value` как UTF-8 JSON object.
- [x] Проверка минимального CDC envelope (`op/source/before/after`).

### 4. Ошибки и политики обработки
- [x] Реализовать `BAD_MESSAGE_POLICY=strict`.
- [x] Реализовать `BAD_MESSAGE_POLICY=skip`.
- [x] Реализовать `BAD_MESSAGE_POLICY=dlq` (+ отправка оригинальных bytes в DLQ).

### 5. Конфигурация и документация
- [x] Добавить `env/consumer.env.example`.
- [x] Добавить рабочий `env/consumer.env`.
- [x] Описать запуск и политики в `README.md`.
- [x] Добавить подробные комментарии в `app/consumer.py`.

### 6. Сертификаты и интеграция
- [x] Подготовить путь для CA сертификата: `./certs/ca.crt`.
- [x] Скопировать `ca.crt` в проект consumer.
- [x] Проверить соответствие `docker-compose` и `SSL_CAFILE`.

### 7. Базовая проверка
- [x] Проверка синтаксиса Python (`py_compile`).
- [x] Проверка compose-конфига (`docker compose config`).

## Ближайшие улучшения (по желанию)
- [ ] Добавить метрики (processed/failed/lag) в Prometheus-friendly формате.
- [ ] Добавить healthcheck-скрипт и structured JSON logs.
- [ ] Добавить интеграционный тестовый сценарий с тестовым топиком и фикстурами сообщений.
- [ ] Добавить отдельный DLQ consumer для разборов проблемных сообщений.
