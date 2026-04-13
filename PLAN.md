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

### 8. Stage + Apply (hard delete first)
- [x] Расширить Postgres sink до stage-модели со статусами apply.
- [x] Добавить one-shot apply entrypoint (`app/apply.py`).
- [x] Реализовать apply simulation:
  - `op=c/u` -> `upsert` (simulation),
  - `op=d` -> `hard_delete` (simulation).
- [x] Добавить запись apply-аудита в CSV (`APPLY_SIMULATION_CSV_PATH`).
- [x] Добавить compose-сервис `oracle-cdc-apply-no-validation`.
- [x] Обновить env/README/components под новый контур.

### 9. Локальная Документация По Папкам
- [x] Добавить mini-`README.md` в рабочие директории проекта:
  - `app/`
  - `app/components/`
  - `app/components/sinks/`
  - `app/components/sinks/postgres/`
  - `env/`
  - `certs/`
  - `state/`
- [x] Зафиксировать краткие правила по назначению и границам изменений для каждой папки.

### 10. Рефакторинг Читаемости (согласуем поэтапно)
- [x] Этап A (без изменения бизнес-логики):
  - [x] Упростить CLI-слой (`consumer.py`, `apply.py`) через общий bootstrap helper.
  - [x] Разбить длинные `run_once` на более мелкие методы.
  - [x] Вынести структуру статистики в отдельные dataclass-модели.
- [ ] Этап B (декомпозиция компонентов):
  - [ ] Разделить слой apply на orchestration + repository + audit writer.
  - [ ] Разнести конфиг по доменным секциям (Kafka/Sink/Postgres/Apply/DLQ).
- [ ] Этап C (контроль регрессий):
  - [ ] Добавить базовые unit-тесты для parser/config/factory/apply action mapping.
  - [ ] Добавить единый локальный check-скрипт для быстрой валидации.

### 11. Runtime-Схема Через Настройки Codex
- [x] Определить файл схемы: `state/runtime_flow.md`.
- [x] Зафиксировать правило в `.codex`, что схема обновляется при изменениях runtime-потоков.
- [x] Добавить шаблон схемы: `.codex/templates/runtime_flow.md.tpl`.
- [x] Убрать runtime-генерацию схемы из кода приложения.
- [x] Обновить документацию по новой модели сопровождения схемы.

## Ближайшие улучшения (по желанию)
- [ ] Добавить метрики (processed/failed/lag) в Prometheus-friendly формате.
- [ ] Добавить healthcheck-скрипт и structured JSON logs.
- [ ] Добавить интеграционный тестовый сценарий с тестовым топиком и фикстурами сообщений.
- [ ] Добавить отдельный DLQ consumer для разборов проблемных сообщений.
- [ ] Заменить apply simulation на реальный apply в target main-table (SQL MERGE/UPSERT/DELETE).
