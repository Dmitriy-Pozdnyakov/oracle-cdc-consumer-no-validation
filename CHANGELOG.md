# Журнал Изменений

Формат близок к `Keep a Changelog`.

## [0.2.0] - 2026-04-10

### Добавлено
- Добавлен отдельный one-shot apply entrypoint: `app/apply.py`.
- Добавлен apply оркестратор: `app/components/apply_runner.py`.
- Добавлен Postgres apply simulator:
  - `app/components/sinks/postgres/apply_simulator.py`;
  - действия: `upsert` для `op=c/u` и `hard_delete` для `op=d`.
- Добавлены новые env-параметры apply-контура:
  - `APPLY_MODE`
  - `APPLY_BATCH_SIZE`
  - `APPLY_MAX_ROWS`
  - `APPLY_SIMULATION_CSV_PATH`
- Добавлен compose-сервис `oracle-cdc-apply-no-validation`.

### Изменено
- `app/components/sinks/postgres/schema.py` расширен до stage-модели:
  - добавлены apply-колонки (`apply_status`, `apply_action`, `apply_error_text`, `apply_retry_count`, `apply_started_at_utc`, `apply_finished_at_utc`);
  - добавлена миграция старых таблиц через `ADD COLUMN IF NOT EXISTS`;
  - добавлены индексы для apply-прохода.
- `app/config.py` расширен apply-настройками и fail-fast валидацией.
- Обновлены:
  - `README.md`
  - `components.md`
  - `PLAN.md`
  - `env/consumer.env`
  - `env/consumer.env.example`

### Проверено
- `python3 -m py_compile` для обновленных модулей (с `PYTHONPYCACHEPREFIX=/tmp/pycache`).
- `docker compose config` для `docker-compose.yaml`.

## [0.1.0] - 2026-04-09

### Добавлено
- Создан отдельный проект `oracle-cdc-consumer-no-validation` на уровне `/Users/dmitrijpozdnakov/work/kafka/`.
- Добавлен `oneshot` consumer для Kafka: `app/consumer.py`.
- Добавлен конфиг-слой: `app/config.py`.
- Добавлена поддержка regex-подписки на `topic-per-table` через `TOPIC_REGEX`.
- Добавлен ручной commit offset (`enable.auto.commit=false` + `consumer.commit(...)`).
- Добавлена валидация входящих CDC сообщений (`op/source/before/after`, UTF-8 JSON).
- Добавлены политики обработки битых сообщений: `strict`, `skip`, `dlq`.
- Добавлен DLQ-путь с сохранением исходных key/value в base64.
- Добавлены файлы запуска и окружения:
  - `docker-compose.yaml`
  - `Dockerfile`
  - `requirements.txt`
  - `env/consumer.env.example`
  - `env/consumer.env`
- Добавлена документация: `README.md`.
- Добавлен план работ: `PLAN.md`.
- Добавлены подробные комментарии в `app/consumer.py`.
- Подготовлен путь под сертификат `certs/ca.crt`, сертификат скопирован в проект.
- Добавлен модуль-заглушка Postgres sink:
  - `app/components/sinks/csv_sink.py`
  - запись обработанных сообщений в CSV вместо реальной БД.
- Добавлен реальный Postgres sink:
  - `app/components/sinks/postgres/sink.py`
  - запись обработанных сообщений в Postgres таблицу.
- Добавлена зависимость `psycopg[binary]` для Postgres sink.

### Изменено
- Выполнен рефакторинг в feature-scope классах:
  - `app/components/consumer_runner.py`
  - `app/components/kafka_clients.py`
  - `app/components/cdc_message_parser.py`
  - `app/components/dlq.py`
  - `app/components/logger.py`
- `app/consumer.py` упрощен до тонкого CLI entrypoint без бизнес-логики.
- Добавлены недостающие комментарии и русские docstring в `app/consumer.py` и `app/components/*`.
- `app/components/consumer_runner.py` обновлен: запись в CSV-заглушку выполняется до commit offset.
- В конфиг добавлен переключатель sink-режима `SINK_TYPE=csv|postgres`.
- Добавлены параметры `CSV_SINK_PATH` и блок `POSTGRES_*` для режима `SINK_TYPE=postgres`.
- `docker-compose.yaml` обновлен: добавлен volume `./state:/state` для CSV-результатов.
- `env/consumer.env` и `env/consumer.env.example` синхронизированы с Docker Kafka проектом:
  - `KAFKA_BROKER=host.docker.internal:19092,19093,19094`
  - `KAFKA_SECURITY_PROTOCOL=SASL_SSL`
  - `SSL_CHECK_HOSTNAME=false`
- Обновлены документация и карта компонентов под новую sink-модель:
  - `README.md`
  - `components.md`
- Postgres-логика вынесена в отдельный subcomponent `app/components/sinks/postgres/*`:
  - `config.py`
  - `schema.py`
  - `sink.py`
- CSV sink вынесен в `app/components/sinks/csv_sink.py`, выбор sink — через `app/components/sinks/factory.py`.

### Проверено
- Синтаксис Python (`py_compile`) для consumer-конфига и runtime.
- Валидность compose-конфига (`docker compose config`).
