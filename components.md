# Components Map

Документ описывает состав модулей в `app/components` и их роли.

## Общая схема потока

`consumer.py`  
-> `entrypoints/common.py` (shared bootstrap)  
-> `components/consumer_runner.py`  
-> `components/kafka_clients.py` (создание клиентов Kafka)  
-> `components/cdc_message_parser.py` (парсинг/валидация CDC payload)  
-> `components/sinks/factory.py` (выбор sink по `SINK_TYPE`)  
-> `components/sinks/csv_sink.py` или `components/sinks/postgres/sink.py` (ingest в stage)  
-> `components/dlq.py` (обработка проблемных сообщений при policy=dlq)  
-> `components/logger.py` (единый формат логов)

Отдельный apply-контур:

`apply.py`  
-> `entrypoints/common.py` (shared bootstrap)  
-> `components/apply_runner.py`  
-> `components/sinks/postgres/apply_orchestrator.py`  
-> `components/sinks/postgres/repository.py` (claim/mark/count)  
-> `components/sinks/postgres/audit_writer.py` (CSV-аудит apply)  
-> `components/sinks/postgres/real_applier.py` (реальный upsert/delete, если `APPLY_MODE=real`)  
-> CSV-аудит действий apply (`APPLY_SIMULATION_CSV_PATH`)

## Компоненты

## 1) `consumer_runner.py`
- Координатор one-shot цикла.
- Выполняет `poll -> parse -> sink -> commit`.
- Применяет политики ошибок: `strict | skip | dlq`.
- Формирует итоговую статистику батча.

## 1.1) `apply_runner.py`
- Координатор one-shot apply цикла.
- Работает только с `SINK_TYPE=postgres`.
- Поддерживает `APPLY_MODE=simulate|real`.
- Выполняет `claim(new) -> apply(simulate|real) -> mark status`.

## 1.2) `stats.py`
- Dataclass-модели статистики one-shot циклов.
- Централизует структуру результата:
  - `ConsumerBatchStats`
  - `ApplyBatchStats`
- Упрощает поддержку и делает формат счетчиков единообразным.

## 2) `kafka_clients.py`
- Фабрика Kafka-клиентов из `Config`.
- Создает:
  - основной `Consumer` (manual commit);
  - `Producer` для DLQ (только при `BAD_MESSAGE_POLICY=dlq`).
- Инкапсулирует security-параметры (`PLAINTEXT/SSL/SASL_*`).

## 3) `cdc_message_parser.py`
- Декодирует `key/value` из bytes в JSON.
- Проверяет базовый CDC envelope:
  - `op` (`c/u/d`);
  - `source` (`schema/table/commit_scn`);
  - обязательное поле `data` (object payload).

## 4) `sinks/*` subcomponent
- `sinks/base.py`:
  - базовый контракт sink-а (`write_processed_message`, `close`).
- `sinks/factory.py`:
  - выбирает sink по `SINK_TYPE`.
- `sinks/csv_sink.py`:
  - CSV sink (имитация записи в БД).
- `sinks/postgres/config.py`:
  - настройки и валидация Postgres sink-а.
- `sinks/postgres/schema.py`:
  - создание schema/table (DDL);
  - миграция stage-колонок apply (`apply_status`, `apply_action`, `apply_error_text`, ...).
- `sinks/postgres/sink.py`:
  - ingest запись в Postgres stage-таблицу;
  - idempotent вставка по ключу `(kafka_topic, kafka_partition, kafka_offset)`.
- `sinks/postgres/repository.py`:
  - SQL-слой apply-контура:
  - `claim_new_rows`, `mark_applied`, `mark_error`, `count_new_rows`.
- `sinks/postgres/audit_writer.py`:
  - запись CSV-аудита действий apply.
- `sinks/postgres/real_applier.py`:
  - реальный `upsert/delete` в target-таблицы Postgres;
  - данные для применения берутся из `value_json.data`;
  - PK для `upsert/delete` определяется по именованному PK-constraint
    `<APPLY_PK_CONSTRAINT_PREFIX><schema>_<table>`;
  - в `applied_real` фиксирует в stage `target_pkey_name` и `target_pkey_columns`;
  - SQL запроса пишет в `apply_sql_text`:
    при `APPLY_SQL_AUDIT_MODE=full` для успеха и всегда при error.
  - schema выбирается из `APPLY_TARGET_SCHEMA` или `source_schema`.
- `sinks/postgres/apply_orchestrator.py`:
  - orchestration one-shot apply (`simulate|real`);
  - в `simulate`: действие рассчитывается без записи в target-таблицу;
  - в `real`: делегирует реальное применение в `real_applier.py`;
  - делегирует SQL stage в repository, CSV-аудит в audit writer.

## 5) `dlq.py`
- Публикует проблемные сообщения в `DLQ_TOPIC`.
- Сохраняет оригинальные key/value (в base64) и метаданные сообщения.
- Делает flush для подтверждения доставки.

## 6) `logger.py`
- Единая обертка логирования.
- Поддерживает `info/warning/error`.
- Контролирует verbose-вывод.

## Зависимости между компонентами

- `consumer_runner` зависит от `parser`, `kafka_clients`, `dlq`, `logger`, `sinks/factory`.
- `apply_runner` зависит от `logger` и `sinks/postgres/apply_orchestrator`.
- `consumer_runner` и `apply_orchestrator` используют `components/stats.py`.
- `apply_orchestrator` зависит от `sinks/postgres/repository.py`, `audit_writer.py`, `real_applier.py`.
- Вся Postgres-логика находится внутри `components/sinks/postgres/*`.
- `config.py` является источником конфигурации для всех компонентов
  и разнесен по доменным секциям: `kafka/sink/postgres/apply/dlq/logging`.

## Правила расширения

- Новую бизнес-фичу добавлять отдельным компонентом в `app/components`.
- Новые типы sink добавлять только в `app/components/sinks`.
- Не смешивать в одном модуле:
  - Kafka transport,
  - бизнес-обработку,
  - sink-логику,
  - DLQ-политику,
  - форматирование логов.
- Любые изменения поведения фиксировать в `README.md` и `CHANGELOG.md`.
