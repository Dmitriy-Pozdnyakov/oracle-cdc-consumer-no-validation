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
-> `components/sinks/postgres/apply_simulator.py`  
-> `components/sinks/postgres/repository.py` (claim/mark/count)  
-> `components/sinks/postgres/audit_writer.py` (CSV-аудит apply)  
-> CSV-аудит симуляции (`APPLY_SIMULATION_CSV_PATH`)

## Компоненты

## 1) `consumer_runner.py`
- Координатор one-shot цикла.
- Выполняет `poll -> parse -> sink -> commit`.
- Применяет политики ошибок: `strict | skip | dlq`.
- Формирует итоговую статистику батча.

## 1.1) `apply_runner.py`
- Координатор one-shot apply simulation цикла.
- Работает только с `SINK_TYPE=postgres`.
- Выполняет `claim(new) -> simulate(upsert/hard_delete) -> mark status`.

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
  - корректность `before/after` по типу операции.

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
  - запись CSV-аудита симулированных действий apply.
- `sinks/postgres/apply_simulator.py`:
  - orchestration one-shot apply simulation;
  - `op=c/u` -> `upsert`, `op=d` -> `hard_delete`;
  - делегирует SQL в repository, CSV-аудит в audit writer.

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
- `apply_runner` зависит от `logger` и `sinks/postgres/apply_simulator`.
- `consumer_runner` и `apply_simulator` используют `components/stats.py`.
- `apply_simulator` зависит от `sinks/postgres/repository.py` и `sinks/postgres/audit_writer.py`.
- Вся Postgres-логика находится внутри `components/sinks/postgres/*`.
- `config.py` является источником конфигурации для всех компонентов.

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
