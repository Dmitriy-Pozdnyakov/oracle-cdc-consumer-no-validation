# Журнал Изменений

Формат близок к `Keep a Changelog`.

## [0.2.10] - 2026-04-14

### Изменено
- Расширен парсер CDC-сообщений под legacy-формат producer-а:
  - `app/components/cdc_message_parser.py` теперь поддерживает `value.data`;
  - нормализация выполняется по `op`:
    - `c/u`: `data -> after`;
    - `d`: `data -> before`.
- Расширен real apply для producer-форматов, где key не содержит бизнес-PK:
  - добавлен env `APPLY_PK_COLUMNS` (comma-separated);
  - при заданном `APPLY_PK_COLUMNS` PK извлекается из payload (`after_json`/`before_json`), а не из `key_json`.
- Протянут новый параметр apply-конфига через runtime-контур:
  - `app/config.py`
  - `app/components/apply_runner.py`
  - `app/components/sinks/postgres/apply_orchestrator.py`
  - `app/components/sinks/postgres/real_applier.py`
- Обновлена документация:
  - `README.md`
  - `components.md`
  - `env/consumer.env.example`

### Проверено
- Попытка запуска тестов: `python3 -m pytest -q` (модуль `pytest` отсутствует в окружении).
- Смоук-проверка парсера и real-apply PK extraction на payload с `value.data` и transport-key (python-скрипт в Docker-образе `local/python311-with-reqs:1`).
- `PYTHONPYCACHEPREFIX=/tmp/pycache python3 -m compileall app`.

## [0.2.9] - 2026-04-14

### Изменено
- Завершена миграция на секционный доступ к конфигурации:
  - из `app/config.py` удален legacy alias API (`cfg.<old_name>` свойства);
  - `Config` оставлен как агрегатор доменных секций без backward-compat прокси.
- Упрощен адаптер Postgres-настроек:
  - `app/components/sinks/postgres/config.py` больше не поддерживает legacy-поля `cfg.postgres_*`;
  - `postgres_settings_from_app_config` теперь использует только `cfg.postgres.*`;
  - сигнатура ужесточена с `Any` до `Config` (через `TYPE_CHECKING`).
- Обновлена документация по структуре `Config`:
  - `README.md`.

### Проверено
- Поиск по коду на legacy-доступ `cfg.<section>_...` в `app/` (остатки не найдены вне удаленного fallback в `postgres/config.py`).
- Попытка запуска тестов: `python3 -m pytest -q` (модуль `pytest` отсутствует в окружении).
- `python3 -m compileall app`.

## [0.2.8] - 2026-04-13

### Изменено
- Удалены fallback-импорты `try/except ImportError` из runtime-модулей `app/*`.
- Проект переведен на единый пакетный стиль импортов: `from app...`.
- Compose-команды запуска обновлены на пакетный режим:
  - `python -m app.consumer`
  - `python -m app.apply`
- Переименован apply orchestration-модуль:
  - `app/components/sinks/postgres/apply_simulator.py` -> `app/components/sinks/postgres/apply_orchestrator.py`
- Класс оркестратора переименован:
  - `PostgresApplySimulator` -> `PostgresApplyOrchestrator`
- Синхронизирована документация:
  - `README.md`
  - `components.md`
  - `app/components/sinks/postgres/README.md`

### Проверено
- `python3 -m py_compile` для всех Python-модулей `app/`.
- `docker compose config` для обновленного `docker-compose.yaml`.

## [0.2.7] - 2026-04-13

### Добавлено
- Добавлен реальный apply-исполнитель:
  - `app/components/sinks/postgres/real_applier.py`
- Добавлен env-параметр:
  - `APPLY_TARGET_SCHEMA` (override target schema для `APPLY_MODE=real`)

### Изменено
- `APPLY_MODE` расширен до `simulate|real`.
- `app/components/sinks/postgres/apply_simulator.py` теперь оркестрирует оба режима:
  - `simulate`: симуляция действий;
  - `real`: реальный `upsert/delete` в target-таблицы.
- `app/components/sinks/postgres/repository.py` поддерживает stage-статус `applied_real`.
- `app/components/apply_runner.py` поддерживает запуск apply в `simulate|real`.
- Обновлены документы:
  - `README.md`
  - `components.md`
  - `app/components/README.md`
  - `app/components/sinks/postgres/README.md`
  - `env/README.md`
  - `env/consumer.env.example`
  - `PLAN.md`
  - `state/runtime_flow.md`

### Проверено
- `python3 -m py_compile` для обновленных модулей apply-контура.

## [0.2.6] - 2026-04-13

### Изменено
- Углублены docstring-комментарии по функциям в runtime-ядре:
  - `app/components/consumer_runner.py`
  - `app/components/cdc_message_parser.py`
  - `app/components/apply_runner.py`
  - `app/components/sinks/base.py`
  - `app/components/sinks/csv_sink.py`
- Комментарии расширены по целям методов, инвариантам и порядку шагов обработки.

### Проверено
- `python3 -m py_compile` для обновленных модулей.

## [0.2.5] - 2026-04-13

### Изменено
- `app/config.py` декомпозирован на доменные секции:
  - `KafkaConfig`
  - `SinkConfig`
  - `PostgresConfig`
  - `ApplyConfig`
  - `DlqConfig`
  - `LoggingConfig`
- Корневой `Config` теперь агрегирует секции (`cfg.kafka`, `cfg.sink`, `cfg.postgres`, `cfg.apply`, `cfg.dlq`, `cfg.logging`).
- Добавлены alias-свойства старого формата (`cfg.kafka_broker`, `cfg.apply_mode` и т.д.) для обратной совместимости.
- Ключевые runtime-компоненты переведены на секционный доступ:
  - `app/components/kafka_clients.py`
  - `app/components/consumer_runner.py`
  - `app/components/apply_runner.py`
  - `app/components/dlq.py`
  - `app/components/logger.py`
  - `app/components/sinks/factory.py`
  - `app/components/sinks/postgres/config.py`

### Документация
- Обновлены:
  - `README.md`
  - `app/README.md`
  - `PLAN.md`

### Проверено
- `python3 -m py_compile` для обновленных модулей конфигурации и runtime.

## [0.2.4] - 2026-04-13

### Добавлено
- Добавлен `repository`-слой apply-контура:
  - `app/components/sinks/postgres/repository.py`
- Добавлен отдельный writer CSV-аудита:
  - `app/components/sinks/postgres/audit_writer.py`

### Изменено
- `app/components/sinks/postgres/apply_simulator.py` упрощен до orchestration-слоя:
  - SQL-операции делегированы в `repository.py`;
  - CSV-аудит делегирован в `audit_writer.py`;
  - бизнес-поведение apply не изменено.
- `app/components/apply_runner.py` упрощен (прямой вызов `simulator.run_once()`).
- Обновлены документы по структуре компонентов:
  - `README.md`
  - `components.md`
  - `app/components/sinks/postgres/README.md`
  - `PLAN.md`

### Проверено
- `python3 -m py_compile` для обновленных apply-модулей.

## [0.2.3] - 2026-04-13

### Изменено
- Генерация runtime-схемы вынесена из кода приложения в процесс сопровождения через `.codex`.
- Из приложения удалены runtime-настройки схемы:
  - `RUNTIME_DIAGRAM_ENABLED`
  - `RUNTIME_DIAGRAM_PATH`
- `state/runtime_flow.md` оставлен как документируемый артефакт архитектуры.

### Удалено
- `app/components/runtime_diagram.py`.
- Best-effort обновление runtime-схемы из `app/entrypoints/common.py`.
- Runtime diagram блок из `env/consumer.env.example`.

### Документация
- Обновлены:
  - `README.md`
  - `components.md`
  - `PLAN.md`
  - `app/components/README.md`
  - `app/entrypoints/README.md`
  - `env/README.md`
  - `state/README.md`
  - `.codex/project.yaml`
  - `.codex/defaults.yaml`
  - `.codex/runbooks/coding-standards.md`
  - `.codex/checklists/done.md`
  - `.codex/templates/runtime_flow.md.tpl`
- Улучшена читаемость runtime-схемы:
  - отдельные схемы для `Ingest` и `Apply`;
  - русские подписи и комментарии по commit/error-поведению.

## [0.2.2] - 2026-04-13

### Добавлено
- Добавлен модуль автогенерации runtime-схемы:
  - `app/components/runtime_diagram.py`
- Добавлены env-настройки runtime-схемы:
  - `RUNTIME_DIAGRAM_ENABLED`
  - `RUNTIME_DIAGRAM_PATH`

### Изменено
- `app/entrypoints/common.py` обновлен:
  - добавлено best-effort обновление runtime-схемы при запуске entrypoint.
- `app/config.py` расширен настройками runtime-схемы и их валидацией.
- `app/consumer.py` и `app/apply.py` передают режим (`consumer|apply`) в shared bootstrap.
- `.gitignore` обновлен: `state/runtime_flow.md` отмечен как локальный runtime-артефакт.
- Обновлены документация и карта компонентов:
  - `README.md`
  - `components.md`
  - `PLAN.md`
  - `env/consumer.env.example`
  - `app/entrypoints/README.md`
  - `app/components/README.md`
  - `env/README.md`
  - `state/README.md`

### Проверено
- `python3 -m py_compile` для обновленных модулей.
- `docker compose config` для `docker-compose.yaml`.

## [0.2.1] - 2026-04-13

### Добавлено
- Добавлен общий CLI bootstrap helper:
  - `app/entrypoints/common.py`
- Добавлены dataclass-модели статистики:
  - `app/components/stats.py`
- Добавлены mini-`README.md` по рабочим директориям:
  - `app/README.md`
  - `app/components/README.md`
  - `app/components/sinks/README.md`
  - `app/components/sinks/postgres/README.md`
  - `app/entrypoints/README.md`
  - `env/README.md`
  - `certs/README.md`
  - `state/README.md`

### Изменено
- `app/consumer.py` и `app/apply.py` упрощены: общий шаблон запуска вынесен в `entrypoints/common.py`.
- `app/components/consumer_runner.py` декомпозирован на небольшие методы без изменения runtime-поведения.
- `app/components/sinks/postgres/apply_simulator.py` декомпозирован на небольшие методы без изменения runtime-поведения.
- Обновлены документация и карта компонентов:
  - `README.md`
  - `components.md`
  - `PLAN.md`

### Проверено
- `python3 -m py_compile` для обновленных модулей.
- `docker compose config` для `docker-compose.yaml`.

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
