# Oracle CDC Consumer (No Validation)

Отдельный изолированный проект consumer-а для сообщений, которые публикует
`oracle-producer-archivelog-sr-no-validation` (ключ и value как plain JSON bytes).

Ограничения этого проекта:
- только `topic-per-table` (подписка через `TOPIC_REGEX`);
- только `oneshot` режим (повторный запуск делает оркестратор);
- manual commit (`enable.auto.commit=false`);
- поддержка `BAD_MESSAGE_POLICY`: `strict` (default), `skip`, `dlq`.
- поддержка двухшагового контура `ingest -> stage -> apply(simulate|real)`.

## Структура

- `app/consumer.py` — CLI entrypoint;
- `app/apply.py` — CLI entrypoint apply-шага (`simulate|real`);
- `app/config.py` — env-конфиг и валидация;
- `app/entrypoints/common.py` — общий bootstrap helper для CLI;
- `app/components/consumer_runner.py` — orchestration one-shot цикла;
- `app/components/apply_runner.py` — orchestration one-shot apply цикла;
- `app/components/stats.py` — dataclass-модели статистики batch/run;
- `app/components/kafka_clients.py` — фабрика Kafka Consumer/Producer;
- `app/components/cdc_message_parser.py` — parse + валидация CDC envelope;
- `app/components/dlq.py` — публикация в DLQ;
- `app/components/logger.py` — единый формат логов;
- `app/components/sinks/factory.py` — выбор sink по `SINK_TYPE`;
- `app/components/sinks/csv_sink.py` — CSV sink (имитация записи в БД);
- `app/components/sinks/postgres/config.py` — Postgres-конфиг и валидация;
- `app/components/sinks/postgres/schema.py` — DDL-управление schema/table;
- `app/components/sinks/postgres/sink.py` — ingest запись в stage таблицу Postgres;
- `app/components/sinks/postgres/repository.py` — SQL-слой apply (`claim/mark/count`);
- `app/components/sinks/postgres/audit_writer.py` — CSV-аудит действий apply;
- `app/components/sinks/postgres/real_applier.py` — реальный `upsert/delete` в target-таблицы;
- `app/components/sinks/postgres/apply_orchestrator.py` — orchestration apply (`simulate|real`);
- `env/consumer.env.example` — пример env;
- `docker-compose.yaml` — запуск контейнера.

## Структура Config (в коде)

Внутри `app/config.py` конфиг разнесен по доменным секциям:
- `cfg.kafka` — Kafka connection + topic subscription;
- `cfg.sink` — oneshot runtime лимиты и тип sink;
- `cfg.postgres` — настройки stage Postgres;
- `cfg.apply` — параметры apply (`simulate|real`);
- `cfg.dlq` — bad-message policy и DLQ;
- `cfg.logging` — флаги логирования.

Для плавной миграции сохранены alias-свойства старого формата (`cfg.kafka_broker`, `cfg.apply_mode` и т.д.).

## Быстрый старт

1. Подготовить env:

```bash
cp env/consumer.env.example env/consumer.env
```

2. Проверить, что доступен CA-файл (если используете SSL/SASL_SSL):

```bash
mkdir -p certs
cp ../apache-kafka-stack/scripts/tls/ca.crt ./certs/ca.crt
```

3. Запустить oneshot consumer:

```bash
docker compose run --rm oracle-cdc-consumer-no-validation
```

4. Запустить oneshot apply:

```bash
docker compose run --rm oracle-cdc-apply-no-validation
```

## Как работает oneshot

Consumer завершает работу, когда выполняется одно из условий:
- обработано `MAX_MESSAGES` успешных сообщений;
- подряд получено `MAX_EMPTY_POLLS` пустых `poll`.

Перед commit offset выполняется sink-шаг:
- `SINK_TYPE=csv`: сообщение пишется в CSV (`CSV_SINK_PATH`, по умолчанию `/state/postgres_sink_stub.csv`);
- `SINK_TYPE=postgres`: сообщение пишется в Postgres stage-таблицу (`POSTGRES_SCHEMA.POSTGRES_TABLE`).

Коммит offset делается только после успешной записи в sink.

Этого достаточно для периодического запуска через:
- cron/systemd timer;
- Kubernetes Job/CronJob;
- любой внешний scheduler.

## Runtime Схема

- Файл схемы: `state/runtime_flow.md` (Markdown + Mermaid).
- Обновление схемы выполняется в процессе разработки по правилам `.codex` (а не в runtime-коде сервиса).

## Stage -> Apply (Hard Delete)

Рекомендуемый поток для продуктивного контура:
1. `ingest` (consumer) читает Kafka CDC и пишет события в `stage` таблицу Postgres.
2. `apply` (отдельный oneshot процесс) забирает записи со статусом `new`.
3. Режим apply выбирается через `APPLY_MODE`:
   - `simulate`: симулирует `upsert/hard_delete` без записи в main-таблицу;
   - `real`: выполняет реальный `upsert/delete` в target-таблицы Postgres.
4. Результат фиксируется:
   - в stage-статусах (`apply_status`, `apply_action`, `apply_error_text`);
   - в CSV-аудите `APPLY_SIMULATION_CSV_PATH`.

Важно:
- для этого контура обязательно выставить `SINK_TYPE=postgres`;
- и заполнить `POSTGRES_*` параметры подключения.

Статусы apply:
- `new` — запись ждёт применения;
- `processing` — запись взята в текущий apply-batch;
- `applied_simulated` — симуляция успешна;
- `applied_real` — реальное применение в target-таблицу успешно;
- `error` — применение завершилось ошибкой (увеличен `apply_retry_count`).

## Sink Конфигурация

Для `SINK_TYPE=csv`:
- `CSV_SINK_PATH` — путь к CSV файлу.

Для `SINK_TYPE=postgres` обязательны:
- `POSTGRES_HOST`
- `POSTGRES_PORT`
- `POSTGRES_DATABASE`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_SCHEMA`
- `POSTGRES_TABLE`

Опционально:
- `POSTGRES_SSLMODE` (default `prefer`)
- `POSTGRES_CONNECT_TIMEOUT_SEC` (default `10`)
- `POSTGRES_APPLICATION_NAME`
- `POSTGRES_AUTO_CREATE_TABLE` (`true|false`)

## Apply Конфигурация

- `APPLY_MODE` — `simulate` или `real`;
- `APPLY_BATCH_SIZE` — сколько stage-событий claim-ить за один SQL-батч;
- `APPLY_MAX_ROWS` — общий лимит apply-событий за один oneshot запуск;
- `APPLY_SIMULATION_CSV_PATH` — CSV-аудит действий apply;
- `APPLY_TARGET_SCHEMA` — override target schema для `real` режима (если пусто, используется `source_schema`).

## Bad Message Policy

`BAD_MESSAGE_POLICY=strict`:
- при битом/неожиданном сообщении процесс падает с ошибкой;
- offset проблемного сообщения не коммитится.

`BAD_MESSAGE_POLICY=skip`:
- сообщение логируется как warning;
- offset коммитится, consumer продолжает работу.

`BAD_MESSAGE_POLICY=dlq`:
- оригинальные key/value байты отправляются в `DLQ_TOPIC`;
- offset исходного сообщения коммитится.

## Что считается битым сообщением

- key/value не UTF-8;
- key/value не JSON-объект;
- у value нет валидного CDC envelope:
  - нет `op` (`c/u/d`);
  - нет `source` или `source.schema/source.table`;
  - `source.commit_scn` не число;
  - неконсистентные `before/after` для конкретного `op`.
