# Oracle CDC Consumer (No Validation)

Отдельный изолированный проект consumer-а для сообщений, которые публикует
`oracle-producer-archivelog-sr-no-validation` (ключ и value как plain JSON bytes).

Ограничения этого проекта:
- только `topic-per-table` (подписка через `TOPIC_REGEX`);
- только `oneshot` режим (повторный запуск делает оркестратор);
- manual commit (`enable.auto.commit=false`);
- поддержка `BAD_MESSAGE_POLICY`: `strict` (default), `skip`, `dlq`.

## Структура

- `app/consumer.py` — CLI entrypoint;
- `app/config.py` — env-конфиг и валидация;
- `app/utils/consumer_runner.py` — orchestration one-shot цикла;
- `app/utils/kafka_clients.py` — фабрика Kafka Consumer/Producer;
- `app/utils/cdc_message_parser.py` — parse + валидация CDC envelope;
- `app/utils/dlq.py` — публикация в DLQ;
- `app/utils/logger.py` — единый формат логов;
- `app/utils/postgres_sink_stub.py` — заглушка записи в Postgres (через CSV);
- `env/consumer.env.example` — пример env;
- `docker-compose.yaml` — запуск контейнера.

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

## Как работает oneshot

Consumer завершает работу, когда выполняется одно из условий:
- обработано `MAX_MESSAGES` успешных сообщений;
- подряд получено `MAX_EMPTY_POLLS` пустых `poll`.

Если `POSTGRES_STUB_ENABLED=true`, то перед commit offset
каждое успешно обработанное сообщение записывается в CSV-файл
`POSTGRES_STUB_CSV_PATH` (по умолчанию `/state/postgres_sink_stub.csv`).
Это имитирует запись в Postgres.

Этого достаточно для периодического запуска через:
- cron/systemd timer;
- Kubernetes Job/CronJob;
- любой внешний scheduler.

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
