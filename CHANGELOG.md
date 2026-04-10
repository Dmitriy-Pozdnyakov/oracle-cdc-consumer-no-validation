# Журнал Изменений

Формат близок к `Keep a Changelog`.

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
  - `app/utils/postgres_sink_stub.py`
  - запись обработанных сообщений в CSV вместо реальной БД.

### Изменено
- Выполнен рефакторинг в feature-scope классах:
  - `app/utils/consumer_runner.py`
  - `app/utils/kafka_clients.py`
  - `app/utils/cdc_message_parser.py`
  - `app/utils/dlq.py`
  - `app/utils/logger.py`
- `app/consumer.py` упрощен до тонкого CLI entrypoint без бизнес-логики.
- Добавлены недостающие комментарии и русские docstring в `app/consumer.py` и `app/utils/*`.
- `app/utils/consumer_runner.py` обновлен: запись в CSV-заглушку выполняется до commit offset.
- В конфиг добавлены параметры `POSTGRES_STUB_ENABLED` и `POSTGRES_STUB_CSV_PATH`.
- `docker-compose.yaml` обновлен: добавлен volume `./state:/state` для CSV-результатов.
- `env/consumer.env` и `env/consumer.env.example` синхронизированы с Docker Kafka проектом:
  - `KAFKA_BROKER=host.docker.internal:19092,19093,19094`
  - `KAFKA_SECURITY_PROTOCOL=PLAINTEXT`
  - `SSL_CHECK_HOSTNAME=false`

### Проверено
- Синтаксис Python (`py_compile`) для consumer-конфига и runtime.
- Валидность compose-конфига (`docker compose config`).
