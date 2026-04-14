# app/components/sinks/postgres

## Назначение
Postgres-подкомпонент для двухшагового контура:
- ingest: запись CDC-событий в stage-таблицу;
- apply: чтение stage и применение в режимах `simulate|real`.

## Ключевые файлы
- `config.py` — настройки Postgres sink.
- `schema.py` — создание/миграция stage-таблицы и индексов.
- `sink.py` — ingest-запись в stage (`ON CONFLICT DO NOTHING`).
- `repository.py` — SQL-операции apply-контура (`claim`, `mark_applied`, `mark_error`, `count_new`).
- `audit_writer.py` — CSV-аудит действий apply.
- `real_applier.py` — реальный `upsert/delete` в target-таблицы Postgres.
- `apply_orchestrator.py` — orchestration apply (`simulate|real`) без raw SQL.

## Инварианты
- Идемпотентность ingest по ключу `(kafka_topic, kafka_partition, kafka_offset)`.
- Apply работает по статусам `apply_status` (`new`, `processing`, `applied_simulated`, `applied_real`, `error`).
- Порядок применения опирается на `(topic, partition, offset)`.
- Для `real` apply PK может извлекаться:
  - из `key_json` (дефолт);
  - из payload по `APPLY_PK_COLUMNS`, если key не несет бизнес-PK.
