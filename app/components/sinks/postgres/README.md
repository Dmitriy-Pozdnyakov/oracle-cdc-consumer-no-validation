# app/components/sinks/postgres

## Назначение
Postgres-подкомпонент для двухшагового контура:
- ingest: запись CDC-событий в stage-таблицу;
- apply: чтение stage и simulation применения (`upsert`/`hard_delete`).

## Ключевые файлы
- `config.py` — настройки Postgres sink.
- `schema.py` — создание/миграция stage-таблицы и индексов.
- `sink.py` — ingest-запись в stage (`ON CONFLICT DO NOTHING`).
- `repository.py` — SQL-операции apply-контура (`claim`, `mark_applied`, `mark_error`, `count_new`).
- `audit_writer.py` — CSV-аудит apply simulation.
- `apply_simulator.py` — orchestration apply simulation (без raw SQL и без CSV-IO деталей).

## Инварианты
- Идемпотентность ingest по ключу `(kafka_topic, kafka_partition, kafka_offset)`.
- Apply работает по статусам `apply_status` (`new`, `processing`, `applied_simulated`, `error`).
- Порядок применения опирается на `(topic, partition, offset)`.
