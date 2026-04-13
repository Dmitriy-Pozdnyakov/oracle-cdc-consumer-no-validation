# app/components/sinks/postgres

## Назначение
Postgres-подкомпонент для двухшагового контура:
- ingest: запись CDC-событий в stage-таблицу;
- apply: чтение stage и simulation применения (`upsert`/`hard_delete`).

## Ключевые файлы
- `config.py` — настройки Postgres sink.
- `schema.py` — создание/миграция stage-таблицы и индексов.
- `sink.py` — ingest-запись в stage (`ON CONFLICT DO NOTHING`).
- `apply_simulator.py` — claim/mark/apply simulation + CSV-аудит.

## Инварианты
- Идемпотентность ingest по ключу `(kafka_topic, kafka_partition, kafka_offset)`.
- Apply работает по статусам `apply_status` (`new`, `processing`, `applied_simulated`, `error`).
- Порядок применения опирается на `(topic, partition, offset)`.

