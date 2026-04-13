# app/components/sinks

## Назначение
Слой записи обработанных сообщений в целевые хранилища (sink abstraction).

## Ключевые файлы
- `base.py` — контракт sink (`write_processed_message`, `close`).
- `factory.py` — выбор sink по `SINK_TYPE`.
- `csv_sink.py` — CSV sink (имитация записи в БД).
- `postgres/` — Postgres sink и apply-часть.

## Как добавить новый sink
1. Реализовать класс по контракту `base.py`.
2. Подключить его в `factory.py`.
3. Добавить env-настройки и валидацию в `app/config.py`.
4. Обновить `README.md` проекта и `components.md`.

