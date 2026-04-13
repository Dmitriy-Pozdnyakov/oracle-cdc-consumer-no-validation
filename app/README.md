# app

## Назначение
Пакет приложения с точками входа и runtime-компонентами CDC consumer.

## Ключевые файлы
- `consumer.py` — one-shot ingest запуск (Kafka -> sink).
- `apply.py` — one-shot apply simulation запуск (stage -> simulated apply).
- `config.py` — загрузка и fail-fast валидация env-конфига.
- `components/` — прикладные компоненты runtime.

## Границы изменений
- Бизнес-логику обработки сообщений держать в `components/`.
- Точки входа (`consumer.py`, `apply.py`) оставлять тонкими.

