# env

## Назначение
Файлы окружения для запуска сервисов consumer/apply.

## Файлы
- `consumer.env.example` — шаблон для first-run.
- `consumer.env` — локальный рабочий env (с секретами, не коммитить).

## Практика
- Сначала копировать шаблон: `cp consumer.env.example consumer.env`.
- Для контура `stage -> apply` выставлять `SINK_TYPE=postgres` и заполнять `POSTGRES_*`.
- Для apply выбирать режим:
  - `APPLY_MODE=simulate` (без записи в main-таблицы),
  - `APPLY_MODE=real` (реальный upsert/delete в target-таблицы).
- Для локальной проверки без БД можно использовать `SINK_TYPE=csv`.
