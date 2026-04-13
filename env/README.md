# env

## Назначение
Файлы окружения для запуска сервисов consumer/apply.

## Файлы
- `consumer.env.example` — шаблон для first-run.
- `consumer.env` — локальный рабочий env (с секретами, не коммитить).

## Практика
- Сначала копировать шаблон: `cp consumer.env.example consumer.env`.
- Для контура `stage -> apply` выставлять `SINK_TYPE=postgres` и заполнять `POSTGRES_*`.
- Для локальной проверки без БД можно использовать `SINK_TYPE=csv`.

