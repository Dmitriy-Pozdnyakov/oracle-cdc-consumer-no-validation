# app/components

## Назначение
Слой прикладных компонентов: orchestration, transport-утилиты, парсинг CDC, DLQ и logging.

## Ключевые файлы
- `consumer_runner.py` — основной oneshot consume-batch цикл.
- `apply_runner.py` — oneshot apply simulation цикл.
- `cdc_message_parser.py` — decode + валидация CDC envelope.
- `kafka_clients.py` — создание Kafka Consumer/Producer.
- `dlq.py` — публикация проблемных сообщений в DLQ.
- `logger.py` — единый формат логов.
- `sinks/` — sink-подкомпоненты записи.

## Границы изменений
- Новые функции добавлять отдельными компонентами.
- Не смешивать в одном модуле transport, бизнес-правила и sink SQL-логику.

