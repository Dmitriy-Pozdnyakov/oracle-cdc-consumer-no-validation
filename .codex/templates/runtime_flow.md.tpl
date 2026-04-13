# Runtime Flow

_Обновлено: <YYYY-MM-DD>_

## Контекст
- `ingest`: `consumer.py` (oneshot)
- `apply`: `apply.py` (oneshot, `simulate|real`)
- `sink_type`: `csv | postgres`
- `bad_message_policy`: `strict | skip | dlq`

## Схема 1. Ingest (consumer.py)

Комментарий:
- offset коммитится только после успешной записи в sink;
- если sink упал, процесс завершается без commit текущего offset.

```mermaid
flowchart LR
  C0["Старт consumer.py"] --> C1["Загрузка и валидация env"]
  C1 --> C2["Подписка по TOPIC_REGEX"]
  C2 --> C3["poll()"]
  C3 --> C4{"Сообщение получено?"}
  C4 -->|нет| C5["empty_polls += 1"]
  C5 --> C6{"empty_polls >= MAX_EMPTY_POLLS?"}
  C6 -->|да| C7["Завершение oneshot"]
  C6 -->|нет| C3

  C4 -->|да| C8["Парсинг CDC key/value"]
  C8 --> C9{"Парсинг успешен?"}

  C9 -->|нет| C10{"BAD_MESSAGE_POLICY"}
  C10 -->|strict| C11["Ошибка и stop (без commit)"]
  C10 -->|skip| C12["warning + commit offset"]
  C10 -->|dlq| C13["публикация в DLQ + commit offset"]
  C12 --> C14["processed += 1"]
  C13 --> C14
  C14 --> C15{"processed >= MAX_MESSAGES?"}
  C15 -->|да| C7
  C15 -->|нет| C3

  C9 -->|да| C16{"SINK_TYPE"}
  C16 -->|csv| C17["Запись в CSV sink"]
  C16 -->|postgres| C18["Запись в Postgres stage"]
  C17 --> C19{"sink ok?"}
  C18 --> C19
  C19 -->|нет| C20["Ошибка и stop (без commit)"]
  C19 -->|да| C21["commit offset"]
  C21 --> C14
```

## Схема 2. Apply (apply.py)

Комментарий:
- apply читает только stage-строки со статусом `new`;
- `APPLY_MODE=simulate`: только симуляция действий;
- `APPLY_MODE=real`: реальный upsert/delete в target-таблицы;
- на ошибке строка маркируется как `error`, offset Kafka не участвует.

```mermaid
flowchart LR
  A0["Старт apply.py"] --> A1["Загрузка и валидация env"]
  A1 --> A2{"SINK_TYPE = postgres?"}
  A2 -->|нет| A3["Ошибка конфигурации и stop"]
  A2 -->|да| A4{"APPLY_MODE"}
  A4 -->|simulate| A5["Claim батча: new -> processing"]
  A4 -->|real| A5
  A5 --> A6{"Батч пуст?"}
  A6 -->|да| A7["Завершение oneshot"]
  A6 -->|нет| A8["Обработка строк батча"]

  A8 --> A9{"op"}
  A9 -->|c/u| A10{"режим apply"}
  A9 -->|d| A11{"режим apply"}
  A10 -->|simulate| A12["Симуляция upsert"]
  A10 -->|real| A13["Реальный upsert в target-table"]
  A11 -->|simulate| A14["Симуляция hard delete"]
  A11 -->|real| A15["Реальный delete в target-table"]
  A12 --> A16["Запись аудита в CSV"]
  A13 --> A16
  A14 --> A16
  A15 --> A16
  A16 --> A17["stage: applied_simulated | applied_real"]

  A8 --> A18["Ошибка строки"]
  A18 --> A19["stage: error + retry_count+1"]

  A17 --> A20{"Достигнут APPLY_MAX_ROWS?"}
  A19 --> A20
  A20 -->|да| A7
  A20 -->|нет| A5
```

## Короткая легенда
- `new` / `processing` / `applied_simulated` / `applied_real` / `error` — статусы строк в stage.
- `commit offset` есть только в ingest-контуре consumer.
- apply-контур работает по данным из stage, а не по offset Kafka.

## Примечания
- Файл поддерживается через процесс Codex, а не runtime-кодом сервиса.
- При изменении пайплайна обновлять вместе с `README.md`, `components.md`, `CHANGELOG.md`.
