# Runtime Flow

_Updated: <YYYY-MM-DD>_

## Context
- `ingest`: `consumer.py` (oneshot)
- `apply`: `apply.py` (oneshot, `simulate|real`)
- `sink_type`: `csv | postgres`
- `bad_message_policy`: `strict | skip | dlq`

## Diagram 1. Ingest (consumer.py)

Comment:
- offset is committed only after a successful sink write;
- if sink fails, the process stops without committing the current offset.

```mermaid
flowchart LR
  C0["Start consumer.py"] --> C1["Load and validate env"]
  C1 --> C2["Subscribe by TOPIC_REGEX"]
  C2 --> C3["poll()"]
  C3 --> C4{"Message received?"}
  C4 -->|no| C5["empty_polls += 1"]
  C5 --> C6{"empty_polls >= MAX_EMPTY_POLLS?"}
  C6 -->|yes| C7["Finish oneshot"]
  C6 -->|no| C3

  C4 -->|yes| C8["Parse CDC key/value"]
  C8 --> C9{"Parse successful?"}

  C9 -->|no| C10{"BAD_MESSAGE_POLICY"}
  C10 -->|strict| C11["Error and stop (no commit)"]
  C10 -->|skip| C12["warning + commit offset"]
  C10 -->|dlq| C13["publish to DLQ + commit offset"]
  C12 --> C14["processed += 1"]
  C13 --> C14
  C14 --> C15{"processed >= MAX_MESSAGES?"}
  C15 -->|yes| C7
  C15 -->|no| C3

  C9 -->|yes| C16{"SINK_TYPE"}
  C16 -->|csv| C17["Write to CSV sink"]
  C16 -->|postgres| C18["Write to Postgres stage"]
  C17 --> C19{"sink ok?"}
  C18 --> C19
  C19 -->|no| C20["Error and stop (no commit)"]
  C19 -->|yes| C21["commit offset"]
  C21 --> C14
```

## Diagram 2. Apply (apply.py)

Comment:
- apply reads only stage rows with status `new`;
- `APPLY_MODE=simulate`: simulate actions only;
- `APPLY_MODE=real`: real upsert/delete in target tables;
- on error, row is marked as `error`; Kafka offsets are not involved.

```mermaid
flowchart LR
  A0["Start apply.py"] --> A1["Load and validate env"]
  A1 --> A2{"SINK_TYPE = postgres?"}
  A2 -->|no| A3["Configuration error and stop"]
  A2 -->|yes| A4{"APPLY_MODE"}
  A4 -->|simulate| A5["Claim batch: new -> processing"]
  A4 -->|real| A5
  A5 --> A6{"Batch empty?"}
  A6 -->|yes| A7["Finish oneshot"]
  A6 -->|no| A8["Process batch rows"]

  A8 --> A9{"op"}
  A9 -->|c/u| A10{"apply mode"}
  A9 -->|d| A11{"apply mode"}
  A10 -->|simulate| A12["Simulate upsert"]
  A10 -->|real| A13["Real upsert to target table"]
  A11 -->|simulate| A14["Simulate hard delete"]
  A11 -->|real| A15["Real delete from target table"]
  A12 --> A16["Write audit to CSV"]
  A13 --> A16
  A14 --> A16
  A15 --> A16
  A16 --> A17["stage: applied_simulated | applied_real"]

  A8 --> A18["Row error"]
  A18 --> A19["stage: error + retry_count+1"]

  A17 --> A20{"APPLY_MAX_ROWS reached?"}
  A19 --> A20
  A20 -->|yes| A7
  A20 -->|no| A5
```

## Short Legend
- `new` / `processing` / `applied_simulated` / `applied_real` / `error` are row statuses in stage.
- `commit offset` exists only in the ingest consumer flow.
- apply flow works with stage data, not Kafka offsets.

## Notes
- This file is maintained through the Codex process, not runtime service code.
- When the pipeline changes, update together with `README.md`, `components.md`, and `CHANGELOG.md`.
