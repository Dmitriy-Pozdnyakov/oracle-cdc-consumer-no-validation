# state

## Назначение
Локальные runtime-артефакты контейнеров (volume `./state:/state`).

## Типичные файлы
- `postgres_sink_stub.csv` — результат `SINK_TYPE=csv`.
- `apply_simulation.csv` — аудит apply simulation.
- `runtime_flow.md` — схема runtime-потоков (Markdown + Mermaid), поддерживается в актуальном виде через `.codex`-процесс.
- локальные `.log` файлы/временные артефакты.

## Важно
- Папка используется рантаймом и может быстро расти.
- Runtime-артефакты (`*.csv`, `*.log`) обычно не коммитятся, но `runtime_flow.md` поддерживается как документ проекта.
