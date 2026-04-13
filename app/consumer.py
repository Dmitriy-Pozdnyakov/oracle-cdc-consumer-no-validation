#!/usr/bin/env python3
"""CLI-точка входа для CDC consumer в режиме одноразового запуска.

Архитектурная заметка:
- вся рабочая логика вынесена в `app/components/*`;
- этот файл оставлен тонкой точкой входа.
"""

from __future__ import annotations

import sys

try:
    from .config import load_config_from_env, validate_config
    from .components.consumer_runner import OneShotConsumerRunner
except ImportError:  # pragma: no cover
    from config import load_config_from_env, validate_config
    from components.consumer_runner import OneShotConsumerRunner


def main() -> int:
    """Загружает конфиг, запускает один батч, возвращает код завершения."""
    # 1) Поднимаем конфиг и fail-fast валидируем обязательные параметры.
    cfg = load_config_from_env()
    validate_config(cfg)

    # 2) Передаем управление orchestration-классу.
    runner = OneShotConsumerRunner(cfg)
    try:
        stats = runner.run_once()
        if cfg.verbose:
            print(f"[oracle-cdc-consumer] done: {stats}")
        return 0
    except Exception as exc:
        print(f"[oracle-cdc-consumer] ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
