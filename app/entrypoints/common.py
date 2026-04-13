"""Общий bootstrap-хелпер для one-shot CLI entrypoint-ов."""

from __future__ import annotations

import sys
from typing import Any, Callable


def run_oneshot_entrypoint(
    *,
    load_config: Callable[[], Any],
    validate_config: Callable[[Any], None],
    build_runner: Callable[[Any], Any],
    log_prefix: str,
) -> int:
    """Выполняет общий шаблон запуска one-shot раннера.

    Шаблон един для `consumer.py` и `apply.py`:
    1) загрузить и валидировать конфиг;
    2) создать раннер;
    3) вызвать `run_once`;
    4) вернуть корректный код выхода.
    """
    cfg = load_config()
    validate_config(cfg)

    runner = build_runner(cfg)
    try:
        stats = runner.run_once()
        if getattr(cfg, "verbose", False):
            print(f"[{log_prefix}] done: {stats}")
        return 0
    except Exception as exc:
        print(f"[{log_prefix}] ERROR: {exc}", file=sys.stderr)
        return 1
