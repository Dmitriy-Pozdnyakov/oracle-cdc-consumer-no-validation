#!/usr/bin/env python3
"""CLI-точка входа для CDC consumer в режиме одноразового запуска.

Архитектурная заметка:
- вся рабочая логика вынесена в `app/components/*`;
- этот файл оставлен тонкой точкой входа.
"""

from __future__ import annotations

from app.config import load_config_from_env, validate_config
from app.components.consumer_runner import OneShotConsumerRunner
from app.entrypoints.common import run_oneshot_entrypoint


def main() -> int:
    """Загружает конфиг, запускает один батч, возвращает код завершения."""
    return run_oneshot_entrypoint(
        load_config=load_config_from_env,
        validate_config=validate_config,
        build_runner=OneShotConsumerRunner,
        log_prefix="oracle-cdc-consumer",
    )


if __name__ == "__main__":
    raise SystemExit(main())
