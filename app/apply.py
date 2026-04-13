#!/usr/bin/env python3
"""CLI-точка входа для one-shot apply (stage -> main simulation)."""

from __future__ import annotations

from app.config import load_config_from_env, validate_config
from app.components.apply_runner import OneShotApplyRunner
from app.entrypoints.common import run_oneshot_entrypoint


def main() -> int:
    """Загружает конфиг, запускает apply oneshot и возвращает код завершения."""
    return run_oneshot_entrypoint(
        load_config=load_config_from_env,
        validate_config=validate_config,
        build_runner=OneShotApplyRunner,
        log_prefix="oracle-cdc-apply",
    )


if __name__ == "__main__":
    raise SystemExit(main())
