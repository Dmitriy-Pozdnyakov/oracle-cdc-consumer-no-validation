#!/usr/bin/env python3
"""CLI-точка входа для one-shot apply (stage -> main simulation)."""

from __future__ import annotations

import sys

try:
    from .config import load_config_from_env, validate_config
    from .components.apply_runner import OneShotApplyRunner
except ImportError:  # pragma: no cover
    from config import load_config_from_env, validate_config
    from components.apply_runner import OneShotApplyRunner


def main() -> int:
    """Загружает конфиг, запускает apply oneshot и возвращает код завершения."""
    cfg = load_config_from_env()
    validate_config(cfg)

    runner = OneShotApplyRunner(cfg)
    try:
        stats = runner.run_once()
        if cfg.verbose:
            print(f"[oracle-cdc-apply] done: {stats}")
        return 0
    except Exception as exc:
        print(f"[oracle-cdc-apply] ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

