"""Оркестратор one-shot apply шага (stage -> main simulation)."""

from __future__ import annotations

from typing import Any, Dict

try:
    from ..config import Config
except ImportError:  # pragma: no cover
    from config import Config

try:
    from .logger import AppLogger
    from .sinks.postgres.apply_simulator import PostgresApplySimulator
    from .sinks.postgres.config import postgres_settings_from_app_config
except ImportError:  # pragma: no cover
    from logger import AppLogger
    from sinks.postgres.apply_simulator import PostgresApplySimulator
    from sinks.postgres.config import postgres_settings_from_app_config


class OneShotApplyRunner:
    """Координирует apply simulation для stage-таблицы Postgres."""

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.logger = AppLogger(cfg, prefix="oracle-cdc-apply")
        self._settings = postgres_settings_from_app_config(cfg)
        self._simulator = PostgresApplySimulator(
            settings=self._settings,
            logger=self.logger,
            simulation_csv_path=cfg.apply_simulation_csv_path,
            batch_size=cfg.apply_batch_size,
            max_rows=cfg.apply_max_rows,
        )

    def _validate_runtime_mode(self) -> None:
        """Проверяет совместимость runtime-режима перед запуском apply."""
        if self.cfg.sink_type != "postgres":
            raise RuntimeError("Apply runner requires SINK_TYPE=postgres (stage table is in Postgres)")
        if self.cfg.apply_mode != "simulate":
            raise RuntimeError("Only APPLY_MODE=simulate is supported now")

    def _run_simulation_once(self) -> Dict[str, Any]:
        """Запускает один apply-batch в simulation-режиме."""
        return self._simulator.run_once()

    def run_once(self) -> Dict[str, Any]:
        """Выполняет один apply-batch в simulation-режиме."""
        self._validate_runtime_mode()

        try:
            return self._run_simulation_once()
        finally:
            self._simulator.close()
