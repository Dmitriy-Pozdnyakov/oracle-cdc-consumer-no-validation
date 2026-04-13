"""Оркестратор one-shot apply шага (stage -> main, simulate|real)."""

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
    """Координирует one-shot apply для stage-таблицы Postgres.

    Раннер не содержит SQL/IO деталей:
    - проверяет корректность runtime-режима,
    - запускает orchestration-слой `PostgresApplySimulator`,
    - гарантирует корректное закрытие ресурсов.
    """

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.logger = AppLogger(cfg, prefix="oracle-cdc-apply")
        self._settings = postgres_settings_from_app_config(cfg)
        self._simulator = PostgresApplySimulator(
            settings=self._settings,
            logger=self.logger,
            apply_mode=cfg.apply.mode,
            simulation_csv_path=cfg.apply.simulation_csv_path,
            target_schema_override=cfg.apply.target_schema,
            batch_size=cfg.apply.batch_size,
            max_rows=cfg.apply.max_rows,
        )

    def _validate_runtime_mode(self) -> None:
        """Проверяет совместимость runtime-режима перед запуском apply.

        Инварианты:
        - apply работает только когда ingest использует `SINK_TYPE=postgres`;
        - поддержаны режимы `APPLY_MODE=simulate|real`.
        """
        if self.cfg.sink.sink_type != "postgres":
            raise RuntimeError("Apply runner requires SINK_TYPE=postgres (stage table is in Postgres)")
        if self.cfg.apply.mode not in {"simulate", "real"}:
            raise RuntimeError("Only APPLY_MODE=simulate|real is supported now")

    def run_once(self) -> Dict[str, Any]:
        """Выполняет один apply-batch в режиме `simulate` или `real`.

        Даже при ошибке гарантирует `close()` симулятора, чтобы освободить
        соединения и завершить процесс в предсказуемом состоянии.
        """
        self._validate_runtime_mode()

        try:
            return self._simulator.run_once()
        finally:
            self._simulator.close()
