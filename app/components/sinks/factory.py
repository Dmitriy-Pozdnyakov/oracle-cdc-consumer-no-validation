"""Фабрика sink-компонентов."""

from __future__ import annotations

from typing import Any

from app.components.sinks.base import Sink
from app.components.sinks.csv_sink import CsvSink
from app.components.sinks.postgres.config import postgres_settings_from_app_config
from app.components.sinks.postgres.sink import PostgresSink


def create_sink(cfg: Any, logger: Any) -> Sink:
    """Создает sink в зависимости от `SINK_TYPE`."""
    if cfg.sink.sink_type == "postgres":
        pg_settings = postgres_settings_from_app_config(cfg)
        logger.info(
            "sink configured: postgres "
            f"(dsn={pg_settings.host}:{pg_settings.port}/{pg_settings.database}, "
            f"table={pg_settings.schema}.{pg_settings.table})"
        )
        return PostgresSink(pg_settings)

    logger.info(f"sink configured: csv (path={cfg.sink.csv_sink_path})")
    return CsvSink(cfg.sink.csv_sink_path)
