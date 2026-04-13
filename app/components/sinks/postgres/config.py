"""Конфигурация и валидация параметров Postgres sink."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List


@dataclass
class PostgresSinkSettings:
    """Нормализованные настройки Postgres sink-а."""

    host: str
    port: int
    database: str
    user: str
    password: str
    schema: str
    table: str
    sslmode: str
    connect_timeout_sec: int
    application_name: str
    auto_create_table: bool


def postgres_settings_from_app_config(cfg: Any) -> PostgresSinkSettings:
    """Строит `PostgresSinkSettings` из общего `Config` приложения."""
    return PostgresSinkSettings(
        host=cfg.postgres_host,
        port=cfg.postgres_port,
        database=cfg.postgres_database,
        user=cfg.postgres_user,
        password=cfg.postgres_password,
        schema=cfg.postgres_schema,
        table=cfg.postgres_table,
        sslmode=cfg.postgres_sslmode,
        connect_timeout_sec=cfg.postgres_connect_timeout_sec,
        application_name=cfg.postgres_application_name,
        auto_create_table=cfg.postgres_auto_create_table,
    )


def validate_postgres_settings(settings: PostgresSinkSettings) -> None:
    """Fail-fast проверка обязательных настроек Postgres sink-а."""
    missing: List[str] = []
    if not settings.host:
        missing.append("POSTGRES_HOST")
    if not settings.database:
        missing.append("POSTGRES_DATABASE")
    if not settings.user:
        missing.append("POSTGRES_USER")
    if not settings.password:
        missing.append("POSTGRES_PASSWORD")
    if not settings.schema:
        missing.append("POSTGRES_SCHEMA")
    if not settings.table:
        missing.append("POSTGRES_TABLE")

    if missing:
        raise RuntimeError(
            "Missing required Postgres env vars for SINK_TYPE=postgres: " + ", ".join(missing)
        )

    if settings.port <= 0:
        raise RuntimeError("POSTGRES_PORT must be > 0")
    if settings.connect_timeout_sec <= 0:
        raise RuntimeError("POSTGRES_CONNECT_TIMEOUT_SEC must be > 0")

