"""Конфигурация и валидация параметров Postgres sink."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    from app.config import Config


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
    apply_target_schema: str


def postgres_settings_from_app_config(cfg: Config) -> PostgresSinkSettings:
    """Строит `PostgresSinkSettings` из общего `Config` приложения.

    Ожидает секционный формат доступа через `cfg.postgres.*`.
    """
    pg = cfg.postgres
    return PostgresSinkSettings(
        host=pg.host,
        port=pg.port,
        database=pg.database,
        user=pg.user,
        password=pg.password,
        schema=pg.schema,
        table=pg.table,
        sslmode=pg.sslmode,
        connect_timeout_sec=pg.connect_timeout_sec,
        application_name=pg.application_name,
        auto_create_table=pg.auto_create_table,
        apply_target_schema=cfg.apply.target_schema,
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
