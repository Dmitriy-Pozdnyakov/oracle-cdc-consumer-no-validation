"""Конфигурационный слой one-shot CDC consumer (режим topic-per-table).

Что находится в этом модуле:
1) Dataclass `Config` со всеми runtime-параметрами.
2) Загрузка параметров из env (`load_config_from_env`).
3) Fail-fast валидация обязательных и критичных значений (`validate_config`).

Зачем вынесено отдельно:
- централизовать все env-параметры в одном месте;
- упростить сопровождение и поиск ошибок конфигурации;
- держать бизнес-логику consumer-а отдельно от конфиг-логики.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List


@dataclass
class Config:
    # =========================
    # Kafka connection
    # =========================
    kafka_broker: str
    kafka_group_id: str
    kafka_client_id: str
    kafka_security_protocol: str
    ssl_cafile: str
    ssl_check_hostname: bool
    kafka_sasl_mechanism: str
    kafka_sasl_username: str
    kafka_sasl_password: str

    # =========================
    # Topic subscription
    # =========================
    topic_regex: str
    auto_offset_reset: str

    # =========================
    # One-shot runtime / sink
    # =========================
    poll_timeout_sec: float
    max_messages: int
    max_empty_polls: int
    sink_type: str
    csv_sink_path: str

    # =========================
    # Postgres sink
    # =========================
    postgres_host: str
    postgres_port: int
    postgres_database: str
    postgres_user: str
    postgres_password: str
    postgres_schema: str
    postgres_table: str
    postgres_sslmode: str
    postgres_connect_timeout_sec: int
    postgres_application_name: str
    postgres_auto_create_table: bool

    # =========================
    # Apply simulation (stage -> main)
    # =========================
    apply_mode: str
    apply_batch_size: int
    apply_max_rows: int
    apply_simulation_csv_path: str

    # =========================
    # Bad message policy
    # =========================
    bad_message_policy: str
    dlq_topic: str
    dlq_flush_timeout_sec: int

    # =========================
    # Logging
    # =========================
    verbose: bool


def _str_to_bool(raw: str, default: bool) -> bool:
    """Нормализует строковый env-параметр в bool.

    Поддерживаемые true-значения:
    - 1, true, yes, on
    Поддерживаемые false-значения:
    - 0, false, no, off

    Если значение не распознано, возвращаем `default`.
    """
    value = str(raw).strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return default


def load_config_from_env() -> Config:
    """Собирает `Config` из переменных окружения с безопасными дефолтами."""
    broker = os.getenv("KAFKA_BROKER", "").strip()
    if not broker:
        # Legacy alias для совместимости со старыми env-файлами.
        broker = os.getenv("BROKER", "").strip()

    csv_sink_path = os.getenv("CSV_SINK_PATH", "").strip()
    if not csv_sink_path:
        # Backward compatibility: читаем старое имя переменной.
        csv_sink_path = os.getenv("POSTGRES_STUB_CSV_PATH", "/state/postgres_sink_stub.csv").strip()

    return Config(
        kafka_broker=broker,
        kafka_group_id=os.getenv("KAFKA_GROUP_ID", "oracle-cdc-consumer-no-validation").strip(),
        kafka_client_id=os.getenv("KAFKA_CLIENT_ID", "oracle-cdc-consumer-no-validation").strip(),
        kafka_security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").strip().upper(),
        ssl_cafile=os.getenv("SSL_CAFILE", "").strip(),
        ssl_check_hostname=_str_to_bool(os.getenv("SSL_CHECK_HOSTNAME", "true"), True),
        kafka_sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "PLAIN").strip(),
        kafka_sasl_username=os.getenv("KAFKA_SASL_USERNAME", "").strip(),
        kafka_sasl_password=os.getenv("KAFKA_SASL_PASSWORD", "").strip(),
        topic_regex=os.getenv("TOPIC_REGEX", r"^oracle\.cdc\..+\..+$").strip(),
        auto_offset_reset=os.getenv("AUTO_OFFSET_RESET", "earliest").strip().lower(),
        poll_timeout_sec=float(os.getenv("POLL_TIMEOUT_SEC", "1.0")),
        max_messages=int(os.getenv("MAX_MESSAGES", "500")),
        max_empty_polls=int(os.getenv("MAX_EMPTY_POLLS", "15")),
        sink_type=os.getenv("SINK_TYPE", "csv").strip().lower(),
        csv_sink_path=csv_sink_path,
        postgres_host=os.getenv("POSTGRES_HOST", "").strip(),
        postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
        postgres_database=os.getenv("POSTGRES_DATABASE", "").strip(),
        postgres_user=os.getenv("POSTGRES_USER", "").strip(),
        postgres_password=os.getenv("POSTGRES_PASSWORD", "").strip(),
        postgres_schema=os.getenv("POSTGRES_SCHEMA", "public").strip(),
        postgres_table=os.getenv("POSTGRES_TABLE", "cdc_events").strip(),
        postgres_sslmode=os.getenv("POSTGRES_SSLMODE", "prefer").strip(),
        postgres_connect_timeout_sec=int(os.getenv("POSTGRES_CONNECT_TIMEOUT_SEC", "10")),
        postgres_application_name=os.getenv(
            "POSTGRES_APPLICATION_NAME",
            "oracle-cdc-consumer-no-validation",
        ).strip(),
        postgres_auto_create_table=_str_to_bool(os.getenv("POSTGRES_AUTO_CREATE_TABLE", "true"), True),
        apply_mode=os.getenv("APPLY_MODE", "simulate").strip().lower(),
        apply_batch_size=int(os.getenv("APPLY_BATCH_SIZE", "200")),
        apply_max_rows=int(os.getenv("APPLY_MAX_ROWS", "5000")),
        apply_simulation_csv_path=os.getenv("APPLY_SIMULATION_CSV_PATH", "/state/apply_simulation.csv").strip(),
        bad_message_policy=os.getenv("BAD_MESSAGE_POLICY", "strict").strip().lower(),
        dlq_topic=os.getenv("DLQ_TOPIC", "").strip(),
        dlq_flush_timeout_sec=int(os.getenv("DLQ_FLUSH_TIMEOUT_SEC", "10")),
        verbose=_str_to_bool(os.getenv("VERBOSE", "true"), True),
    )


def validate_config(cfg: Config) -> None:
    """Проверяет обязательные поля и валидность критичных опций.

    Принцип: fail-fast перед запуском runtime-цикла.
    Это позволяет поймать ошибку конфигурации до старта чтения Kafka.
    """
    missing: List[str] = []
    if not cfg.kafka_broker:
        missing.append("KAFKA_BROKER or BROKER")
    if not cfg.kafka_group_id:
        missing.append("KAFKA_GROUP_ID")
    if not cfg.topic_regex:
        missing.append("TOPIC_REGEX")

    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

    # Мы используем regex subscription, поэтому ожидаем явный якорь начала.
    if not cfg.topic_regex.startswith("^"):
        raise RuntimeError("TOPIC_REGEX must start with '^' (regex subscription)")

    if cfg.auto_offset_reset not in {"earliest", "latest"}:
        raise RuntimeError("AUTO_OFFSET_RESET must be one of: earliest, latest")

    if cfg.kafka_security_protocol not in {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}:
        raise RuntimeError("KAFKA_SECURITY_PROTOCOL must be one of: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL")

    if cfg.poll_timeout_sec <= 0:
        raise RuntimeError("POLL_TIMEOUT_SEC must be > 0")
    if cfg.max_messages <= 0:
        raise RuntimeError("MAX_MESSAGES must be > 0")
    if cfg.max_empty_polls <= 0:
        raise RuntimeError("MAX_EMPTY_POLLS must be > 0")
    if cfg.sink_type not in {"csv", "postgres"}:
        raise RuntimeError("SINK_TYPE must be one of: csv, postgres")

    if cfg.sink_type == "csv" and not cfg.csv_sink_path:
        raise RuntimeError("CSV_SINK_PATH is required when SINK_TYPE=csv")

    if cfg.sink_type == "postgres":
        # Валидация Postgres-полей вынесена в отдельный sink subcomponent.
        try:
            from .components.sinks.postgres.config import (
                postgres_settings_from_app_config,
                validate_postgres_settings,
            )
        except ImportError:  # pragma: no cover
            from components.sinks.postgres.config import (
                postgres_settings_from_app_config,
                validate_postgres_settings,
            )
        validate_postgres_settings(postgres_settings_from_app_config(cfg))

    if cfg.apply_mode not in {"simulate"}:
        raise RuntimeError("APPLY_MODE must be: simulate")
    if cfg.apply_batch_size <= 0:
        raise RuntimeError("APPLY_BATCH_SIZE must be > 0")
    if cfg.apply_max_rows <= 0:
        raise RuntimeError("APPLY_MAX_ROWS must be > 0")
    if cfg.apply_mode == "simulate" and not cfg.apply_simulation_csv_path:
        raise RuntimeError("APPLY_SIMULATION_CSV_PATH is required when APPLY_MODE=simulate")

    if cfg.bad_message_policy not in {"strict", "skip", "dlq"}:
        raise RuntimeError("BAD_MESSAGE_POLICY must be one of: strict, skip, dlq")

    if cfg.bad_message_policy == "dlq" and not cfg.dlq_topic:
        raise RuntimeError("DLQ_TOPIC is required when BAD_MESSAGE_POLICY=dlq")

    if cfg.dlq_flush_timeout_sec <= 0:
        raise RuntimeError("DLQ_FLUSH_TIMEOUT_SEC must be > 0")
