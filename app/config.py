"""Конфигурационный слой one-shot CDC consumer (режим topic-per-table).

Что находится в этом модуле:
1) Доменные dataclass-секции конфигурации (Kafka/Sink/Postgres/Apply/DLQ/Logging).
2) Корневой `Config`, агрегирующий секции.
3) Загрузка параметров из env (`load_config_from_env`).
4) Fail-fast валидация обязательных и критичных значений (`validate_config`).

Почему так:
- проще читать и поддерживать конфиг по доменам;
- легче эволюционировать секции независимо;
- единый доступ к параметрам через секции `cfg.<domain>.<field>`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List


@dataclass
class KafkaConfig:
    """Секция Kafka подключения и подписки."""

    broker: str
    group_id: str
    client_id: str
    security_protocol: str
    ssl_cafile: str
    ssl_check_hostname: bool
    sasl_mechanism: str
    sasl_username: str
    sasl_password: str
    topic_regex: str
    auto_offset_reset: str


@dataclass
class SinkConfig:
    """Секция sink-поведения ingest-контура."""

    sink_type: str
    csv_sink_path: str
    poll_timeout_sec: float
    max_messages: int
    max_empty_polls: int


@dataclass
class PostgresConfig:
    """Секция Postgres подключения и stage-таблицы."""

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


@dataclass
class ApplyConfig:
    """Секция one-shot apply (simulate/real).

    Поля:
    - `mode`: режим применения (`simulate` или `real`);
    - `batch_size`/`max_rows`: лимиты one-shot запуска;
    - `simulation_csv_path`: CSV-аудит действий apply;
    - `target_schema`: override схемы target-таблиц для `real` режима.
    - `pk_constraint_prefix`: префикс имени PK-constraint для автопоиска
      (шаблон `<prefix><schema>_<table>`), по умолчанию `cdc_pkey_`.
    """

    mode: str
    batch_size: int
    max_rows: int
    simulation_csv_path: str
    target_schema: str
    pk_constraint_prefix: str


@dataclass
class DlqConfig:
    """Секция bad-message policy и DLQ параметров."""

    bad_message_policy: str
    topic: str
    flush_timeout_sec: int


@dataclass
class LoggingConfig:
    """Секция параметров логирования."""

    verbose: bool


@dataclass
class Config:
    """Корневой агрегатор доменных секций конфигурации.

    Внутри runtime-кода используем только секционный доступ:
    `cfg.kafka.*`, `cfg.sink.*`, `cfg.postgres.*`, `cfg.apply.*`,
    `cfg.dlq.*`, `cfg.logging.*`.
    """

    kafka: KafkaConfig
    sink: SinkConfig
    postgres: PostgresConfig
    apply: ApplyConfig
    dlq: DlqConfig
    logging: LoggingConfig


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

    kafka = KafkaConfig(
        broker=broker,
        group_id=os.getenv("KAFKA_GROUP_ID", "oracle-cdc-consumer-no-validation").strip(),
        client_id=os.getenv("KAFKA_CLIENT_ID", "oracle-cdc-consumer-no-validation").strip(),
        security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").strip().upper(),
        ssl_cafile=os.getenv("SSL_CAFILE", "").strip(),
        ssl_check_hostname=_str_to_bool(os.getenv("SSL_CHECK_HOSTNAME", "true"), True),
        sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "PLAIN").strip(),
        sasl_username=os.getenv("KAFKA_SASL_USERNAME", "").strip(),
        sasl_password=os.getenv("KAFKA_SASL_PASSWORD", "").strip(),
        topic_regex=os.getenv("TOPIC_REGEX", r"^oracle\.cdc\..+\..+$").strip(),
        auto_offset_reset=os.getenv("AUTO_OFFSET_RESET", "earliest").strip().lower(),
    )

    sink = SinkConfig(
        sink_type=os.getenv("SINK_TYPE", "csv").strip().lower(),
        csv_sink_path=csv_sink_path,
        poll_timeout_sec=float(os.getenv("POLL_TIMEOUT_SEC", "1.0")),
        max_messages=int(os.getenv("MAX_MESSAGES", "500")),
        max_empty_polls=int(os.getenv("MAX_EMPTY_POLLS", "15")),
    )

    postgres = PostgresConfig(
        host=os.getenv("POSTGRES_HOST", "").strip(),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DATABASE", "").strip(),
        user=os.getenv("POSTGRES_USER", "").strip(),
        password=os.getenv("POSTGRES_PASSWORD", "").strip(),
        schema=os.getenv("POSTGRES_SCHEMA", "public").strip(),
        table=os.getenv("POSTGRES_TABLE", "cdc_events").strip(),
        sslmode=os.getenv("POSTGRES_SSLMODE", "prefer").strip(),
        connect_timeout_sec=int(os.getenv("POSTGRES_CONNECT_TIMEOUT_SEC", "10")),
        application_name=os.getenv(
            "POSTGRES_APPLICATION_NAME",
            "oracle-cdc-consumer-no-validation",
        ).strip(),
        auto_create_table=_str_to_bool(os.getenv("POSTGRES_AUTO_CREATE_TABLE", "true"), True),
    )

    apply = ApplyConfig(
        mode=os.getenv("APPLY_MODE", "simulate").strip().lower(),
        batch_size=int(os.getenv("APPLY_BATCH_SIZE", "200")),
        max_rows=int(os.getenv("APPLY_MAX_ROWS", "5000")),
        simulation_csv_path=os.getenv("APPLY_SIMULATION_CSV_PATH", "/state/apply_simulation.csv").strip(),
        target_schema=os.getenv("APPLY_TARGET_SCHEMA", "").strip(),
        pk_constraint_prefix=os.getenv("APPLY_PK_CONSTRAINT_PREFIX", "cdc_pkey_").strip(),
    )

    dlq = DlqConfig(
        bad_message_policy=os.getenv("BAD_MESSAGE_POLICY", "strict").strip().lower(),
        topic=os.getenv("DLQ_TOPIC", "").strip(),
        flush_timeout_sec=int(os.getenv("DLQ_FLUSH_TIMEOUT_SEC", "10")),
    )

    logging = LoggingConfig(
        verbose=_str_to_bool(os.getenv("VERBOSE", "true"), True),
    )

    return Config(
        kafka=kafka,
        sink=sink,
        postgres=postgres,
        apply=apply,
        dlq=dlq,
        logging=logging,
    )


def validate_config(cfg: Config) -> None:
    """Проверяет обязательные поля и валидность критичных опций.

    Принцип: fail-fast перед запуском runtime-цикла.
    Это позволяет поймать ошибку конфигурации до старта чтения Kafka.
    """
    missing: List[str] = []
    if not cfg.kafka.broker:
        missing.append("KAFKA_BROKER or BROKER")
    if not cfg.kafka.group_id:
        missing.append("KAFKA_GROUP_ID")
    if not cfg.kafka.topic_regex:
        missing.append("TOPIC_REGEX")

    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

    # Мы используем regex subscription, поэтому ожидаем явный якорь начала.
    if not cfg.kafka.topic_regex.startswith("^"):
        raise RuntimeError("TOPIC_REGEX must start with '^' (regex subscription)")

    if cfg.kafka.auto_offset_reset not in {"earliest", "latest"}:
        raise RuntimeError("AUTO_OFFSET_RESET must be one of: earliest, latest")

    if cfg.kafka.security_protocol not in {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}:
        raise RuntimeError("KAFKA_SECURITY_PROTOCOL must be one of: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL")

    if cfg.sink.poll_timeout_sec <= 0:
        raise RuntimeError("POLL_TIMEOUT_SEC must be > 0")
    if cfg.sink.max_messages <= 0:
        raise RuntimeError("MAX_MESSAGES must be > 0")
    if cfg.sink.max_empty_polls <= 0:
        raise RuntimeError("MAX_EMPTY_POLLS must be > 0")
    if cfg.sink.sink_type not in {"csv", "postgres"}:
        raise RuntimeError("SINK_TYPE must be one of: csv, postgres")

    if cfg.sink.sink_type == "csv" and not cfg.sink.csv_sink_path:
        raise RuntimeError("CSV_SINK_PATH is required when SINK_TYPE=csv")

    if cfg.sink.sink_type == "postgres":
        # Валидация Postgres-полей вынесена в отдельный sink subcomponent.
        from app.components.sinks.postgres.config import (
            postgres_settings_from_app_config,
            validate_postgres_settings,
        )
        validate_postgres_settings(postgres_settings_from_app_config(cfg))

    if cfg.apply.mode not in {"simulate", "real"}:
        raise RuntimeError("APPLY_MODE must be one of: simulate, real")
    if cfg.apply.batch_size <= 0:
        raise RuntimeError("APPLY_BATCH_SIZE must be > 0")
    if cfg.apply.max_rows <= 0:
        raise RuntimeError("APPLY_MAX_ROWS must be > 0")
    if not cfg.apply.simulation_csv_path:
        raise RuntimeError("APPLY_SIMULATION_CSV_PATH is required")
    if not cfg.apply.pk_constraint_prefix:
        raise RuntimeError("APPLY_PK_CONSTRAINT_PREFIX is required for strict real apply mode")

    if cfg.dlq.bad_message_policy not in {"strict", "skip", "dlq"}:
        raise RuntimeError("BAD_MESSAGE_POLICY must be one of: strict, skip, dlq")

    if cfg.dlq.bad_message_policy == "dlq" and not cfg.dlq.topic:
        raise RuntimeError("DLQ_TOPIC is required when BAD_MESSAGE_POLICY=dlq")

    if cfg.dlq.flush_timeout_sec <= 0:
        raise RuntimeError("DLQ_FLUSH_TIMEOUT_SEC must be > 0")
