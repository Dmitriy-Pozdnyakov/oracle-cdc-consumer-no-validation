"""DDL-утилиты Postgres sink-а."""

from __future__ import annotations

import psycopg
from psycopg import sql

from app.components.sinks.postgres.config import PostgresSinkSettings


class PostgresSchemaManager:
    """Управляет созданием schema/table для Postgres sink-а."""

    def __init__(self, settings: PostgresSinkSettings) -> None:
        self.settings = settings

    def ensure_table(self, conn: psycopg.Connection) -> None:
        """Создает/мигрирует stage-таблицу для ingest+apply пайплайна."""
        schema_ident = sql.Identifier(self.settings.schema)
        table_ident = sql.Identifier(self.settings.table)

        create_schema_sql = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(schema_ident)
        create_table_sql = sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {}.{} (
                kafka_topic TEXT NOT NULL,
                kafka_partition INTEGER NOT NULL,
                kafka_offset BIGINT NOT NULL,
                processed_at_utc TIMESTAMPTZ NOT NULL,
                op TEXT NOT NULL,
                source_schema TEXT,
                source_table TEXT,
                target_schema TEXT,
                target_table TEXT,
                commit_scn BIGINT,
                key_json JSONB NOT NULL,
                value_json JSONB NOT NULL,
                apply_status TEXT NOT NULL DEFAULT 'new',
                apply_action TEXT,
                apply_error_text TEXT,
                apply_retry_count INTEGER NOT NULL DEFAULT 0,
                apply_started_at_utc TIMESTAMPTZ,
                apply_finished_at_utc TIMESTAMPTZ,
                PRIMARY KEY (kafka_topic, kafka_partition, kafka_offset)
            )
            """
        ).format(schema_ident, table_ident)

        ensure_apply_status_sql = sql.SQL(
            "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS apply_status TEXT NOT NULL DEFAULT 'new'"
        ).format(schema_ident, table_ident)
        ensure_target_schema_sql = sql.SQL(
            "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS target_schema TEXT"
        ).format(schema_ident, table_ident)
        ensure_target_table_sql = sql.SQL(
            "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS target_table TEXT"
        ).format(schema_ident, table_ident)
        ensure_apply_action_sql = sql.SQL(
            "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS apply_action TEXT"
        ).format(schema_ident, table_ident)
        ensure_apply_error_text_sql = sql.SQL(
            "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS apply_error_text TEXT"
        ).format(schema_ident, table_ident)
        ensure_apply_retry_count_sql = sql.SQL(
            "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS apply_retry_count INTEGER NOT NULL DEFAULT 0"
        ).format(schema_ident, table_ident)
        ensure_apply_started_at_sql = sql.SQL(
            "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS apply_started_at_utc TIMESTAMPTZ"
        ).format(schema_ident, table_ident)
        ensure_apply_finished_at_sql = sql.SQL(
            "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS apply_finished_at_utc TIMESTAMPTZ"
        ).format(schema_ident, table_ident)
        normalize_apply_status_sql = sql.SQL(
            "UPDATE {}.{} SET apply_status = 'new' WHERE apply_status IS NULL"
        ).format(schema_ident, table_ident)

        status_index_ident = sql.Identifier(f"{self.settings.table}_apply_status_order_idx")
        source_table_index_ident = sql.Identifier(f"{self.settings.table}_source_table_idx")
        create_status_index_sql = sql.SQL(
            "CREATE INDEX IF NOT EXISTS {} ON {}.{} (apply_status, kafka_topic, kafka_partition, kafka_offset)"
        ).format(status_index_ident, schema_ident, table_ident)
        create_source_table_index_sql = sql.SQL(
            "CREATE INDEX IF NOT EXISTS {} ON {}.{} (source_schema, source_table, commit_scn)"
        ).format(source_table_index_ident, schema_ident, table_ident)

        with conn.cursor() as cur:
            cur.execute(create_schema_sql)
            cur.execute(create_table_sql)
            # Миграция для ранее созданных таблиц (до появления apply-* колонок).
            cur.execute(ensure_target_schema_sql)
            cur.execute(ensure_target_table_sql)
            cur.execute(ensure_apply_status_sql)
            cur.execute(ensure_apply_action_sql)
            cur.execute(ensure_apply_error_text_sql)
            cur.execute(ensure_apply_retry_count_sql)
            cur.execute(ensure_apply_started_at_sql)
            cur.execute(ensure_apply_finished_at_sql)
            cur.execute(normalize_apply_status_sql)
            cur.execute(create_status_index_sql)
            cur.execute(create_source_table_index_sql)
        conn.commit()
