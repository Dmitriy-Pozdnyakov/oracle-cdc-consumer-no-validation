"""Repository-слой для one-shot apply по stage таблице Postgres.

Зона ответственности:
- SQL-claim записей `new -> processing`;
- фиксация статуса `applied_simulated` или `error`;
- чтение остатка `new` записей.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

try:
    from .config import PostgresSinkSettings
    from .schema import PostgresSchemaManager
except ImportError:  # pragma: no cover
    from config import PostgresSinkSettings
    from schema import PostgresSchemaManager


class PostgresStageApplyRepository:
    """Инкапсулирует SQL-операции apply-контра по stage таблице."""

    def __init__(self, settings: PostgresSinkSettings) -> None:
        self.settings = settings
        self._conn: Optional[psycopg.Connection] = None
        self._schema_manager = PostgresSchemaManager(settings)

    def _connect(self) -> psycopg.Connection:
        """Открывает соединение к Postgres (лениво)."""
        if self._conn is not None:
            return self._conn

        conn = psycopg.connect(
            host=self.settings.host,
            port=self.settings.port,
            dbname=self.settings.database,
            user=self.settings.user,
            password=self.settings.password,
            sslmode=self.settings.sslmode,
            connect_timeout=self.settings.connect_timeout_sec,
            application_name=f"{self.settings.application_name}-apply",
        )
        conn.autocommit = False
        self._conn = conn

        if self.settings.auto_create_table:
            self._schema_manager.ensure_table(conn)

        return conn

    def claim_new_rows(self, limit: int) -> List[Dict[str, Any]]:
        """Забирает батч `new` записей и атомарно переводит их в `processing`."""
        conn = self._connect()
        claim_sql = sql.SQL(
            """
            WITH candidate AS (
                SELECT kafka_topic, kafka_partition, kafka_offset
                FROM {}.{}
                WHERE apply_status = 'new'
                ORDER BY kafka_topic, kafka_partition, kafka_offset
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            UPDATE {}.{} AS stage
            SET
                apply_status = 'processing',
                apply_started_at_utc = NOW(),
                apply_finished_at_utc = NULL,
                apply_error_text = NULL
            FROM candidate
            WHERE
                stage.kafka_topic = candidate.kafka_topic
                AND stage.kafka_partition = candidate.kafka_partition
                AND stage.kafka_offset = candidate.kafka_offset
            RETURNING
                stage.kafka_topic,
                stage.kafka_partition,
                stage.kafka_offset,
                stage.op,
                stage.source_schema,
                stage.source_table,
                stage.commit_scn,
                stage.key_json,
                stage.before_json,
                stage.after_json,
                stage.value_json
            """
        ).format(
            sql.Identifier(self.settings.schema),
            sql.Identifier(self.settings.table),
            sql.Identifier(self.settings.schema),
            sql.Identifier(self.settings.table),
        )

        try:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(claim_sql, (limit,))
                rows = cur.fetchall()
            conn.commit()
            return rows
        except Exception:
            conn.rollback()
            raise

    def mark_applied(self, row: Dict[str, Any], action: str) -> None:
        """Фиксирует успешную симуляцию apply по одной stage-записи."""
        conn = self._connect()
        update_sql = sql.SQL(
            """
            UPDATE {}.{}
            SET
                apply_status = 'applied_simulated',
                apply_action = %s,
                apply_finished_at_utc = NOW(),
                apply_error_text = NULL
            WHERE
                kafka_topic = %s
                AND kafka_partition = %s
                AND kafka_offset = %s
            """
        ).format(
            sql.Identifier(self.settings.schema),
            sql.Identifier(self.settings.table),
        )

        try:
            with conn.cursor() as cur:
                cur.execute(
                    update_sql,
                    (
                        action,
                        row["kafka_topic"],
                        row["kafka_partition"],
                        row["kafka_offset"],
                    ),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def mark_error(self, row: Dict[str, Any], error_text: str) -> None:
        """Фиксирует ошибку apply и увеличивает retry-счетчик."""
        conn = self._connect()
        update_sql = sql.SQL(
            """
            UPDATE {}.{}
            SET
                apply_status = 'error',
                apply_error_text = %s,
                apply_retry_count = apply_retry_count + 1,
                apply_finished_at_utc = NOW()
            WHERE
                kafka_topic = %s
                AND kafka_partition = %s
                AND kafka_offset = %s
            """
        ).format(
            sql.Identifier(self.settings.schema),
            sql.Identifier(self.settings.table),
        )

        try:
            with conn.cursor() as cur:
                cur.execute(
                    update_sql,
                    (
                        error_text[:8000],
                        row["kafka_topic"],
                        row["kafka_partition"],
                        row["kafka_offset"],
                    ),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def count_new_rows(self) -> int:
        """Считает количество stage-записей со статусом `new`."""
        conn = self._connect()
        count_sql = sql.SQL("SELECT COUNT(*) FROM {}.{} WHERE apply_status = 'new'").format(
            sql.Identifier(self.settings.schema),
            sql.Identifier(self.settings.table),
        )
        with conn.cursor() as cur:
            cur.execute(count_sql)
            result = cur.fetchone()
        return int(result[0]) if result else 0

    def close(self) -> None:
        """Закрывает соединение с Postgres."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
