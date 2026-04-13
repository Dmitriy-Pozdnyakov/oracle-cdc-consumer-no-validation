"""One-shot apply-эмулятор для stage таблицы Postgres.

Модуль нужен для первого этапа внедрения:
- ingest пишет CDC события в промежуточную (stage) таблицу;
- apply-джоб читает stage и "симулирует" применение в main таблицу.

Пока применяется только simulation-режим:
- действие `upsert` для op=c/u;
- действие `hard_delete` для op=d.
"""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
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


class PostgresApplySimulator:
    """Выполняет one-shot apply из stage таблицы в режиме эмуляции."""

    FIELDNAMES = [
        "applied_at_utc",
        "action",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "source_schema",
        "source_table",
        "commit_scn",
        "key_json",
        "before_json",
        "after_json",
        "value_json",
    ]

    def __init__(
        self,
        settings: PostgresSinkSettings,
        logger: Any,
        simulation_csv_path: str,
        batch_size: int,
        max_rows: int,
    ) -> None:
        self.settings = settings
        self.logger = logger
        self.batch_size = batch_size
        self.max_rows = max_rows
        self.simulation_csv_path = Path(simulation_csv_path)
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

        # Для v1 хотим безопасный first-run:
        # если auto_create включен, поднимем stage DDL автоматически.
        if self.settings.auto_create_table:
            self._schema_manager.ensure_table(conn)

        return conn

    @staticmethod
    def _json_dump(payload: Any) -> str:
        """Сериализует произвольный payload в компактный JSON-текст."""
        return json.dumps(payload if payload is not None else {}, ensure_ascii=False, separators=(",", ":"))

    def _ensure_simulation_header(self) -> None:
        """Создает CSV файл симуляции и пишет header, если его еще нет."""
        if self.simulation_csv_path.exists():
            return
        self.simulation_csv_path.parent.mkdir(parents=True, exist_ok=True)
        with self.simulation_csv_path.open("w", encoding="utf-8", newline="") as fp:
            writer = csv.DictWriter(fp, fieldnames=self.FIELDNAMES)
            writer.writeheader()

    @staticmethod
    def _resolve_action(op: str) -> str:
        """Преобразует CDC op в apply-действие."""
        if op == "d":
            return "hard_delete"
        if op in {"c", "u"}:
            return "upsert"
        raise RuntimeError(f"unsupported op for apply simulation: {op}")

    def _claim_batch(self, limit: int) -> List[Dict[str, Any]]:
        """Забирает батч `new` записей и помечает их как `processing`.

        Важный нюанс:
        - claim и смена статуса делаются одним SQL-оператором;
        - это упрощает защиту от параллельного apply запуска.
        """
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

    def _mark_applied(self, row: Dict[str, Any], action: str) -> None:
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

    def _mark_error(self, row: Dict[str, Any], error_text: str) -> None:
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

    def _append_simulation_row(self, row: Dict[str, Any], action: str) -> None:
        """Пишет факт симуляции apply в CSV-аудит."""
        self._ensure_simulation_header()
        csv_row = {
            "applied_at_utc": datetime.now(timezone.utc).isoformat(),
            "action": action,
            "kafka_topic": row["kafka_topic"],
            "kafka_partition": row["kafka_partition"],
            "kafka_offset": row["kafka_offset"],
            "source_schema": row["source_schema"],
            "source_table": row["source_table"],
            "commit_scn": row["commit_scn"],
            "key_json": self._json_dump(row["key_json"]),
            "before_json": self._json_dump(row["before_json"]),
            "after_json": self._json_dump(row["after_json"]),
            "value_json": self._json_dump(row["value_json"]),
        }
        with self.simulation_csv_path.open("a", encoding="utf-8", newline="") as fp:
            writer = csv.DictWriter(fp, fieldnames=self.FIELDNAMES)
            writer.writerow(csv_row)

    def _count_new_rows(self) -> int:
        """Считает остаток непримененных stage-событий (`apply_status='new'`)."""
        conn = self._connect()
        count_sql = sql.SQL(
            "SELECT COUNT(*) FROM {}.{} WHERE apply_status = 'new'"
        ).format(
            sql.Identifier(self.settings.schema),
            sql.Identifier(self.settings.table),
        )
        with conn.cursor() as cur:
            cur.execute(count_sql)
            result = cur.fetchone()
        return int(result[0]) if result else 0

    def run_once(self) -> Dict[str, Any]:
        """Выполняет один apply-batch (oneshot) в simulation-режиме."""
        processed = 0
        applied = 0
        errors = 0
        upserted = 0
        hard_deleted = 0
        batches = 0

        self.logger.info(
            "start apply simulation "
            f"(table={self.settings.schema}.{self.settings.table}, "
            f"batch_size={self.batch_size}, max_rows={self.max_rows}, "
            f"simulation_csv={self.simulation_csv_path})"
        )

        while processed < self.max_rows:
            limit = min(self.batch_size, self.max_rows - processed)
            rows = self._claim_batch(limit)
            if not rows:
                break

            batches += 1
            for row in rows:
                processed += 1
                try:
                    action = self._resolve_action(str(row.get("op")))
                    self._append_simulation_row(row, action)
                    self._mark_applied(row, action)
                    applied += 1
                    if action == "hard_delete":
                        hard_deleted += 1
                    else:
                        upserted += 1
                except Exception as exc:
                    errors += 1
                    try:
                        self._mark_error(row, str(exc))
                    except Exception as mark_exc:
                        self.logger.error(
                            "failed to mark apply error "
                            f"topic={row.get('kafka_topic')} partition={row.get('kafka_partition')} "
                            f"offset={row.get('kafka_offset')} error={mark_exc}"
                        )
                        raise
                    self.logger.warning(
                        "apply simulation error "
                        f"topic={row.get('kafka_topic')} partition={row.get('kafka_partition')} "
                        f"offset={row.get('kafka_offset')} error={exc}"
                    )

        result = {
            "processed": processed,
            "applied": applied,
            "upserted": upserted,
            "hard_deleted": hard_deleted,
            "errors": errors,
            "batches": batches,
            "remaining_new": self._count_new_rows(),
            "max_rows": self.max_rows,
            "batch_size": self.batch_size,
        }
        self.logger.info(f"apply simulation finished: {result}")
        return result

    def close(self) -> None:
        """Закрывает соединение с Postgres."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
