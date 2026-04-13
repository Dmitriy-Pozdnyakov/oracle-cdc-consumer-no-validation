"""Реальный apply-исполнитель для stage -> main в Postgres.

Задача компонента:
- взять одну stage-запись CDC;
- преобразовать ее в SQL-действие (`upsert` или `hard_delete`);
- применить действие к целевой таблице Postgres.

Текущие правила:
- PK берется из `key_json`;
- данные для `upsert` берутся из `after_json`;
- `delete` строится по PK из `key_json`;
- целевая таблица определяется как `<schema>.<source_table>`,
  где schema — `APPLY_TARGET_SCHEMA` (если задан), иначе `source_schema`.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

try:
    from .config import PostgresSinkSettings
except ImportError:  # pragma: no cover
    from config import PostgresSinkSettings


class PostgresRealApplier:
    """Применяет CDC-изменения в реальные таблицы Postgres."""

    def __init__(
        self,
        settings: PostgresSinkSettings,
        target_schema_override: str,
    ) -> None:
        self.settings = settings
        self.target_schema_override = target_schema_override.strip()
        self._conn: Optional[psycopg.Connection] = None

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
            application_name=f"{self.settings.application_name}-real-apply",
        )
        conn.autocommit = False
        self._conn = conn
        return conn

    @staticmethod
    def _normalize_sql_value(value: Any) -> Any:
        """Нормализует Python-значение для передачи в SQL.

        Если в payload есть вложенный dict/list, передаем его как JSONB.
        """
        if isinstance(value, (dict, list)):
            return Jsonb(value)
        return value

    @staticmethod
    def _extract_pk_values(row: Dict[str, Any]) -> Dict[str, Any]:
        """Извлекает PK-значения из `key_json`."""
        key_json = row.get("key_json")
        if not isinstance(key_json, dict) or not key_json:
            raise RuntimeError("real apply requires non-empty key_json for PK matching")
        return key_json

    @staticmethod
    def _extract_after_values(row: Dict[str, Any]) -> Dict[str, Any]:
        """Извлекает значения строки для upsert из `after_json`."""
        after_json = row.get("after_json")
        if not isinstance(after_json, dict) or not after_json:
            raise RuntimeError("real upsert requires non-empty after_json payload")
        return after_json

    def _resolve_target(self, row: Dict[str, Any]) -> Tuple[str, str]:
        """Определяет целевую таблицу по данным stage-строки."""
        source_schema = str(row.get("source_schema") or "").strip()
        source_table = str(row.get("source_table") or "").strip()
        if not source_table:
            raise RuntimeError("real apply requires non-empty source_table")

        target_schema = self.target_schema_override or source_schema
        if not target_schema:
            raise RuntimeError("real apply requires source_schema or APPLY_TARGET_SCHEMA")

        return target_schema, source_table

    def _upsert(self, row: Dict[str, Any]) -> None:
        """Выполняет INSERT ... ON CONFLICT DO UPDATE для stage-строки."""
        target_schema, target_table = self._resolve_target(row)
        pk_values = self._extract_pk_values(row)
        after_values = self._extract_after_values(row)

        # На insert всегда включаем PK-поля (если их нет в after_json, дополняем).
        payload: Dict[str, Any] = dict(after_values)
        for key, value in pk_values.items():
            payload.setdefault(key, value)

        columns = list(payload.keys())
        pk_columns = list(pk_values.keys())
        values = [self._normalize_sql_value(payload[col]) for col in columns]

        insert_cols_sql = sql.SQL(", ").join(sql.Identifier(col) for col in columns)
        placeholders_sql = sql.SQL(", ").join(sql.SQL("%s") for _ in columns)
        conflict_cols_sql = sql.SQL(", ").join(sql.Identifier(col) for col in pk_columns)

        update_columns = [col for col in columns if col not in pk_columns]
        if update_columns:
            set_sql = sql.SQL(", ").join(
                sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col))
                for col in update_columns
            )
            on_conflict_sql = sql.SQL("DO UPDATE SET {}").format(set_sql)
        else:
            # Если в payload только PK-поля, upsert сводится к "insert or keep existing".
            on_conflict_sql = sql.SQL("DO NOTHING")

        query = sql.SQL(
            """
            INSERT INTO {}.{} ({})
            VALUES ({})
            ON CONFLICT ({}) {}
            """
        ).format(
            sql.Identifier(target_schema),
            sql.Identifier(target_table),
            insert_cols_sql,
            placeholders_sql,
            conflict_cols_sql,
            on_conflict_sql,
        )

        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(query, values)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def _hard_delete(self, row: Dict[str, Any]) -> None:
        """Выполняет DELETE по PK из `key_json`."""
        target_schema, target_table = self._resolve_target(row)
        pk_values = self._extract_pk_values(row)

        where_sql = sql.SQL(" AND ").join(
            sql.SQL("{} = %s").format(sql.Identifier(col)) for col in pk_values.keys()
        )
        query = sql.SQL("DELETE FROM {}.{} WHERE {}").format(
            sql.Identifier(target_schema),
            sql.Identifier(target_table),
            where_sql,
        )

        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(query, list(pk_values.values()))
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def apply_row(self, row: Dict[str, Any]) -> str:
        """Применяет одну stage-строку в target-таблицу.

        Возвращает строковое действие (`upsert` или `hard_delete`),
        которое потом фиксируется в stage (`apply_action`).
        """
        op = str(row.get("op") or "").strip().lower()
        if op == "d":
            self._hard_delete(row)
            return "hard_delete"
        if op in {"c", "u"}:
            self._upsert(row)
            return "upsert"
        raise RuntimeError(f"unsupported op for real apply: {op}")

    def close(self) -> None:
        """Закрывает соединение с Postgres."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
