"""Реальный apply-исполнитель для stage -> main в Postgres.

Задача компонента:
- взять одну stage-запись CDC;
- преобразовать ее в SQL-действие (`upsert` или `hard_delete`);
- применить действие к целевой таблице Postgres.

Текущие правила:
- PK может определяться по именованному PK-constraint вида
  `<APPLY_PK_CONSTRAINT_PREFIX><schema>_<table>`;
- если constraint с таким именем не найден, применяется fallback:
  - `APPLY_PK_COLUMNS` (если задан);
  - иначе `key_json` как в базовом режиме;
- данные для `upsert` берутся из `value_json.data`;
- `delete` строится по извлеченному PK;
- целевая таблица берется из stage-полей `target_schema/target_table`;
- fallback для старых stage-строк: `APPLY_TARGET_SCHEMA` (если задан) и
  `source_schema/source_table`;
- schema/table идентификаторы приводятся к lower-case.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from app.components.sinks.postgres.config import PostgresSinkSettings


class PostgresRealApplier:
    """Применяет CDC-изменения в реальные таблицы Postgres."""

    _PG_IDENTIFIER_MAX_LEN = 63

    def __init__(
        self,
        settings: PostgresSinkSettings,
        target_schema_override: str,
        pk_columns: List[str],
        pk_constraint_prefix: str,
    ) -> None:
        self.settings = settings
        self.target_schema_override = target_schema_override.strip()
        self.pk_columns = []
        for col in pk_columns:
            normalized = self._normalize_identifier(col)
            if normalized:
                self.pk_columns.append(normalized)
        self.pk_constraint_prefix = pk_constraint_prefix.strip()
        self._conn: Optional[psycopg.Connection] = None
        self._pk_columns_cache: Dict[Tuple[str, str], Optional[List[str]]] = {}

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
    def _extract_pk_values_from_key(row: Dict[str, Any]) -> Dict[str, Any]:
        """Извлекает PK-значения из `key_json` (базовый режим)."""
        key_json = row.get("key_json")
        if not isinstance(key_json, dict) or not key_json:
            raise RuntimeError("real apply requires non-empty key_json for PK matching")
        return PostgresRealApplier._normalize_column_mapping(key_json, "key_json")

    @staticmethod
    def _build_expected_pk_constraint_name(prefix: str, target_schema: str, target_table: str) -> str:
        """Строит ожидаемое имя PK-constraint с учетом лимита PG identifier."""
        raw_name = f"{prefix}{target_schema}_{target_table}"
        return raw_name[:PostgresRealApplier._PG_IDENTIFIER_MAX_LEN]

    def _resolve_pk_columns_from_named_constraint(
        self,
        target_schema: str,
        target_table: str,
    ) -> Optional[List[str]]:
        """Возвращает PK-колонки из constraint `<prefix><schema>_<table>`.

        Возвращает `None`, если автопоиск по constraint отключен (пустой prefix)
        или constraint не найден.
        """
        if not self.pk_constraint_prefix:
            return None

        cache_key = (target_schema, target_table)
        if cache_key in self._pk_columns_cache:
            return self._pk_columns_cache[cache_key]

        expected_name = self._build_expected_pk_constraint_name(
            self.pk_constraint_prefix,
            target_schema,
            target_table,
        )

        query = """
            SELECT attr.attname
            FROM pg_constraint con
            JOIN pg_class tbl
              ON tbl.oid = con.conrelid
            JOIN pg_namespace ns
              ON ns.oid = tbl.relnamespace
            JOIN unnest(con.conkey) WITH ORDINALITY AS key_cols(attnum, ord)
              ON TRUE
            JOIN pg_attribute attr
              ON attr.attrelid = tbl.oid
             AND attr.attnum = key_cols.attnum
            WHERE con.contype = 'p'
              AND ns.nspname = %s
              AND tbl.relname = %s
              AND con.conname = %s
            ORDER BY key_cols.ord
        """

        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(query, (target_schema, target_table, expected_name))
            columns = [self._normalize_identifier(row[0]) for row in cur.fetchall()]

        resolved = columns or None
        self._pk_columns_cache[cache_key] = resolved
        return resolved

    def _extract_pk_values_by_columns(
        self,
        row: Dict[str, Any],
        pk_columns: List[str],
    ) -> Dict[str, Any]:
        """Извлекает PK-значения по заданному списку колонок.

        Приоритет источников:
        1) `key_json`;
        2) `value_json.data`.
        """
        key_json_raw = row.get("key_json")
        key_json = (
            self._normalize_column_mapping(key_json_raw, "key_json")
            if isinstance(key_json_raw, dict)
            else {}
        )
        payload = self._extract_data_values(row)

        pk_values: Dict[str, Any] = {}
        missing: List[str] = []
        for col in pk_columns:
            normalized_col = self._normalize_identifier(col)
            if not normalized_col:
                continue
            if key_json.get(normalized_col) is not None:
                pk_values[normalized_col] = key_json.get(normalized_col)
                continue
            if payload.get(normalized_col) is not None:
                pk_values[normalized_col] = payload.get(normalized_col)
                continue
            missing.append(normalized_col)

        if missing:
            raise RuntimeError(
                "real apply cannot resolve PK values by columns, missing: "
                + ", ".join(missing)
            )
        return pk_values

    def _extract_pk_values_from_payload(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Извлекает PK-значения из `value_json.data` по `APPLY_PK_COLUMNS`."""
        payload = self._extract_data_values(row)

        pk_values: Dict[str, Any] = {}
        missing: List[str] = []
        for col in self.pk_columns:
            value = payload.get(col)
            if value is None:
                missing.append(col)
                continue
            pk_values[col] = value

        if missing:
            raise RuntimeError(
                "real apply cannot extract PK from payload, missing columns: "
                + ", ".join(missing)
            )
        return pk_values

    def _extract_pk_values(self, row: Dict[str, Any], target_schema: str, target_table: str) -> Dict[str, Any]:
        """Извлекает PK-значения в зависимости от режима конфигурации.

        Порядок:
        - если найден именованный constraint `<prefix><schema>_<table>`:
          PK-колонки берутся из него;
        - если задан `APPLY_PK_COLUMNS`: PK читается из payload;
        - иначе используется `key_json`.
        """
        constraint_columns = self._resolve_pk_columns_from_named_constraint(
            target_schema=target_schema,
            target_table=target_table,
        )
        if constraint_columns:
            return self._extract_pk_values_by_columns(row, constraint_columns)

        if self.pk_columns:
            return self._extract_pk_values_from_payload(row)
        return self._extract_pk_values_from_key(row)

    @staticmethod
    def _extract_data_values(row: Dict[str, Any]) -> Dict[str, Any]:
        """Извлекает значения строки из `value_json.data`."""
        value_json = row.get("value_json")
        if not isinstance(value_json, dict):
            raise RuntimeError("real apply requires value_json object")
        data = value_json.get("data")
        if not isinstance(data, dict) or not data:
            raise RuntimeError("real apply requires non-empty value_json.data payload")
        return PostgresRealApplier._normalize_column_mapping(data, "value_json.data")

    @staticmethod
    def _normalize_identifier(value: Any) -> str:
        """Нормализует имя схемы/таблицы к lower-case."""
        raw = str(value or "").strip()
        if not raw:
            return ""
        return raw.lower()

    @staticmethod
    def _normalize_column_mapping(raw: Dict[str, Any], source: str) -> Dict[str, Any]:
        """Нормализует ключи колонок к lower-case и проверяет коллизии."""
        normalized: Dict[str, Any] = {}
        for raw_col, value in raw.items():
            normalized_col = PostgresRealApplier._normalize_identifier(raw_col)
            if not normalized_col:
                raise RuntimeError(f"real apply received empty column name in {source}")
            if normalized_col in normalized:
                raise RuntimeError(
                    f"real apply detected duplicate column after lowercase normalization in {source}: "
                    f"{normalized_col}"
                )
            normalized[normalized_col] = value
        return normalized

    def _resolve_target(self, row: Dict[str, Any]) -> Tuple[str, str]:
        """Определяет целевую таблицу по данным stage-строки."""
        target_schema = self._normalize_identifier(row.get("target_schema"))
        target_table = self._normalize_identifier(row.get("target_table"))

        if not target_table:
            target_table = self._normalize_identifier(row.get("source_table"))
        if not target_table:
            raise RuntimeError("real apply requires non-empty target_table/source_table")

        if not target_schema:
            target_schema = self._normalize_identifier(self.target_schema_override)
        if not target_schema:
            target_schema = self._normalize_identifier(row.get("source_schema"))
        if not target_schema:
            raise RuntimeError("real apply requires target_schema/source_schema/APPLY_TARGET_SCHEMA")

        return target_schema, target_table

    def _upsert(self, row: Dict[str, Any]) -> None:
        """Выполняет INSERT ... ON CONFLICT DO UPDATE для stage-строки."""
        target_schema, target_table = self._resolve_target(row)
        pk_values = self._extract_pk_values(row, target_schema, target_table)
        data_values = self._extract_data_values(row)

        # На insert всегда включаем PK-поля (если их нет в data, дополняем).
        payload: Dict[str, Any] = dict(data_values)
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
        """Выполняет DELETE по извлеченному PK."""
        target_schema, target_table = self._resolve_target(row)
        pk_values = self._extract_pk_values(row, target_schema, target_table)

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
