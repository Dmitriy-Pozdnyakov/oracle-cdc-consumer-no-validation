"""Реальный Postgres sink для записи обработанных CDC-сообщений в БД."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import psycopg
from psycopg import sql
from confluent_kafka import Message

from app.components.sinks.base import Sink
from app.components.sinks.postgres.config import PostgresSinkSettings
from app.components.sinks.postgres.schema import PostgresSchemaManager


class PostgresSink(Sink):
    """Пишет обработанные сообщения в таблицу Postgres.

    Ключевые свойства реализации:
    - соединение открывается лениво при первом write;
    - таблица может создаваться автоматически (`POSTGRES_AUTO_CREATE_TABLE=true`);
    - запись idempotent по ключу `(kafka_topic, kafka_partition, kafka_offset)`.
    """

    def __init__(self, settings: PostgresSinkSettings) -> None:
        self.settings = settings
        self._conn: Optional[psycopg.Connection] = None
        self._schema_manager = PostgresSchemaManager(settings)

    def _connect(self) -> psycopg.Connection:
        """Открывает соединение к Postgres (или возвращает уже открытое)."""
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
            application_name=self.settings.application_name,
        )
        conn.autocommit = False
        self._conn = conn

        if self.settings.auto_create_table:
            self._schema_manager.ensure_table(conn)

        return conn

    @staticmethod
    def _json_dump(payload: Dict[str, Any]) -> str:
        """Сериализует dict в JSON-строку для JSONB колонок."""
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

    def write_processed_message(
        self,
        msg: Message,
        key_obj: Dict[str, Any],
        value_obj: Dict[str, Any],
    ) -> None:
        """Записывает одно сообщение в Postgres.

        Важный момент:
        - при конфликте по `(kafka_topic, kafka_partition, kafka_offset)` запись пропускается (`DO NOTHING`);
        - это полезно при at-least-once доставке и повторных чтениях.
        """
        conn = self._connect()
        source = value_obj.get("source", {}) if isinstance(value_obj.get("source"), dict) else {}

        insert_sql = sql.SQL(
            """
            INSERT INTO {}.{} (
                kafka_topic,
                kafka_partition,
                kafka_offset,
                processed_at_utc,
                op,
                source_schema,
                source_table,
                commit_scn,
                key_json,
                value_json
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb
            )
            ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING
            """
        ).format(
            sql.Identifier(self.settings.schema),
            sql.Identifier(self.settings.table),
        )

        values = (
            msg.topic(),
            msg.partition(),
            msg.offset(),
            datetime.now(timezone.utc),
            value_obj.get("op"),
            source.get("schema"),
            source.get("table"),
            source.get("commit_scn"),
            self._json_dump(key_obj),
            self._json_dump(value_obj),
        )

        try:
            with conn.cursor() as cur:
                cur.execute(insert_sql, values)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def close(self) -> None:
        """Закрывает соединение с БД (если оно было открыто)."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
