"""CSV sink: имитация записи в БД через файл."""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from confluent_kafka import Message

try:
    from .base import Sink
except ImportError:  # pragma: no cover
    from base import Sink


class CsvSink(Sink):
    """Пишет обработанные события в CSV (append-only)."""

    FIELDNAMES = [
        "processed_at_utc",
        "topic",
        "partition",
        "offset",
        "op",
        "schema",
        "table",
        "commit_scn",
        "key_json",
        "before_json",
        "after_json",
        "value_json",
    ]

    def __init__(self, csv_path: str) -> None:
        self.path = Path(csv_path)

    def _ensure_parent(self) -> None:
        """Гарантирует наличие каталога для CSV файла."""
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def _ensure_header(self) -> None:
        """Создает CSV и пишет header, если файла еще нет."""
        if self.path.exists():
            return

        self._ensure_parent()
        with self.path.open("w", encoding="utf-8", newline="") as fp:
            writer = csv.DictWriter(fp, fieldnames=self.FIELDNAMES)
            writer.writeheader()

    @staticmethod
    def _json_dump(payload: Dict[str, Any]) -> str:
        """Сериализует dict в компактный JSON для одной CSV-ячейки."""
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

    def write_processed_message(
        self,
        msg: Message,
        key_obj: Dict[str, Any],
        value_obj: Dict[str, Any],
    ) -> None:
        """Пишет одну обработанную запись в CSV."""
        source = value_obj.get("source", {}) if isinstance(value_obj.get("source"), dict) else {}
        row = {
            "processed_at_utc": datetime.now(timezone.utc).isoformat(),
            "topic": msg.topic(),
            "partition": msg.partition(),
            "offset": msg.offset(),
            "op": value_obj.get("op"),
            "schema": source.get("schema"),
            "table": source.get("table"),
            "commit_scn": source.get("commit_scn"),
            "key_json": self._json_dump(key_obj),
            "before_json": self._json_dump(value_obj.get("before") or {}),
            "after_json": self._json_dump(value_obj.get("after") or {}),
            "value_json": self._json_dump(value_obj),
        }

        self._ensure_header()
        with self.path.open("a", encoding="utf-8", newline="") as fp:
            writer = csv.DictWriter(fp, fieldnames=self.FIELDNAMES)
            writer.writerow(row)

    def close(self) -> None:
        """CSV sink не держит соединений, поэтому close no-op."""
        return

