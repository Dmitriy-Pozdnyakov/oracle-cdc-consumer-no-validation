"""CSV-аудит apply simulation для Postgres stage-контура."""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict


class ApplySimulationAuditWriter:
    """Пишет результат apply simulation в CSV-файл аудита."""

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

    def __init__(self, simulation_csv_path: str) -> None:
        self.simulation_csv_path = Path(simulation_csv_path)

    @staticmethod
    def _json_dump(payload: Any) -> str:
        """Сериализует произвольный payload в компактный JSON-текст."""
        return json.dumps(payload if payload is not None else {}, ensure_ascii=False, separators=(",", ":"))

    def _ensure_header(self) -> None:
        """Создает CSV-файл и header, если файл еще не существует."""
        if self.simulation_csv_path.exists():
            return

        self.simulation_csv_path.parent.mkdir(parents=True, exist_ok=True)
        with self.simulation_csv_path.open("w", encoding="utf-8", newline="") as fp:
            writer = csv.DictWriter(fp, fieldnames=self.FIELDNAMES)
            writer.writeheader()

    def append_action(self, row: Dict[str, Any], action: str) -> None:
        """Добавляет одну запись в CSV-аудит apply simulation."""
        self._ensure_header()
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
