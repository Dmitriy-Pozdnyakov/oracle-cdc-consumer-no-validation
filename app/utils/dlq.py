"""Утилиты для работы с DLQ (dead-letter queue)."""

from __future__ import annotations

import base64
import json
import time
from typing import Any, Dict, Optional

from confluent_kafka import Message, Producer

try:
    from ..config import Config
except ImportError:  # pragma: no cover
    from config import Config

try:
    from .logger import AppLogger
except ImportError:  # pragma: no cover
    from logger import AppLogger


class DlqPublisher:
    """Публикует проблемные сообщения в отдельный DLQ topic."""

    def __init__(self, cfg: Config, producer: Optional[Producer], logger: AppLogger) -> None:
        self.cfg = cfg
        self.producer = producer
        self.logger = logger

    @staticmethod
    def _build_payload(msg: Message, error_text: str) -> Dict[str, Any]:
        """Собирает DLQ payload, сохраняя оригинальные key/value байты."""
        headers_payload: Dict[str, Optional[str]] = {}
        for key, value in (msg.headers() or []):
            # Заголовки и payload сохраняем в base64, чтобы не потерять бинарные значения.
            headers_payload[key] = None if value is None else base64.b64encode(value).decode("ascii")

        return {
            "error": error_text,
            "source_topic": msg.topic(),
            "source_partition": msg.partition(),
            "source_offset": msg.offset(),
            "source_timestamp": msg.timestamp()[1],
            "source_headers_b64": headers_payload,
            "key_b64": None if msg.key() is None else base64.b64encode(msg.key()).decode("ascii"),
            "value_b64": None if msg.value() is None else base64.b64encode(msg.value()).decode("ascii"),
            "dlq_ts_ms": int(time.time() * 1000),
        }

    def publish(self, msg: Message, error_text: str) -> None:
        """Отправляет одно проблемное сообщение в DLQ и ждет flush."""
        if self.producer is None:
            raise RuntimeError("DLQ producer is not initialized")

        payload = self._build_payload(msg, error_text)
        payload_bytes = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.producer.produce(self.cfg.dlq_topic, key=msg.key(), value=payload_bytes)
        remaining = self.producer.flush(self.cfg.dlq_flush_timeout_sec)
        if remaining > 0:
            # Если flush не опустошил очередь, считаем отправку ненадежной и падаем.
            raise RuntimeError(
                f"DLQ producer flush timeout: {remaining} messages left in queue "
                f"after {self.cfg.dlq_flush_timeout_sec}s"
            )

        self.logger.warning(
            "sent bad message to DLQ "
            f"topic={self.cfg.dlq_topic} source_topic={msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )

    def flush_on_shutdown(self) -> None:
        """Делает best-effort flush DLQ producer во время shutdown."""
        if self.producer is not None:
            self.producer.flush(self.cfg.dlq_flush_timeout_sec)
