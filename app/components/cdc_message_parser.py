"""Парсинг Kafka сообщений и валидация CDC envelope."""

from __future__ import annotations

import json
from typing import Any, Dict, Optional, Tuple

from confluent_kafka import Message


class CdcMessageParser:
    """Парсит Kafka payload и валидирует минимальный CDC-контракт.

    Парсер намеренно "строгий":
    - сначала проверяет формат (UTF-8 + JSON object),
    - затем проверяет обязательные поля CDC envelope,
    - при несоответствии сразу бросает исключение для fail-fast политики.
    """

    @staticmethod
    def _decode_json_bytes(raw: Optional[bytes], field_name: str) -> Dict[str, Any]:
        """Декодирует `bytes` в JSON-объект словаря.

        Этапы:
        1) проверка, что payload не `None`;
        2) декодирование как UTF-8;
        3) `json.loads`;
        4) проверка, что результат — именно `dict`.
        """
        if raw is None:
            raise ValueError(f"{field_name} is null")
        try:
            decoded = raw.decode("utf-8")
        except Exception as exc:
            raise ValueError(f"{field_name} is not valid UTF-8") from exc

        try:
            payload = json.loads(decoded)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{field_name} is not valid JSON ({exc})") from exc

        if not isinstance(payload, dict):
            raise ValueError(f"{field_name} JSON must be an object")
        return payload

    @staticmethod
    def _validate_cdc_envelope(value_obj: Dict[str, Any]) -> None:
        """Проверяет обязательные поля CDC envelope.

        Инварианты:
        - `op` только из набора `c/u/d`;
        - `source.schema`, `source.table` непустые строки;
        - `source.commit_scn` целое число;
        - `data` присутствует и является объектом.
        """
        # op должен быть только из базового набора DML-операций.
        op = value_obj.get("op")
        if op not in {"c", "u", "d"}:
            raise ValueError("value.op must be one of: c, u, d")

        # source обязателен: без него теряется контекст таблицы/SCN.
        source = value_obj.get("source")
        if not isinstance(source, dict):
            raise ValueError("value.source must be an object")

        for key in ("schema", "table"):
            if not isinstance(source.get(key), str) or not source.get(key):
                raise ValueError(f"value.source.{key} must be non-empty string")

        if not isinstance(source.get("commit_scn"), int):
            raise ValueError("value.source.commit_scn must be int")

        data_payload = value_obj.get("data")
        if not isinstance(data_payload, dict):
            raise ValueError("value.data must be object")
        if not data_payload:
            raise ValueError("value.data must be non-empty object")

    def parse_message(self, msg: Message) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Парсит Kafka-сообщение и возвращает `(key_obj, value_obj)`.

        Метод объединяет все этапы в одном месте:
        - decode/parse `key`;
        - decode/parse `value`;
        - валидация CDC envelope для `value`.
        """
        key_obj = self._decode_json_bytes(msg.key(), "key")
        value_obj = self._decode_json_bytes(msg.value(), "value")
        self._validate_cdc_envelope(value_obj)
        return key_obj, value_obj
