"""Фабрика Kafka-клиентов для выполнения consumer-а."""

from __future__ import annotations

from typing import Any, Dict, Optional

from confluent_kafka import Consumer, Producer

try:
    from ..config import Config
except ImportError:  # pragma: no cover
    from config import Config


class KafkaClientFactory:
    """Создает Kafka Consumer/Producer на основе общего `Config`."""

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg

    def _security_config(self) -> Dict[str, Any]:
        """Собирает общий security-конфиг для всех Kafka клиентов."""
        conf: Dict[str, Any] = {
            "security.protocol": self.cfg.kafka_security_protocol,
        }
        # SSL-параметры применяются в SSL/SASL_SSL режимах.
        if self.cfg.kafka_security_protocol in {"SSL", "SASL_SSL"}:
            if self.cfg.ssl_cafile:
                conf["ssl.ca.location"] = self.cfg.ssl_cafile
            conf["ssl.endpoint.identification.algorithm"] = "https" if self.cfg.ssl_check_hostname else "none"
        # SASL-параметры применяются в SASL_* режимах.
        if self.cfg.kafka_security_protocol in {"SASL_SSL", "SASL_PLAINTEXT"}:
            conf["sasl.mechanism"] = self.cfg.kafka_sasl_mechanism
            conf["sasl.username"] = self.cfg.kafka_sasl_username
            conf["sasl.password"] = self.cfg.kafka_sasl_password
        return conf

    def build_consumer(self) -> Consumer:
        """Создает основной consumer в режиме ручного commit offset."""
        conf: Dict[str, Any] = {
            "bootstrap.servers": self.cfg.kafka_broker,
            "group.id": self.cfg.kafka_group_id,
            "client.id": self.cfg.kafka_client_id,
            "auto.offset.reset": self.cfg.auto_offset_reset,
            # Ключевой флаг: offset коммитится только явным вызовом commit().
            "enable.auto.commit": False,
        }
        conf.update(self._security_config())
        return Consumer(conf)

    def build_dlq_producer(self) -> Optional[Producer]:
        """Создает producer для DLQ только при `BAD_MESSAGE_POLICY=dlq`."""
        if self.cfg.bad_message_policy != "dlq":
            return None

        conf: Dict[str, Any] = {
            "bootstrap.servers": self.cfg.kafka_broker,
            "client.id": f"{self.cfg.kafka_client_id}-dlq",
            "acks": "all",
            # Idempotence повышает надежность отправки в DLQ при ретраях.
            "enable.idempotence": True,
        }
        conf.update(self._security_config())
        return Producer(conf)
