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
        kafka = self.cfg.kafka
        conf: Dict[str, Any] = {
            "security.protocol": kafka.security_protocol,
        }
        # SSL-параметры применяются в SSL/SASL_SSL режимах.
        if kafka.security_protocol in {"SSL", "SASL_SSL"}:
            if kafka.ssl_cafile:
                conf["ssl.ca.location"] = kafka.ssl_cafile
            conf["ssl.endpoint.identification.algorithm"] = "https" if kafka.ssl_check_hostname else "none"
        # SASL-параметры применяются в SASL_* режимах.
        if kafka.security_protocol in {"SASL_SSL", "SASL_PLAINTEXT"}:
            conf["sasl.mechanism"] = kafka.sasl_mechanism
            conf["sasl.username"] = kafka.sasl_username
            conf["sasl.password"] = kafka.sasl_password
        return conf

    def build_consumer(self) -> Consumer:
        """Создает основной consumer в режиме ручного commit offset."""
        kafka = self.cfg.kafka
        conf: Dict[str, Any] = {
            "bootstrap.servers": kafka.broker,
            "group.id": kafka.group_id,
            "client.id": kafka.client_id,
            "auto.offset.reset": kafka.auto_offset_reset,
            # Ключевой флаг: offset коммитится только явным вызовом commit().
            "enable.auto.commit": False,
        }
        conf.update(self._security_config())
        return Consumer(conf)

    def build_dlq_producer(self) -> Optional[Producer]:
        """Создает producer для DLQ только при `BAD_MESSAGE_POLICY=dlq`."""
        if self.cfg.dlq.bad_message_policy != "dlq":
            return None

        kafka = self.cfg.kafka
        conf: Dict[str, Any] = {
            "bootstrap.servers": kafka.broker,
            "client.id": f"{kafka.client_id}-dlq",
            "acks": "all",
            # Idempotence повышает надежность отправки в DLQ при ретраях.
            "enable.idempotence": True,
        }
        conf.update(self._security_config())
        return Producer(conf)
