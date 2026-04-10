"""Класс оркестрации для одноразового цикла чтения Kafka."""

from __future__ import annotations

from typing import Any, Dict

from confluent_kafka import KafkaError

try:
    from ..config import Config
except ImportError:  # pragma: no cover
    from config import Config

try:
    from .cdc_message_parser import CdcMessageParser
    from .dlq import DlqPublisher
    from .kafka_clients import KafkaClientFactory
    from .logger import AppLogger
    from .postgres_sink_stub import PostgresSinkStub
except ImportError:  # pragma: no cover
    from cdc_message_parser import CdcMessageParser
    from dlq import DlqPublisher
    from kafka_clients import KafkaClientFactory
    from logger import AppLogger
    from postgres_sink_stub import PostgresSinkStub


class OneShotConsumerRunner:
    """Оркестрирует полный поток: poll -> parse -> policy -> commit."""

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.logger = AppLogger(cfg)
        self.client_factory = KafkaClientFactory(cfg)
        self.parser = CdcMessageParser()
        self.postgres_stub = PostgresSinkStub(cfg)

    def run_once(self) -> Dict[str, Any]:
        """Запускает один consume-batch и возвращает статистику выполнения."""
        consumer = self.client_factory.build_consumer()
        dlq_producer = self.client_factory.build_dlq_producer()
        dlq_publisher = DlqPublisher(self.cfg, dlq_producer, self.logger)

        processed = 0
        committed = 0
        polled = 0
        bad_messages = 0
        skipped_bad_messages = 0
        dlq_sent = 0
        empty_polls = 0

        self.logger.info(
            "start oneshot batch "
            f"(topic_regex={self.cfg.topic_regex}, group_id={self.cfg.kafka_group_id}, "
            f"max_messages={self.cfg.max_messages}, max_empty_polls={self.cfg.max_empty_polls}, "
            f"bad_policy={self.cfg.bad_message_policy})"
        )

        consumer.subscribe([self.cfg.topic_regex])

        try:
            while processed < self.cfg.max_messages and empty_polls < self.cfg.max_empty_polls:
                msg = consumer.poll(self.cfg.poll_timeout_sec)
                if msg is None:
                    # Пустой poll — штатный случай при отсутствии данных.
                    # Счетчик нужен для мягкого завершения одноразового запуска.
                    empty_polls += 1
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    self.logger.info(f"kafka message error: {msg.error()}")
                    continue

                polled += 1
                # Реальное сообщение пришло — сбрасываем idle-счетчик.
                empty_polls = 0

                try:
                    _key_obj, value_obj = self.parser.parse_message(msg)
                    source = value_obj.get("source", {})
                    self.logger.info(
                        "processed "
                        f"topic={msg.topic()} partition={msg.partition()} offset={msg.offset()} "
                        f"op={value_obj.get('op')} "
                        f"table={source.get('schema')}.{source.get('table')} "
                        f"commit_scn={source.get('commit_scn')}"
                    )

                    # Имитация бизнес-действия "запись в Postgres":
                    # при включенной заглушке сохраняем сообщение в CSV.
                    if self.cfg.postgres_stub_enabled:
                        self.postgres_stub.write_processed_message(msg, _key_obj, value_obj)

                    # Коммит offset только после успешной обработки.
                    # Это поддерживает at-least-once модель доставки.
                    consumer.commit(message=msg, asynchronous=False)
                    committed += 1
                    processed += 1
                except Exception as exc:
                    bad_messages += 1
                    error_text = (
                        f"bad message topic={msg.topic()} partition={msg.partition()} "
                        f"offset={msg.offset()} error={exc}"
                    )

                    if self.cfg.bad_message_policy == "skip":
                        # skip: фиксируем warning, коммитим offset и идем дальше.
                        self.logger.warning(f"{error_text} (policy=skip)")
                        consumer.commit(message=msg, asynchronous=False)
                        committed += 1
                        skipped_bad_messages += 1
                        continue

                    if self.cfg.bad_message_policy == "dlq":
                        # dlq: сохраняем оригинал проблемного сообщения и продолжаем поток.
                        dlq_publisher.publish(msg, str(exc))
                        consumer.commit(message=msg, asynchronous=False)
                        committed += 1
                        dlq_sent += 1
                        continue

                    # strict: fail-fast без commit, чтобы сообщение перечиталось при следующем запуске.
                    raise RuntimeError(error_text) from exc

            result = {
                "processed": processed,
                "committed": committed,
                "polled": polled,
                "bad_messages": bad_messages,
                "skipped_bad_messages": skipped_bad_messages,
                "dlq_sent": dlq_sent,
                "empty_polls": empty_polls,
                "max_messages": self.cfg.max_messages,
                "max_empty_polls": self.cfg.max_empty_polls,
            }
            self.logger.info(f"oneshot batch finished: {result}")
            return result
        finally:
            try:
                # Закрываем consumer всегда, чтобы корректно освободить group membership.
                consumer.close()
            finally:
                # Отдельно завершаем DLQ producer (если он был создан).
                dlq_publisher.flush_on_shutdown()
