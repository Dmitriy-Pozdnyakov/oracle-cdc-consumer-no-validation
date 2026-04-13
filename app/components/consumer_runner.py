"""Класс оркестрации для одноразового цикла чтения Kafka."""

from __future__ import annotations

from typing import Any, Dict

from confluent_kafka import Consumer, KafkaError, Message

from app.config import Config
from app.components.cdc_message_parser import CdcMessageParser
from app.components.dlq import DlqPublisher
from app.components.kafka_clients import KafkaClientFactory
from app.components.logger import AppLogger
from app.components.sinks.factory import create_sink
from app.components.stats import ConsumerBatchStats


class OneShotConsumerRunner:
    """Оркестрирует полный one-shot поток: poll -> parse -> sink -> policy -> commit.

    Ключевая цель раннера:
    - прочитать ограниченный батч сообщений;
    - корректно применить политику ошибок (`strict/skip/dlq`);
    - коммитить offset только после успешной обработки.
    """

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.logger = AppLogger(cfg)
        self.client_factory = KafkaClientFactory(cfg)
        self.parser = CdcMessageParser()
        self.sink = create_sink(cfg, self.logger)

    def _init_stats(self) -> ConsumerBatchStats:
        """Инициализирует счетчики текущего one-shot запуска.

        Лимиты (`max_messages`, `max_empty_polls`) копируются в статистику,
        чтобы итоговый отчет всегда содержал исходные runtime-параметры.
        """
        return ConsumerBatchStats(
            max_messages=self.cfg.sink.max_messages,
            max_empty_polls=self.cfg.sink.max_empty_polls,
        )

    def _log_batch_start(self) -> None:
        """Логирует старт батча с ключевыми параметрами выполнения.

        Сообщение помогает быстро понять:
        - по какой regex-подписке читаем;
        - какие лимиты завершения;
        - какая активна bad-message policy.
        """
        self.logger.info(
            "start oneshot batch "
            f"(topic_regex={self.cfg.kafka.topic_regex}, group_id={self.cfg.kafka.group_id}, "
            f"max_messages={self.cfg.sink.max_messages}, max_empty_polls={self.cfg.sink.max_empty_polls}, "
            f"bad_policy={self.cfg.dlq.bad_message_policy})"
        )

    @staticmethod
    def _is_kafka_system_message(msg: Message) -> bool:
        """Определяет, является ли объект `Message` системным событием Kafka.

        Для таких событий payload обычно отсутствует, а логика обработки иная,
        чем у обычных data-сообщений.
        """
        return bool(msg.error())

    def _handle_poll_result(self, msg: Message | None, stats: ConsumerBatchStats) -> bool:
        """Обрабатывает результат poll.

        Возвращает `True`, если основной цикл должен сразу перейти к следующему `poll`,
        и `False`, если пришло нормальное data-сообщение для дальнейшей обработки.
        """
        if msg is None:
            # Пустой poll — штатный случай при отсутствии данных.
            # Счетчик нужен для мягкого завершения одноразового запуска.
            stats.empty_polls += 1
            return True

        if self._is_kafka_system_message(msg):
            if msg.error().code() == KafkaError._PARTITION_EOF:
                return True
            self.logger.info(f"kafka message error: {msg.error()}")
            return True

        stats.polled += 1
        # Реальное сообщение пришло — сбрасываем idle-счетчик.
        stats.empty_polls = 0
        return False

    def _log_processed_message(self, msg: Message, value_obj: Dict[str, Any]) -> None:
        """Логирует метаданные успешно распарсенного CDC-сообщения.

        Лог содержит минимум, нужный для трассировки:
        topic/partition/offset, тип операции, таблица и commit_scn.
        """
        source = value_obj.get("source", {})
        self.logger.info(
            "processed "
            f"topic={msg.topic()} partition={msg.partition()} offset={msg.offset()} "
            f"op={value_obj.get('op')} "
            f"table={source.get('schema')}.{source.get('table')} "
            f"commit_scn={source.get('commit_scn')}"
        )

    @staticmethod
    def _build_bad_message_error_text(msg: Message, exc: Exception) -> str:
        """Формирует стандартизированный текст ошибки для bad-message сценария.

        Единый формат нужен, чтобы проще искать инциденты в логах и DLQ payload.
        """
        return (
            f"bad message topic={msg.topic()} partition={msg.partition()} "
            f"offset={msg.offset()} error={exc}"
        )

    def _handle_bad_message(
        self,
        consumer: Consumer,
        msg: Message,
        exc: Exception,
        dlq_publisher: DlqPublisher,
        stats: ConsumerBatchStats,
    ) -> None:
        """Применяет `BAD_MESSAGE_POLICY` к проблемному сообщению.

        Ветка поведения:
        - `skip`: warning + commit;
        - `dlq`: publish в DLQ + commit;
        - `strict`: исключение без commit (fail-fast).
        """
        stats.bad_messages += 1
        error_text = self._build_bad_message_error_text(msg, exc)

        if self.cfg.dlq.bad_message_policy == "skip":
            # skip: фиксируем warning, коммитим offset и идем дальше.
            self.logger.warning(f"{error_text} (policy=skip)")
            consumer.commit(message=msg, asynchronous=False)
            stats.committed += 1
            stats.skipped_bad_messages += 1
            return

        if self.cfg.dlq.bad_message_policy == "dlq":
            # dlq: сохраняем оригинал проблемного сообщения и продолжаем поток.
            dlq_publisher.publish(msg, str(exc))
            consumer.commit(message=msg, asynchronous=False)
            stats.committed += 1
            stats.dlq_sent += 1
            return

        # strict: fail-fast без commit, чтобы сообщение перечиталось при следующем запуске.
        raise RuntimeError(error_text) from exc

    def _process_data_message(
        self,
        consumer: Consumer,
        msg: Message,
        dlq_publisher: DlqPublisher,
        stats: ConsumerBatchStats,
    ) -> None:
        """Обрабатывает одно data-сообщение end-to-end.

        Последовательность:
        1) parse и validate сообщения;
        2) запись в sink;
        3) commit offset при успехе;
        4) при ошибке — делегирование в policy handler.
        """
        try:
            key_obj, value_obj = self.parser.parse_message(msg)
            self._log_processed_message(msg, value_obj)

            # Бизнес-действие sink:
            # - csv: имитация записи в Postgres через CSV;
            # - postgres: реальная запись в Postgres.
            self.sink.write_processed_message(msg, key_obj, value_obj)

            # Коммит offset только после успешной обработки.
            # Это поддерживает at-least-once модель доставки.
            consumer.commit(message=msg, asynchronous=False)
            stats.committed += 1
            stats.processed += 1
        except Exception as exc:
            self._handle_bad_message(
                consumer=consumer,
                msg=msg,
                exc=exc,
                dlq_publisher=dlq_publisher,
                stats=stats,
            )

    def run_once(self) -> Dict[str, Any]:
        """Запускает один bounded consume-batch и возвращает статистику.

        Цикл завершается при достижении любого лимита:
        - `MAX_MESSAGES` успешно обработанных сообщений;
        - `MAX_EMPTY_POLLS` подряд пустых poll.
        """
        consumer = self.client_factory.build_consumer()
        dlq_producer = self.client_factory.build_dlq_producer()
        dlq_publisher = DlqPublisher(self.cfg, dlq_producer, self.logger)
        stats = self._init_stats()

        self._log_batch_start()

        consumer.subscribe([self.cfg.kafka.topic_regex])

        try:
            while stats.processed < self.cfg.sink.max_messages and stats.empty_polls < self.cfg.sink.max_empty_polls:
                msg = consumer.poll(self.cfg.sink.poll_timeout_sec)
                if self._handle_poll_result(msg, stats):
                    continue

                self._process_data_message(
                    consumer=consumer,
                    msg=msg,
                    dlq_publisher=dlq_publisher,
                    stats=stats,
                )

            result = stats.as_dict()
            self.logger.info(f"oneshot batch finished: {result}")
            return result
        finally:
            try:
                # Закрываем consumer всегда, чтобы корректно освободить group membership.
                consumer.close()
            finally:
                self.sink.close()
                # Отдельно завершаем DLQ producer (если он был создан).
                dlq_publisher.flush_on_shutdown()
