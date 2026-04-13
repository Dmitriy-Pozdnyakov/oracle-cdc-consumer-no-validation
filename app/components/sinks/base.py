"""Базовый контракт sink-компонентов."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict

from confluent_kafka import Message


class Sink(ABC):
    """Базовый контракт sink-а для обработанных CDC-сообщений.

    Реализация sink-а обязана:
    - принимать уже валидированные `key/value` объекты;
    - бросать исключение при ошибке записи;
    - корректно освобождать ресурсы в `close()`.
    """

    @abstractmethod
    def write_processed_message(
        self,
        msg: Message,
        key_obj: Dict[str, Any],
        value_obj: Dict[str, Any],
    ) -> None:
        """Записывает одно сообщение в целевое хранилище.

        Важный контракт для раннера:
        - успешный возврат означает, что можно коммитить Kafka offset;
        - исключение означает, что commit делать нельзя.
        """

    @abstractmethod
    def close(self) -> None:
        """Корректно завершает sink и освобождает ресурсы.

        Метод должен быть идемпотентным: повторный вызов не должен падать.
        """
