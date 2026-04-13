"""Базовый контракт sink-компонентов."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict

from confluent_kafka import Message


class Sink(ABC):
    """Интерфейс sink-а, куда пишутся успешно обработанные сообщения."""

    @abstractmethod
    def write_processed_message(
        self,
        msg: Message,
        key_obj: Dict[str, Any],
        value_obj: Dict[str, Any],
    ) -> None:
        """Записывает одно сообщение в целевое хранилище."""

    @abstractmethod
    def close(self) -> None:
        """Корректно завершает sink и освобождает ресурсы."""

