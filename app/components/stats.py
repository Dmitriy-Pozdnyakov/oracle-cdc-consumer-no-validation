"""Структуры статистики для one-shot batch/run циклов."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Dict


@dataclass
class ConsumerBatchStats:
    """Накопительные счетчики one-shot consume-batch цикла."""

    processed: int = 0
    committed: int = 0
    polled: int = 0
    bad_messages: int = 0
    skipped_bad_messages: int = 0
    dlq_sent: int = 0
    empty_polls: int = 0
    max_messages: int = 0
    max_empty_polls: int = 0

    def as_dict(self) -> Dict[str, int]:
        """Возвращает сериализуемое представление статистики."""
        return asdict(self)


@dataclass
class ApplyBatchStats:
    """Накопительные счетчики one-shot apply simulation цикла."""

    processed: int = 0
    applied: int = 0
    upserted: int = 0
    hard_deleted: int = 0
    errors: int = 0
    batches: int = 0
    remaining_new: int = 0
    max_rows: int = 0
    batch_size: int = 0

    def as_dict(self) -> Dict[str, int]:
        """Возвращает сериализуемое представление статистики."""
        return asdict(self)

