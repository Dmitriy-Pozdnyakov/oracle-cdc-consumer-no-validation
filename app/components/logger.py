"""Утилиты логирования для выполнения consumer-а."""

from __future__ import annotations

import sys

try:
    from ..config import Config
except ImportError:  # pragma: no cover
    from config import Config


class AppLogger:
    """Небольшая обертка над print для единообразного формата логов.

    Зачем отдельный класс:
    - все модули используют одинаковый префикс;
    - правила verbose-вывода находятся в одном месте;
    - проще перейти на structured logging в будущем.
    """

    def __init__(self, cfg: Config, prefix: str = "oracle-cdc-consumer") -> None:
        self.cfg = cfg
        self.prefix = prefix

    def info(self, message: str) -> None:
        """Печатает информационное сообщение только при `VERBOSE=true`."""
        if self.cfg.logging.verbose:
            print(f"[{self.prefix}] {message}")

    def warning(self, message: str) -> None:
        """Печатает warning всегда, независимо от режима verbose."""
        print(f"[{self.prefix}] WARNING: {message}")

    def error(self, message: str) -> None:
        """Печатает ошибку всегда и направляет вывод в `stderr`."""
        print(f"[{self.prefix}] ERROR: {message}", file=sys.stderr)
