"""One-shot apply-эмулятор для stage таблицы Postgres.

Модуль нужен для первого этапа внедрения:
- ingest пишет CDC события в промежуточную (stage) таблицу;
- apply-джоб читает stage и "симулирует" применение в main таблицу.

Пока применяется только simulation-режим:
- действие `upsert` для op=c/u;
- действие `hard_delete` для op=d.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

try:
    from .audit_writer import ApplySimulationAuditWriter
    from .config import PostgresSinkSettings
    from .repository import PostgresStageApplyRepository
    from ...stats import ApplyBatchStats
except ImportError:  # pragma: no cover
    from audit_writer import ApplySimulationAuditWriter
    from config import PostgresSinkSettings
    from repository import PostgresStageApplyRepository
    from stats import ApplyBatchStats


class PostgresApplySimulator:
    """Выполняет one-shot apply из stage таблицы в режиме эмуляции.

    Здесь остается orchestration-логика:
    - взять батч записей из repository;
    - определить действие apply;
    - записать аудит;
    - зафиксировать статус строки.
    """

    def __init__(
        self,
        settings: PostgresSinkSettings,
        logger: Any,
        simulation_csv_path: str,
        batch_size: int,
        max_rows: int,
        repository: Optional[PostgresStageApplyRepository] = None,
        audit_writer: Optional[ApplySimulationAuditWriter] = None,
    ) -> None:
        self.settings = settings
        self.logger = logger
        self.batch_size = batch_size
        self.max_rows = max_rows
        self._repository = repository or PostgresStageApplyRepository(settings)
        self._audit_writer = audit_writer or ApplySimulationAuditWriter(simulation_csv_path)

    @staticmethod
    def _resolve_action(op: str) -> str:
        """Преобразует CDC op в apply-действие."""
        if op == "d":
            return "hard_delete"
        if op in {"c", "u"}:
            return "upsert"
        raise RuntimeError(f"unsupported op for apply simulation: {op}")

    def _init_stats(self) -> ApplyBatchStats:
        """Инициализирует счетчики текущего one-shot apply цикла."""
        return ApplyBatchStats(
            max_rows=self.max_rows,
            batch_size=self.batch_size,
        )

    def _log_apply_start(self) -> None:
        """Логирует старт one-shot apply simulation цикла."""
        self.logger.info(
            "start apply simulation "
            f"(table={self.settings.schema}.{self.settings.table}, "
            f"batch_size={self.batch_size}, max_rows={self.max_rows}, "
            f"simulation_csv={self._audit_writer.simulation_csv_path})"
        )

    def _process_claimed_row(self, row: Dict[str, Any], stats: ApplyBatchStats) -> None:
        """Обрабатывает одну stage-запись из already-claimed батча."""
        stats.processed += 1
        try:
            action = self._resolve_action(str(row.get("op")))
            self._audit_writer.append_action(row, action)
            self._repository.mark_applied(row, action)
            stats.applied += 1
            if action == "hard_delete":
                stats.hard_deleted += 1
            else:
                stats.upserted += 1
        except Exception as exc:
            stats.errors += 1
            try:
                self._repository.mark_error(row, str(exc))
            except Exception as mark_exc:
                self.logger.error(
                    "failed to mark apply error "
                    f"topic={row.get('kafka_topic')} partition={row.get('kafka_partition')} "
                    f"offset={row.get('kafka_offset')} error={mark_exc}"
                )
                raise
            self.logger.warning(
                "apply simulation error "
                f"topic={row.get('kafka_topic')} partition={row.get('kafka_partition')} "
                f"offset={row.get('kafka_offset')} error={exc}"
            )

    def run_once(self) -> Dict[str, Any]:
        """Выполняет один apply-batch (oneshot) в simulation-режиме."""
        stats = self._init_stats()
        self._log_apply_start()

        while stats.processed < self.max_rows:
            limit = min(self.batch_size, self.max_rows - stats.processed)
            rows = self._repository.claim_new_rows(limit)
            if not rows:
                break

            stats.batches += 1
            for row in rows:
                self._process_claimed_row(row, stats)

        stats.remaining_new = self._repository.count_new_rows()
        result = stats.as_dict()
        self.logger.info(f"apply simulation finished: {result}")
        return result

    def close(self) -> None:
        """Закрывает соединение с Postgres."""
        self._repository.close()
