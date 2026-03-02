"""Live stats collector and reporter."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

from crawler.storage.base import AbstractStorage

logger = logging.getLogger(__name__)


class StatsCollector:
    """Collects and periodically reports crawl statistics.

    Maintains in-memory counters for real-time speed, and periodically
    queries storage for durable aggregates.
    """

    def __init__(
        self,
        storage: AbstractStorage,
        report_interval: float = 30.0,
    ):
        self._storage = storage
        self._report_interval = report_interval

        # In-memory fast counters
        self._urls_crawled = 0
        self._urls_success = 0
        self._urls_error = 0
        self._urls_discovered = 0
        self._robots_blocked = 0

        self._start_time = time.monotonic()
        self._report_task: Optional[asyncio.Task] = None

    def record_crawl(self, success: bool) -> None:
        self._urls_crawled += 1
        if success:
            self._urls_success += 1
        else:
            self._urls_error += 1

    def record_discovered(self, count: int) -> None:
        self._urls_discovered += count

    def record_robots_blocked(self) -> None:
        self._robots_blocked += 1

    async def start(self) -> None:
        self._start_time = time.monotonic()
        self._report_task = asyncio.create_task(self._report_loop())

    async def stop(self) -> None:
        if self._report_task:
            self._report_task.cancel()
            try:
                await self._report_task
            except asyncio.CancelledError:
                pass
        await self._print_report(final=True)

    async def _report_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._report_interval)
                await self._print_report()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("Stats report error: %s", e)

    async def _print_report(self, final: bool = False) -> None:
        elapsed = time.monotonic() - self._start_time
        hours = int(elapsed // 3600)
        minutes = int((elapsed % 3600) // 60)
        seconds = int(elapsed % 60)

        qps = self._urls_crawled / elapsed if elapsed > 0 else 0.0

        # Query durable storage for accurate counts
        try:
            db_stats = await self._storage.get_stats()
        except Exception:
            db_stats = {}

        header = "═══ FINAL STATS ═══" if final else "─── CRAWL STATS ───"

        report = (
            f"\n{header}\n"
            f"  Elapsed:         {hours:02d}:{minutes:02d}:{seconds:02d}\n"
            f"  Crawled (mem):   {self._urls_crawled:,}\n"
            f"  Success (mem):   {self._urls_success:,}\n"
            f"  Errors (mem):    {self._urls_error:,}\n"
            f"  Discovered:      {self._urls_discovered:,}\n"
            f"  Robots blocked:  {self._robots_blocked:,}\n"
            f"  QPS (overall):   {qps:.2f}\n"
        )

        if db_stats:
            report += (
                f"  DB total:        {db_stats.get('total_crawled', 0):,}\n"
                f"  DB success:      {db_stats.get('successful', 0):,}\n"
                f"  DB domains:      {db_stats.get('unique_domains', 0):,}\n"
            )

        report += f"{'═' * 25 if final else '─' * 25}"
        logger.info(report)

    @property
    def summary(self) -> dict:
        elapsed = time.monotonic() - self._start_time
        return {
            "crawled": self._urls_crawled,
            "success": self._urls_success,
            "errors": self._urls_error,
            "discovered": self._urls_discovered,
            "robots_blocked": self._robots_blocked,
            "elapsed_seconds": elapsed,
            "qps": self._urls_crawled / elapsed if elapsed > 0 else 0.0,
        }
