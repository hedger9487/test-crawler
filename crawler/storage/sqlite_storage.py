"""SQLite-based storage with WAL mode for concurrent access.

Stores crawl results and provides efficient aggregation queries.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Dict

import aiosqlite

from crawler.storage.base import AbstractStorage

logger = logging.getLogger(__name__)


class SQLiteStorage(AbstractStorage):
    """Async SQLite storage using WAL mode for performance."""

    def __init__(self, db_path: str = "crawl_data.db", batch_size: int = 100):
        self._db_path = db_path
        self._batch_size = batch_size
        self._db: aiosqlite.Connection | None = None

        # Write buffer for batch inserts
        self._buffer: list[tuple] = []
        self._buffer_lock = asyncio.Lock()

    async def init(self) -> None:
        self._db = await aiosqlite.connect(self._db_path)
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA synchronous=NORMAL")
        await self._db.execute("PRAGMA cache_size=-64000")  # 64MB cache
        await self._db.execute("PRAGMA temp_store=MEMORY")

        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS crawled_urls (
                url             TEXT PRIMARY KEY,
                domain          TEXT NOT NULL,
                status          INTEGER NOT NULL DEFAULT 0,
                depth           INTEGER NOT NULL DEFAULT 0,
                content_type    TEXT DEFAULT '',
                error           TEXT DEFAULT '',
                redirect_url    TEXT DEFAULT '',
                crawled_at      REAL NOT NULL
            )
        """)
        await self._db.execute("""
            CREATE INDEX IF NOT EXISTS idx_domain ON crawled_urls(domain)
        """)
        await self._db.execute("""
            CREATE INDEX IF NOT EXISTS idx_status ON crawled_urls(status)
        """)
        await self._db.commit()
        logger.info("SQLite storage initialized at %s", self._db_path)

    async def save_result(
        self,
        url: str,
        domain: str,
        status: int,
        depth: int,
        content_type: str = "",
        error: str = "",
        redirect_url: str = "",
    ) -> None:
        row = (url, domain, status, depth, content_type, error, redirect_url, time.time())
        async with self._buffer_lock:
            self._buffer.append(row)
            if len(self._buffer) >= self._batch_size:
                await self._flush_buffer()

    async def _flush_buffer(self) -> None:
        """Write buffered rows to SQLite."""
        if not self._buffer or not self._db:
            return
        rows = self._buffer[:]
        self._buffer.clear()
        try:
            await self._db.executemany(
                """INSERT OR IGNORE INTO crawled_urls
                   (url, domain, status, depth, content_type, error, redirect_url, crawled_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                rows,
            )
            await self._db.commit()
        except Exception as e:
            logger.error("SQLite write error: %s", e)

    async def flush(self) -> None:
        """Force flush the write buffer."""
        async with self._buffer_lock:
            await self._flush_buffer()

    async def count_discovered(self) -> int:
        if not self._db:
            return 0
        async with self._db.execute("SELECT COUNT(*) FROM crawled_urls") as cur:
            row = await cur.fetchone()
            return row[0] if row else 0

    async def count_successful(self) -> int:
        if not self._db:
            return 0
        async with self._db.execute(
            "SELECT COUNT(*) FROM crawled_urls WHERE status = 200"
        ) as cur:
            row = await cur.fetchone()
            return row[0] if row else 0

    async def get_stats(self) -> Dict[str, int]:
        if not self._db:
            return {}

        stats: Dict[str, int] = {}

        async with self._db.execute("SELECT COUNT(*) FROM crawled_urls") as cur:
            row = await cur.fetchone()
            stats["total_crawled"] = row[0] if row else 0

        async with self._db.execute(
            "SELECT COUNT(*) FROM crawled_urls WHERE status = 200"
        ) as cur:
            row = await cur.fetchone()
            stats["successful"] = row[0] if row else 0

        async with self._db.execute(
            "SELECT COUNT(*) FROM crawled_urls WHERE error != ''"
        ) as cur:
            row = await cur.fetchone()
            stats["errors"] = row[0] if row else 0

        async with self._db.execute(
            "SELECT COUNT(DISTINCT domain) FROM crawled_urls"
        ) as cur:
            row = await cur.fetchone()
            stats["unique_domains"] = row[0] if row else 0

        return stats

    async def close(self) -> None:
        if self._db:
            async with self._buffer_lock:
                await self._flush_buffer()
            await self._db.close()
            self._db = None
