"""Abstract storage interface."""

from __future__ import annotations

import abc
from typing import Dict


class AbstractStorage(abc.ABC):
    """Interface for crawl data storage."""

    @abc.abstractmethod
    async def init(self) -> None:
        """Initialize storage (create tables, etc.)."""
        ...

    @abc.abstractmethod
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
        """Save a single crawl result."""
        ...

    @abc.abstractmethod
    async def count_discovered(self) -> int:
        """Number of unique URLs discovered."""
        ...

    @abc.abstractmethod
    async def count_successful(self) -> int:
        """Number of URLs with status 200."""
        ...

    @abc.abstractmethod
    async def get_stats(self) -> Dict[str, int]:
        """Get comprehensive stats dict."""
        ...

    @abc.abstractmethod
    async def close(self) -> None:
        """Cleanup."""
        ...
