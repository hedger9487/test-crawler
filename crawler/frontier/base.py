"""Abstract base classes for the frontier."""

from __future__ import annotations

import abc
from dataclasses import dataclass
from typing import Optional


@dataclass
class CrawlURL:
    """A URL queued for crawling."""
    url: str
    domain: str
    depth: int = 0
    priority: float = 0.0  # lower = higher priority

    def __lt__(self, other: "CrawlURL") -> bool:
        return self.priority < other.priority


class AbstractFrontier(abc.ABC):
    """Interface for URL frontier implementations."""

    @abc.abstractmethod
    async def add_url(self, url: str, depth: int = 0, priority: float = 0.0) -> bool:
        """Add a URL. Returns True if newly added, False if duplicate."""
        ...

    @abc.abstractmethod
    async def add_urls(self, urls: list[str], depth: int = 0) -> int:
        """Bulk-add URLs. Returns count of newly added."""
        ...

    @abc.abstractmethod
    async def get_next(self) -> Optional[CrawlURL]:
        """Get the next URL to crawl. Returns None if empty."""
        ...

    @abc.abstractmethod
    async def size(self) -> int:
        """Number of URLs waiting in the frontier."""
        ...

    @abc.abstractmethod
    async def total_seen(self) -> int:
        """Total number of unique URLs ever seen (including already crawled)."""
        ...

    @abc.abstractmethod
    async def save_checkpoint(self, path: str) -> None:
        """Persist frontier state to disk."""
        ...

    @abc.abstractmethod
    async def load_checkpoint(self, path: str) -> None:
        """Restore frontier state from disk."""
        ...
