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
    depth: int = 0       # BFS hop depth from seed
    url_depth: int = 0   # Full-URL slash count (includes https:// prefix):
                          # https://example.com/          → 3
                          # https://example.com/sports/x/ → 6
                          # Used for within-domain queue ordering (shallow first).
    priority: float = 0.0   # reserved for caller overrides (not used by default scheduler)
    issue_time: float = 0.0  # set by frontier at dispatch time
    reservation_id: int = 0  # set by frontier when domain enters Reserved

    def __lt__(self, other: "CrawlURL") -> bool:
        # Within a domain queue: prefer shallow BFS hops first,
        # then prefer URLs with fewer path segments (less deeply nested pages).
        return (self.depth, self.url_depth) < (other.depth, other.url_depth)


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
    async def requeue_issued_url(self, item: CrawlURL) -> bool:
        """Requeue a URL that was already issued by get_next().

        This bypasses dedup checks because the URL is already known/seen.
        Returns True if it was put back into the frontier.
        """
        ...

    @abc.abstractmethod
    async def mark_acquired(self, item: CrawlURL) -> bool:
        """Mark that the worker has successfully acquired a rate-limit slot.

        This transitions the issued domain from Reserved -> Cooling/Empty.
        Returns True if a matching reservation was finalized.
        """
        ...

    @abc.abstractmethod
    async def release_issued_url(self, item: CrawlURL, *, to_cooling: bool = False) -> bool:
        """Release a reserved issued URL without page fetch.

        Used when a worker aborts/defers this issued task.
        If to_cooling=True, the domain returns to Cooling; else Ready/Empty.
        Returns True if a matching reservation was released.
        """
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
