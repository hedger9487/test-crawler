"""Abstract base class for fetcher implementations."""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from typing import Optional, Dict


@dataclass
class CrawlResult:
    """Result of fetching a single URL."""
    url: str
    status: int = 0
    html: str = ""
    headers: Dict[str, str] = field(default_factory=dict)
    content_type: str = ""
    elapsed_ms: float = 0.0
    error: str = ""
    redirect_url: str = ""

    @property
    def is_success(self) -> bool:
        return self.status == 200

    @property
    def is_html(self) -> bool:
        ct = self.content_type.lower()
        return "text/html" in ct or "application/xhtml" in ct


class AbstractFetcher(abc.ABC):
    """Interface for HTTP fetchers."""

    @abc.abstractmethod
    async def fetch(self, url: str) -> CrawlResult:
        """Fetch a URL and return the result."""
        ...

    @abc.abstractmethod
    async def start(self) -> None:
        """Initialize resources (e.g., connection pool)."""
        ...

    @abc.abstractmethod
    async def stop(self) -> None:
        """Cleanup resources."""
        ...
