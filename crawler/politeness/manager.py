"""Politeness manager — combines rate limiter + robots handler."""

from __future__ import annotations

from typing import Optional

from crawler.fetcher.robots_handler import RobotsHandler
from crawler.politeness.rate_limiter import RateLimiter
from crawler.parser.url_normalizer import get_domain


class PolitenessManager:
    """Single entry point for all politeness checks.

    Usage:
        if not await manager.can_crawl(url):
            skip(url)
        wait_time = await manager.wait_for_slot(url)  # blocks until rate-limit allows
        ... do fetch ...
    """

    def __init__(self, robots: RobotsHandler, rate_limiter: RateLimiter):
        self._robots = robots
        self._rate_limiter = rate_limiter

    @property
    def robots(self) -> RobotsHandler:
        return self._robots

    @property
    def rate_limiter(self) -> RateLimiter:
        return self._rate_limiter

    async def can_crawl(self, url: str) -> bool:
        """Check if the URL is allowed by robots.txt."""
        return await self._robots.is_allowed(url)

    async def wait_for_slot(self, url: str) -> float:
        """Block until the per-domain rate limiter allows a request.

        Respect Crawl-delay from robots.txt if specified.
        Returns the actual wait time in seconds.
        """
        domain = get_domain(url)
        if domain:
            crawl_delay = await self._robots.get_crawl_delay(domain)
            return await self._rate_limiter.acquire(domain, crawl_delay=crawl_delay)
        return 0.0

    async def start(self) -> None:
        await self._rate_limiter.start()

    async def stop(self) -> None:
        await self._rate_limiter.stop()
