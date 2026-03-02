"""Robots.txt handler with 24-hour caching.

Fetches, parses, and caches robots.txt per domain. Follows standard rules:
  - 2xx: parse and enforce rules
  - 4xx: allow all (no robots.txt exists)
  - 5xx: disallow all (server error, be conservative)
  - Timeout/network error: disallow all (conservative compliance)

Also extracts Crawl-delay and applies it as a minimum delay per domain.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

# Regex to extract Crawl-delay from robots.txt (stdlib doesn't parse it)
_CRAWL_DELAY_RE = re.compile(
    r"^crawl-delay:\s*(\d+(?:\.\d+)?)\s*$", re.IGNORECASE | re.MULTILINE
)


class _CachedRobots:
    """A cached robots.txt entry."""
    __slots__ = ("parser", "fetched_at", "allows_all", "disallows_all", "crawl_delay")

    def __init__(
        self,
        parser: Optional[RobotFileParser],
        fetched_at: float,
        allows_all: bool = False,
        disallows_all: bool = False,
        crawl_delay: Optional[float] = None,
    ):
        self.parser = parser
        self.fetched_at = fetched_at
        self.allows_all = allows_all
        self.disallows_all = disallows_all
        self.crawl_delay = crawl_delay  # seconds between requests, if specified


class RobotsHandler:
    """Fetches, parses, and caches robots.txt with a configurable TTL.

    Compliance rules:
      - 2xx → parse rules, extract Crawl-delay
      - 4xx → allow all (no robots.txt)
      - 5xx → disallow all (server error → conservative)
      - Timeout → disallow all (conservative compliance)
      - Network error → disallow all (conservative compliance)
    """

    def __init__(
        self,
        user_agent: str = "*",
        cache_ttl: int = 86_400,
        fetch_timeout: int = 10,
        session: Optional[aiohttp.ClientSession] = None,
    ):
        self._user_agent = user_agent
        self._cache_ttl = cache_ttl
        self._fetch_timeout = fetch_timeout
        self._session = session

        # domain -> _CachedRobots
        self._cache: dict[str, _CachedRobots] = {}
        # domain -> asyncio.Lock (prevent thundering herd)
        self._locks: dict[str, asyncio.Lock] = {}
        self._global_lock = asyncio.Lock()

    def set_session(self, session: aiohttp.ClientSession) -> None:
        self._session = session

    async def is_allowed(self, url: str) -> bool:
        """Check if the given URL is allowed by robots.txt rules."""
        parsed = urlparse(url)
        domain = parsed.hostname
        if not domain:
            return False

        scheme = parsed.scheme or "https"
        cached = await self._get_or_fetch(domain, scheme)

        if cached.allows_all:
            return True
        if cached.disallows_all:
            return False
        if cached.parser is None:
            return True  # shouldn't happen, but fail-open

        try:
            return cached.parser.can_fetch(self._user_agent, url)
        except Exception:
            return True  # fail-open on parse errors

    async def get_crawl_delay(self, domain: str) -> Optional[float]:
        """Get the Crawl-delay for a domain, if specified in robots.txt.

        Returns None if no Crawl-delay was specified, or the delay in seconds.
        """
        cached = self._cache.get(domain)
        if cached:
            return cached.crawl_delay
        return None

    async def _get_or_fetch(self, domain: str, scheme: str) -> _CachedRobots:
        """Return cached entry, or fetch if expired/missing."""
        cached = self._cache.get(domain)
        if cached and (time.monotonic() - cached.fetched_at) < self._cache_ttl:
            return cached

        # Get per-domain lock (avoid fetching robots.txt multiple times)
        async with self._global_lock:
            if domain not in self._locks:
                self._locks[domain] = asyncio.Lock()
            lock = self._locks[domain]

        async with lock:
            # Double-check after acquiring lock
            cached = self._cache.get(domain)
            if cached and (time.monotonic() - cached.fetched_at) < self._cache_ttl:
                return cached

            entry = await self._fetch_robots(domain, scheme)
            self._cache[domain] = entry
            return entry

    async def _fetch_robots(self, domain: str, scheme: str) -> _CachedRobots:
        """Fetch and parse robots.txt for a domain."""
        robots_url = f"{scheme}://{domain}/robots.txt"
        now = time.monotonic()

        if not self._session:
            # No session → conservative: disallow all
            logger.warning("RobotsHandler: no session, disallowing %s", domain)
            return _CachedRobots(None, now, disallows_all=True)

        try:
            async with self._session.get(
                robots_url,
                timeout=aiohttp.ClientTimeout(total=self._fetch_timeout),
                allow_redirects=True,
                ssl=False,
            ) as resp:
                if 400 <= resp.status < 500:
                    # 4xx → allow all (robots.txt doesn't exist)
                    logger.debug("robots.txt 4xx for %s → allow all", domain)
                    return _CachedRobots(None, now, allows_all=True)

                if resp.status >= 500:
                    # 5xx → conservative: disallow all
                    logger.debug("robots.txt 5xx for %s → disallow all", domain)
                    return _CachedRobots(None, now, disallows_all=True)

                text = await resp.text(errors="replace")

                # Parse standard rules
                parser = RobotFileParser()
                parser.parse(text.splitlines())

                # Extract Crawl-delay (stdlib doesn't handle this)
                crawl_delay = self._extract_crawl_delay(text, domain)

                return _CachedRobots(parser, now, crawl_delay=crawl_delay)

        except asyncio.TimeoutError:
            # Timeout → conservative: disallow all
            logger.debug("robots.txt timeout for %s → disallow all", domain)
            return _CachedRobots(None, now, disallows_all=True)

        except Exception as e:
            # Network error → conservative: disallow all
            logger.debug("robots.txt error for %s: %s → disallow all", domain, e)
            return _CachedRobots(None, now, disallows_all=True)

    def _extract_crawl_delay(self, robots_text: str, domain: str) -> Optional[float]:
        """Extract Crawl-delay for our user-agent from robots.txt text.

        Strategy: look for Crawl-delay in our user-agent block first,
        then fall back to the wildcard (*) block's Crawl-delay.
        Simple approach: just find all Crawl-delay directives and use the
        first one (most robots.txt files only have one).
        """
        match = _CRAWL_DELAY_RE.search(robots_text)
        if match:
            try:
                delay = float(match.group(1))
                if delay > 0:
                    logger.debug("Crawl-delay for %s: %.1fs", domain, delay)
                    return delay
            except ValueError:
                pass
        return None

    @property
    def cache_size(self) -> int:
        return len(self._cache)
