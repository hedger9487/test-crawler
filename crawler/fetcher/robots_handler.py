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

# Regex to extract Sitemap URLs from robots.txt
_SITEMAP_RE = re.compile(
    r"^sitemap:\s*(https?://\S+)\s*$", re.IGNORECASE | re.MULTILINE
)


class _CachedRobots:
    """A cached robots.txt entry."""
    __slots__ = ("parser", "fetched_at", "allows_all", "disallows_all",
                 "crawl_delay", "sitemap_urls")

    def __init__(
        self,
        parser: Optional[RobotFileParser],
        fetched_at: float,
        allows_all: bool = False,
        disallows_all: bool = False,
        crawl_delay: Optional[float] = None,
        sitemap_urls: Optional[list[str]] = None,
    ):
        self.parser = parser
        self.fetched_at = fetched_at
        self.allows_all = allows_all
        self.disallows_all = disallows_all
        self.crawl_delay = crawl_delay
        self.sitemap_urls = sitemap_urls or []


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

                # Extract Crawl-delay
                crawl_delay = self._extract_crawl_delay(text, domain)

                # Extract Sitemap URLs
                sitemap_urls = _SITEMAP_RE.findall(text)
                if sitemap_urls:
                    logger.debug("Found %d sitemap(s) for %s", len(sitemap_urls), domain)

                return _CachedRobots(parser, now, crawl_delay=crawl_delay,
                                     sitemap_urls=sitemap_urls)

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

    def get_sitemap_urls(self, domain: str) -> list[str]:
        """Get sitemap URLs found in this domain's robots.txt.

        Returns a list of sitemap XML URLs (may be empty).
        Must call is_allowed() first to trigger robots.txt fetch.
        """
        cached = self._cache.get(domain)
        if cached:
            return cached.sitemap_urls
        return []

    async def fetch_sitemap(
        self, sitemap_url: str,
    ) -> list[str]:
        """Fetch and parse a sitemap XML, returning ALL page URLs.

        Handles both <urlset> (direct URLs) and <sitemapindex>
        (links to nested sitemap files).

        Memory is managed by the frontier cap, not here.
        """
        if not self._session:
            return []

        try:
            async with self._session.get(
                sitemap_url,
                timeout=aiohttp.ClientTimeout(total=self._fetch_timeout),
                allow_redirects=True,
                ssl=False,
            ) as resp:
                if resp.status != 200:
                    return []

                text = await resp.text(errors="replace")

                import xml.etree.ElementTree as ET
                try:
                    root = ET.fromstring(text)
                except ET.ParseError:
                    return []

                # Check if this is a sitemap index (contains nested sitemaps)
                nested_sitemaps = []
                urls = []
                for elem in root.iter():
                    tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
                    if tag == "loc" and elem.text:
                        loc = elem.text.strip()
                        if loc.startswith("http"):
                            # Heuristic: if URL ends with .xml or contains "sitemap",
                            # it's likely a nested sitemap
                            if loc.endswith(".xml") or "sitemap" in loc.lower():
                                nested_sitemaps.append(loc)
                            else:
                                urls.append(loc)

                # If this was a sitemap index, fetch first 3 nested sitemaps
                if nested_sitemaps and not urls:
                    for nested in nested_sitemaps[:3]:
                        nested_urls = await self.fetch_sitemap(nested)
                        urls.extend(nested_urls)

                logger.debug("Sitemap %s: found %d URLs", sitemap_url, len(urls))
                return urls

        except Exception as e:
            logger.debug("Sitemap fetch error for %s: %s", sitemap_url, e)
            return []
