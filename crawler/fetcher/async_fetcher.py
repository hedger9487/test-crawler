"""Async HTTP fetcher using aiohttp.

Features:
  - Configurable connection pool
  - Content-type pre-check via HEAD or Content-Type header
  - Timeout and redirect handling
  - SSL verification disabled by default for maximum crawl coverage
"""

from __future__ import annotations

import asyncio
import logging
import time

import aiohttp

from crawler.fetcher.base import AbstractFetcher, CrawlResult

logger = logging.getLogger(__name__)

# Maximum HTML size to download (5 MB) to avoid memory issues
MAX_HTML_SIZE = 5 * 1024 * 1024


class AsyncFetcher(AbstractFetcher):
    """aiohttp-based async fetcher with connection pooling."""

    def __init__(
        self,
        user_agent: str = "HW1-Crawler/0.1",
        timeout: int = 30,
        max_redirects: int = 5,
        pool_size: int = 100,
        per_host: int = 2,
        accept_content_types: list[str] | None = None,
        ssl_verify: bool = False,
    ):
        self._user_agent = user_agent
        self._timeout = aiohttp.ClientTimeout(
            total=timeout,       # overall request limit
            sock_connect=10,      # TCP connect timeout (prevents DNS/handshake hangs)
            sock_read=timeout,   # per-read timeout (kills zombie connections)
        )
        self._max_redirects = max_redirects
        self._pool_size = pool_size
        self._per_host = per_host
        self._ssl = None if ssl_verify else False  # None = default verify, False = skip
        self._accept_types = accept_content_types or [
            "text/html",
            "application/xhtml+xml",
        ]
        self._session: aiohttp.ClientSession | None = None
        self._connector: aiohttp.TCPConnector | None = None

    async def start(self) -> None:
        self._connector = aiohttp.TCPConnector(
            limit=self._pool_size,
            limit_per_host=self._per_host,
            ttl_dns_cache=300,        # 5 min DNS cache
            enable_cleanup_closed=True,
            ssl=self._ssl,
        )
        self._session = aiohttp.ClientSession(
            connector=self._connector,
            timeout=self._timeout,
            headers={
                "User-Agent": self._user_agent,
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate",
            },
            max_field_size=16384,
        )

    async def stop(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None
        if self._connector:
            await self._connector.close()
            self._connector = None

    @property
    def session(self) -> aiohttp.ClientSession | None:
        return self._session

    async def fetch(self, url: str) -> CrawlResult:
        if not self._session:
            raise RuntimeError("Fetcher not started. Call start() first.")

        t0 = time.monotonic()

        try:
            async with self._session.get(
                url,
                allow_redirects=True,
                max_redirects=self._max_redirects,
            ) as resp:
                elapsed = (time.monotonic() - t0) * 1000

                # Check content type before reading body
                content_type = resp.headers.get("Content-Type", "")
                redirect_url = str(resp.url) if str(resp.url) != url else ""

                if not self._is_acceptable_type(content_type):
                    return CrawlResult(
                        url=url,
                        status=resp.status,
                        content_type=content_type,
                        elapsed_ms=elapsed,
                        redirect_url=redirect_url,
                        headers=dict(resp.headers),
                    )

                # Read body with size limit
                try:
                    html = await resp.content.read(MAX_HTML_SIZE)
                    # Try to decode
                    encoding = resp.get_encoding() or "utf-8"
                    try:
                        html_text = html.decode(encoding, errors="replace")
                    except (LookupError, UnicodeDecodeError):
                        html_text = html.decode("utf-8", errors="replace")
                except Exception as e:
                    return CrawlResult(
                        url=url,
                        status=resp.status,
                        content_type=content_type,
                        elapsed_ms=elapsed,
                        error=f"read error: {e}",
                        redirect_url=redirect_url,
                    )

                return CrawlResult(
                    url=url,
                    status=resp.status,
                    html=html_text,
                    content_type=content_type,
                    elapsed_ms=elapsed,
                    redirect_url=redirect_url,
                    headers=dict(resp.headers),
                )

        except asyncio.TimeoutError:
            elapsed = (time.monotonic() - t0) * 1000
            return CrawlResult(url=url, elapsed_ms=elapsed, error="timeout")

        except aiohttp.ClientError as e:
            elapsed = (time.monotonic() - t0) * 1000
            return CrawlResult(url=url, elapsed_ms=elapsed, error=str(e))

        except Exception as e:
            elapsed = (time.monotonic() - t0) * 1000
            logger.debug("Unexpected fetch error for %s: %s", url, e)
            return CrawlResult(url=url, elapsed_ms=elapsed, error=str(e))

    def _is_acceptable_type(self, content_type: str) -> bool:
        """Check if the Content-Type matches our accepted types."""
        if not content_type:
            return True  # Give it a chance if server doesn't specify
        ct_lower = content_type.lower()
        return any(at in ct_lower for at in self._accept_types)
