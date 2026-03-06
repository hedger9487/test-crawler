"""Per-domain rate limiter using token bucket algorithm.

Enforces max 0.5 QPS (= 1 request every 2 seconds) per domain.
Also respects Crawl-delay from robots.txt (takes the stricter of the two).
Automatically cleans up idle domains to save memory.
"""

from __future__ import annotations

import asyncio
import time
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class _DomainBucket:
    """Token bucket for a single domain."""
    __slots__ = ("tokens", "max_tokens", "refill_rate", "last_refill", "last_used",
                 "min_interval")

    def __init__(self, max_qps: float, crawl_delay: Optional[float] = None):
        # Base interval from max_qps
        base_interval = 1.0 / max_qps if max_qps > 0 else 2.0

        # If Crawl-delay is specified, use the stricter (longer) interval
        if crawl_delay and crawl_delay > base_interval:
            self.min_interval = crawl_delay
            effective_qps = 1.0 / crawl_delay
        else:
            self.min_interval = base_interval
            effective_qps = max_qps

        self.max_tokens = 1.0
        self.tokens = 1.0  # start with 1 token = can fetch immediately
        self.refill_rate = effective_qps  # tokens per second
        self.last_refill = time.monotonic()
        self.last_used = time.monotonic()

    def refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.max_tokens, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now

    def try_consume(self) -> float:
        """Try to consume a token. Returns 0 if consumed, or wait time in seconds."""
        self.refill()
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            self.last_used = time.monotonic()
            return 0.0
        # How long until we have a token?
        deficit = 1.0 - self.tokens
        return deficit / self.refill_rate


class RateLimiter:
    """Per-domain rate limiter with Crawl-delay support and idle cleanup."""

    def __init__(
        self,
        max_qps: float = 0.5,
        idle_cleanup_seconds: float = 300.0,
        cleanup_interval: float = 60.0,
    ):
        self._max_qps = max_qps
        self._idle_cleanup = idle_cleanup_seconds
        self._cleanup_interval = cleanup_interval

        self._buckets: Dict[str, _DomainBucket] = {}
        self._locks: Dict[str, asyncio.Lock] = {}
        self._global_lock = asyncio.Lock()

        self._cleanup_task: asyncio.Task | None = None

        # Tracking: total time workers spent waiting on rate limiter
        self._total_wait_time: float = 0.0
        self._total_acquires: int = 0

    async def start(self) -> None:
        """Start the background cleanup task."""
        if self._cleanup_task is None:
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    async def stop(self) -> None:
        """Stop the background cleanup task."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None

    async def acquire(self, domain: str, crawl_delay: Optional[float] = None) -> float:
        """Wait until a request to this domain is allowed.

        Args:
            domain: The target domain.
            crawl_delay: Optional Crawl-delay from robots.txt (seconds).

        Returns:
            The actual wait time in seconds (0 if no wait needed).
        """
        # Get or create per-domain lock
        async with self._global_lock:
            if domain not in self._locks:
                self._locks[domain] = asyncio.Lock()
            lock = self._locks[domain]

        t0 = time.monotonic()

        async with lock:
            bucket = self._buckets.get(domain)
            if bucket is None:
                bucket = _DomainBucket(self._max_qps, crawl_delay)
                self._buckets[domain] = bucket
            elif crawl_delay and (crawl_delay > bucket.min_interval):
                # Update if robots.txt now specifies a stricter delay
                # Preserve tokens=0 so the next request must wait the full interval
                bucket = _DomainBucket(self._max_qps, crawl_delay)
                bucket.tokens = 0.0  # don't give a free ticket on rebuild
                self._buckets[domain] = bucket

            wait_time = bucket.try_consume()
            if wait_time > 0:
                await asyncio.sleep(wait_time)
                # After sleeping, refill and consume
                bucket.refill()
                bucket.tokens = max(0, bucket.tokens - 1.0)
                bucket.last_used = time.monotonic()

        actual_wait = time.monotonic() - t0
        self._total_wait_time += actual_wait
        self._total_acquires += 1
        return actual_wait

    def peek_wait_time(self, domain: str) -> float:
        """Non-blocking estimate of how long until a domain is available.

        Returns 0.0 if the domain is ready (or unknown/new).
        This is a COARSE estimate — no locks, reads stale state on purpose.
        Used by the frontier to skip obviously-cooling-down domains.
        """
        bucket = self._buckets.get(domain)
        if bucket is None:
            return 0.0  # new domain → ready immediately
        # Simulate a refill without mutating state
        now = time.monotonic()
        elapsed = now - bucket.last_refill
        simulated_tokens = min(bucket.max_tokens, bucket.tokens + elapsed * bucket.refill_rate)
        if simulated_tokens >= 1.0:
            return 0.0
        deficit = 1.0 - simulated_tokens
        return deficit / bucket.refill_rate

    @property
    def default_interval(self) -> float:
        """The base interval between requests (1/max_qps)."""
        return 1.0 / self._max_qps if self._max_qps > 0 else 2.0

    @property
    def active_domains(self) -> int:
        return len(self._buckets)

    @property
    def avg_wait_time(self) -> float:
        if self._total_acquires == 0:
            return 0.0
        return self._total_wait_time / self._total_acquires

    @property
    def total_wait_time(self) -> float:
        return self._total_wait_time

    async def _cleanup_loop(self) -> None:
        """Periodically remove idle domain buckets."""
        while True:
            try:
                await asyncio.sleep(self._cleanup_interval)
                now = time.monotonic()
                async with self._global_lock:
                    expired = [
                        domain
                        for domain, bucket in self._buckets.items()
                        if (now - bucket.last_used) > self._idle_cleanup
                    ]
                    for domain in expired:
                        del self._buckets[domain]
                        self._locks.pop(domain, None)
                    if expired:
                        logger.debug("Cleaned up %d idle domain buckets", len(expired))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("Cleanup error: %s", e)
