"""In-memory URL frontier with Bloom filter deduplication and domain-aware scheduling.

Uses per-domain queues for fair round-robin scheduling and a Bloom filter
for O(1) memory-efficient dedup at scale (50M URLs ≈ 80 MB).

Hybrid scheduling strategy:
  - Round-robins across domains to ensure breadth
  - SKIPS domains that are in rate-limit cooldown (coarse check via peek)
  - Worker does fine-grained rate-limit acquire for precise compliance
  - Within a domain, URLs are ordered by depth (BFS: shallow first)
"""

from __future__ import annotations

import asyncio
import collections
import heapq
import pickle
import time
from pathlib import Path
from typing import Optional, Callable

from bitarray import bitarray
import mmh3

from crawler.frontier.base import AbstractFrontier, CrawlURL
from crawler.parser.url_normalizer import get_domain, normalize_url


class BloomFilter:
    """Simple Bloom filter backed by bitarray + mmh3."""

    __slots__ = ("_size", "_num_hashes", "_bits", "_count")

    def __init__(self, capacity: int = 50_000_000, error_rate: float = 0.001):
        import math
        self._size = self._optimal_size(capacity, error_rate)
        self._num_hashes = self._optimal_hashes(self._size, capacity)
        self._bits = bitarray(self._size)
        self._bits.setall(False)
        self._count = 0

    @staticmethod
    def _optimal_size(n: int, p: float) -> int:
        import math
        return int(-n * math.log(p) / (math.log(2) ** 2))

    @staticmethod
    def _optimal_hashes(m: int, n: int) -> int:
        import math
        return max(1, int((m / n) * math.log(2)))

    def add(self, item: str) -> bool:
        """Add item. Returns True if item was NEW (not previously seen)."""
        indices = self._get_indices(item)
        is_new = not all(self._bits[i] for i in indices)
        for i in indices:
            self._bits[i] = True
        if is_new:
            self._count += 1
        return is_new

    def __contains__(self, item: str) -> bool:
        return all(self._bits[i] for i in self._get_indices(item))

    def __len__(self) -> int:
        return self._count

    def _get_indices(self, item: str) -> list[int]:
        h1 = mmh3.hash(item, 0, signed=False)
        h2 = mmh3.hash(item, h1, signed=False)
        return [(h1 + i * h2) % self._size for i in range(self._num_hashes)]


class MemoryFrontier(AbstractFrontier):
    """In-memory frontier with domain-aware scheduling.

    Hybrid design:
      - Frontier skips domains currently in cooldown (coarse-grained)
      - Worker still does precise rate-limit acquire (fine-grained safety net)
      - Result: workers almost never block on rate-limit, maximizing throughput

    The cooldown checker is injected via set_cooldown_checker().
    """

    def __init__(
        self,
        bloom_capacity: int = 50_000_000,
        bloom_error_rate: float = 0.001,
        max_depth: int = -1,
    ):
        self._bloom = BloomFilter(bloom_capacity, bloom_error_rate)
        self._max_depth = max_depth

        # domain -> list of CrawlURL (used as a min-heap by depth)
        self._domain_queues: dict[str, list[CrawlURL]] = collections.defaultdict(list)

        # Round-robin state: ordered list of domains with pending URLs
        self._active_domains: collections.deque[str] = collections.deque()
        self._active_domain_set: set[str] = set()

        # Domain priority scores (higher = better, from profiler)
        self._domain_priority: dict[str, float] = {}

        self._pending_count = 0
        self._lock = asyncio.Lock()

        # Cooldown checker: domain -> estimated wait seconds (0 = ready)
        # Injected by orchestrator, calls rate_limiter.peek_wait_time()
        self._cooldown_checker: Optional[Callable[[str], float]] = None

        # Profiling: how many domains were skipped due to cooldown
        self._cooldown_skips = 0

    def set_cooldown_checker(self, checker: Callable[[str], float]) -> None:
        """Inject a function that returns estimated cooldown for a domain.

        Args:
            checker: A callable(domain) -> float. Returns 0.0 if ready,
                     or estimated seconds until the domain is available.
        """
        self._cooldown_checker = checker

    def update_domain_priority(self, domain: str, score: float) -> None:
        """Update the priority score for a domain (from profiler yield data)."""
        self._domain_priority[domain] = score

    def update_domain_priorities(self, scores: dict[str, float]) -> None:
        """Batch update domain priorities."""
        self._domain_priority.update(scores)

    async def add_url(
        self, url: str, depth: int = 0, priority: float = 0.0
    ) -> bool:
        normalized = normalize_url(url)
        if normalized is None:
            return False
        if self._max_depth >= 0 and depth > self._max_depth:
            return False

        async with self._lock:
            if not self._bloom.add(normalized):
                return False  # duplicate

            domain = get_domain(normalized) or "unknown"

            # Priority = depth (BFS: shallow first)
            crawl_url = CrawlURL(
                url=normalized, domain=domain, depth=depth, priority=float(depth)
            )
            heapq.heappush(self._domain_queues[domain], crawl_url)
            self._pending_count += 1

            if domain not in self._active_domain_set:
                self._active_domains.append(domain)
                self._active_domain_set.add(domain)

            return True

    async def add_urls(self, urls: list[str], depth: int = 0) -> int:
        count = 0
        for url in urls:
            if await self.add_url(url, depth=depth):
                count += 1
        return count

    async def get_next(self) -> Optional[CrawlURL]:
        """Get the next URL to crawl, skipping cooldown domains.

        Strategy:
          1. Round-robin across active domains
          2. For each domain, check if it's in cooldown (coarse estimate)
          3. If in cooldown, skip it and try the next domain
          4. If ALL domains are in cooldown, return the one with the
             shortest remaining cooldown (don't waste worker time)

        Returns:
            A CrawlURL if available, None if frontier is empty.
        """
        async with self._lock:
            num_domains = len(self._active_domains)
            if num_domains == 0:
                return None

            best_cooldown_url: Optional[CrawlURL] = None
            best_cooldown_wait: float = float("inf")
            best_cooldown_domain: Optional[str] = None

            tried = 0
            while tried < num_domains:
                domain = self._active_domains[0]
                self._active_domains.rotate(-1)
                tried += 1

                queue = self._domain_queues.get(domain)
                if not queue:
                    continue

                # ── Cooldown check (coarse, lock-free) ──
                if self._cooldown_checker:
                    remaining = self._cooldown_checker(domain)
                    if remaining > 0.1:  # >100ms cooldown → skip
                        self._cooldown_skips += 1
                        # Track the domain with the shortest cooldown as fallback
                        if remaining < best_cooldown_wait:
                            best_cooldown_wait = remaining
                            best_cooldown_domain = domain
                        continue

                # ── Domain is ready → dequeue ──
                item = heapq.heappop(queue)
                self._pending_count -= 1
                if not queue:
                    self._active_domains.remove(domain)
                    self._active_domain_set.discard(domain)
                    del self._domain_queues[domain]
                return item

            # All domains are in cooldown — dequeue from the one
            # that will be ready soonest (minimize worker idle time)
            if best_cooldown_domain:
                queue = self._domain_queues.get(best_cooldown_domain)
                if queue:
                    item = heapq.heappop(queue)
                    self._pending_count -= 1
                    if not queue:
                        self._active_domains.remove(best_cooldown_domain)
                        self._active_domain_set.discard(best_cooldown_domain)
                        del self._domain_queues[best_cooldown_domain]
                    return item

            return None  # frontier empty

    async def size(self) -> int:
        return self._pending_count

    async def total_seen(self) -> int:
        return len(self._bloom)

    async def domain_count(self) -> int:
        """Number of domains with pending URLs."""
        return len(self._active_domain_set)

    @property
    def cooldown_skips(self) -> int:
        """Number of times a domain was skipped due to cooldown."""
        return self._cooldown_skips

    async def save_checkpoint(self, path: str) -> None:
        """Serialize frontier state to disk."""
        async with self._lock:
            state = {
                "bloom": self._bloom,
                "domain_queues": dict(self._domain_queues),
                "active_domains": list(self._active_domains),
                "pending_count": self._pending_count,
                "domain_priority": dict(self._domain_priority),
            }
            p = Path(path)
            p.parent.mkdir(parents=True, exist_ok=True)
            with open(p, "wb") as f:
                pickle.dump(state, f, protocol=pickle.HIGHEST_PROTOCOL)

    async def load_checkpoint(self, path: str) -> None:
        """Restore frontier state from disk."""
        p = Path(path)
        if not p.exists():
            return
        with open(p, "rb") as f:
            state = pickle.load(f)
        async with self._lock:
            self._bloom = state["bloom"]
            self._domain_queues = collections.defaultdict(
                list, state["domain_queues"]
            )
            self._active_domains = collections.deque(state["active_domains"])
            self._active_domain_set = set(self._active_domains)
            self._pending_count = state["pending_count"]
            self._domain_priority = state.get("domain_priority", {})
