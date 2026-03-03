"""In-memory URL frontier with three-state domain scheduling.

Domain states:
  Ready   → max-heap by yield score: domains available for immediate fetch
  Cooling → min-heap by ready_at: domains waiting for rate-limit cooldown
  Empty   → set: domains with no pending URLs

Transitions:
  Ready → Cooling: after get_next() pops a URL from a domain
  Cooling → Ready: when cooldown expires (promoted on next get_next())
  Ready → Empty: domain's queue exhausted after pop
  Empty → Ready: add_url() adds a URL to an empty domain
  Cooling + add_url: stays Cooling (will return to Ready when cooldown expires)

Uses per-domain URL queues (min-heap by depth for BFS within a domain)
and a Bloom filter for O(1) memory-efficient dedup at scale.
"""

from __future__ import annotations

import asyncio
import collections
import heapq
import logging
import pickle
import time
from pathlib import Path
from typing import Optional, Callable

from bitarray import bitarray
import mmh3

from crawler.frontier.base import AbstractFrontier, CrawlURL
from crawler.parser.url_normalizer import get_domain, normalize_url

logger = logging.getLogger(__name__)


class BloomFilter:
    """Simple Bloom filter backed by bitarray + mmh3."""

    __slots__ = ("_size", "_num_hashes", "_bits", "_count")

    def __init__(self, capacity: int = 100_000_000, error_rate: float = 0.001):
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


# Default cooldown when no checker is available (seconds)
_DEFAULT_COOLDOWN = 2.0


class MemoryFrontier(AbstractFrontier):
    """In-memory frontier with three-state domain scheduling.

    States:
      Ready:   domains with URLs, not in cooldown → pick highest yield first
      Cooling: domains with URLs, waiting for cooldown → auto-promote when ready
      Empty:   domains with no pending URLs → re-activate when URLs arrive

    get_next() is O(log N) — no scanning needed.
    """

    def __init__(
        self,
        bloom_capacity: int = 100_000_000,
        bloom_error_rate: float = 0.001,
        max_depth: int = -1,
        max_pending: int = 5_000_000,
    ):
        self._bloom = BloomFilter(bloom_capacity, bloom_error_rate)
        self._max_depth = max_depth
        self._max_pending = max_pending

        # Per-domain URL queues (min-heap by depth for BFS within domain)
        self._domain_queues: dict[str, list[CrawlURL]] = collections.defaultdict(list)

        # ── Three-state domain management ──

        # Ready: max-heap by yield score.
        # Stored as (-yield_score, tie_breaker, domain) for min-heap inversion.
        self._ready: list[tuple[float, int, str]] = []

        # Cooling: min-heap by ready_at timestamp.
        # Stored as (ready_at, tie_breaker, domain).
        self._cooling: list[tuple[float, int, str]] = []

        # Empty: domains with no pending URLs.
        self._empty: set[str] = set()

        # Track which state each domain is in (for O(1) lookup)
        # Values: "ready", "cooling", "empty", or absent (never seen)
        self._domain_state: dict[str, str] = {}

        # Yield scores: discovered_urls / pages_crawled per domain
        # Higher = more valuable for discovery
        self._domain_yield: dict[str, float] = {}

        # Monotonic tie-breaker to keep heap stable
        self._tie_counter = 0

        self._pending_count = 0
        self._lock = asyncio.Lock()

        # Cooldown checker: domain -> estimated wait seconds (0 = ready)
        self._cooldown_checker: Optional[Callable[[str], float]] = None

        # Profiling
        self._cooldown_skips = 0     # for backward compat with profiler
        self._promotions = 0         # cooling → ready transitions
        self._lock_wait_total: float = 0.0
        self._lock_hold_total: float = 0.0
        self._lock_acquisitions: int = 0
        self._urls_dropped: int = 0

    # ── Configuration ──

    def set_cooldown_checker(self, checker: Callable[[str], float]) -> None:
        """Inject a function that returns estimated cooldown for a domain."""
        self._cooldown_checker = checker

    def update_domain_priority(self, domain: str, score: float) -> None:
        """Update the yield score for a domain."""
        self._domain_yield[domain] = score

    def update_domain_priorities(self, scores: dict[str, float]) -> None:
        """Batch update yield scores from profiler."""
        self._domain_yield.update(scores)

    # ── Heap helpers ──

    def _next_tie(self) -> int:
        self._tie_counter += 1
        return self._tie_counter

    def _push_ready(self, domain: str) -> None:
        """Push domain into the Ready heap."""
        score = self._domain_yield.get(domain, 1.0)
        heapq.heappush(self._ready, (-score, self._next_tie(), domain))
        self._domain_state[domain] = "ready"

    def _push_cooling(self, domain: str, cooldown_secs: float) -> None:
        """Push domain into the Cooling heap."""
        ready_at = time.monotonic() + cooldown_secs
        heapq.heappush(self._cooling, (ready_at, self._next_tie(), domain))
        self._domain_state[domain] = "cooling"

    def _set_empty(self, domain: str) -> None:
        """Mark domain as Empty."""
        self._empty.add(domain)
        self._domain_state[domain] = "empty"

    def _promote_cooling(self) -> None:
        """Move all expired Cooling domains → Ready."""
        now = time.monotonic()
        while self._cooling and self._cooling[0][0] <= now:
            _, _, domain = heapq.heappop(self._cooling)

            # Only promote if domain still has URLs
            if self._domain_queues.get(domain):
                self._push_ready(domain)
                self._promotions += 1
            else:
                self._set_empty(domain)

    # ── Core API ──

    async def add_url(
        self, url: str, depth: int = 0, priority: float = 0.0
    ) -> bool:
        """Add a URL to the frontier.

        Returns True if the URL was newly seen (for discovered count),
        even if it was dropped due to frontier cap.
        """
        normalized = normalize_url(url)
        if normalized is None:
            return False
        if self._max_depth >= 0 and depth > self._max_depth:
            return False

        async with self._lock:
            if not self._bloom.add(normalized):
                return False  # duplicate — not new

            # Frontier cap: URL is new (counted as discovered),
            # but don't queue if we're over capacity
            if self._pending_count >= self._max_pending:
                self._urls_dropped += 1
                return True  # still counts as "discovered"

            domain = get_domain(normalized) or "unknown"

            crawl_url = CrawlURL(
                url=normalized, domain=domain, depth=depth, priority=float(depth)
            )
            heapq.heappush(self._domain_queues[domain], crawl_url)
            self._pending_count += 1

            # State transition: if domain was Empty → move to Ready
            state = self._domain_state.get(domain)
            if state is None or state == "empty":
                self._empty.discard(domain)
                self._push_ready(domain)
            # If Cooling: stays Cooling (will promote when cooldown expires)
            # If Ready: stays Ready (already in heap)

            return True

    async def add_urls(self, urls: list[str], depth: int = 0) -> int:
        count = 0
        for url in urls:
            if await self.add_url(url, depth=depth):
                count += 1
        return count

    async def get_next(self) -> Optional[CrawlURL]:
        """Get the next URL to crawl.

        O(log N) — no domain scanning needed:
          1. Promote expired Cooling → Ready
          2. Pop highest-yield domain from Ready heap
          3. Pop URL from that domain's queue
          4. Move domain → Cooling (or Empty if queue exhausted)
        """
        t_wait_start = time.monotonic()
        async with self._lock:
            t_acquired = time.monotonic()
            self._lock_wait_total += t_acquired - t_wait_start
            self._lock_acquisitions += 1

            # Step 1: promote expired cooling domains
            self._promote_cooling()

            # Step 2: find a ready domain with URLs
            # (stale entries in heap are possible if yield scores changed)
            while self._ready:
                neg_score, _, domain = heapq.heappop(self._ready)

                queue = self._domain_queues.get(domain)
                if not queue:
                    # Domain was emptied by another path — mark empty
                    self._set_empty(domain)
                    continue

                # Check if this entry is stale (domain moved to cooling/empty)
                if self._domain_state.get(domain) != "ready":
                    continue

                # Step 3: pop URL from domain's queue
                item = heapq.heappop(queue)
                self._pending_count -= 1

                # Step 4: transition domain state
                if queue:
                    # Domain still has URLs → move to Cooling
                    cooldown = _DEFAULT_COOLDOWN
                    if self._cooldown_checker:
                        cooldown = max(self._cooldown_checker(domain), _DEFAULT_COOLDOWN)
                    self._push_cooling(domain, cooldown)
                else:
                    # Domain exhausted → Empty
                    self._set_empty(domain)

                self._lock_hold_total += time.monotonic() - t_acquired
                return item

            # No ready domains — check if any cooling domains exist
            # If so, pop the soonest one (minimize worker idle time)
            if self._cooling:
                _, _, domain = heapq.heappop(self._cooling)
                queue = self._domain_queues.get(domain)
                if queue:
                    item = heapq.heappop(queue)
                    self._pending_count -= 1
                    self._cooldown_skips += 1  # counts as "forced skip of cooldown"

                    if queue:
                        self._push_cooling(domain, _DEFAULT_COOLDOWN)
                    else:
                        self._set_empty(domain)

                    self._lock_hold_total += time.monotonic() - t_acquired
                    return item

            self._lock_hold_total += time.monotonic() - t_acquired
            return None  # frontier truly empty

    # ── Metrics ──

    async def size(self) -> int:
        return self._pending_count

    async def total_seen(self) -> int:
        return len(self._bloom)

    async def domain_count(self) -> int:
        return sum(1 for q in self._domain_queues.values() if q)

    @property
    def cooldown_skips(self) -> int:
        return self._cooldown_skips

    @property
    def urls_dropped(self) -> int:
        return self._urls_dropped

    @property
    def lock_stats(self) -> dict:
        n = max(self._lock_acquisitions, 1)
        return {
            "lock_wait_total": self._lock_wait_total,
            "lock_hold_total": self._lock_hold_total,
            "lock_acquisitions": self._lock_acquisitions,
            "avg_lock_wait_ms": (self._lock_wait_total / n) * 1000,
            "avg_lock_hold_ms": (self._lock_hold_total / n) * 1000,
        }

    @property
    def state_stats(self) -> dict:
        """Three-state distribution."""
        return {
            "ready": len(self._ready),
            "cooling": len(self._cooling),
            "empty": len(self._empty),
            "promotions": self._promotions,
        }

    # ── Checkpoint ──

    async def save_checkpoint(self, path: str) -> None:
        async with self._lock:
            state = {
                "bloom": self._bloom,
                "domain_queues": dict(self._domain_queues),
                "ready": list(self._ready),
                "cooling": list(self._cooling),
                "empty": set(self._empty),
                "domain_state": dict(self._domain_state),
                "domain_yield": dict(self._domain_yield),
                "pending_count": self._pending_count,
                "tie_counter": self._tie_counter,
            }
            p = Path(path)
            p.parent.mkdir(parents=True, exist_ok=True)
            with open(p, "wb") as f:
                pickle.dump(state, f, protocol=pickle.HIGHEST_PROTOCOL)

    async def load_checkpoint(self, path: str) -> None:
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
            self._ready = state.get("ready", [])
            self._cooling = state.get("cooling", [])
            self._empty = state.get("empty", set())
            self._domain_state = state.get("domain_state", {})
            self._domain_yield = state.get("domain_yield", {})
            self._pending_count = state["pending_count"]
            self._tie_counter = state.get("tie_counter", 0)

            # Re-promote any cooling domains that expired during downtime
            self._promote_cooling()
