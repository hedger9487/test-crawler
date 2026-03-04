"""In-memory URL frontier with reserved-aware domain scheduling.

Domain states:
  Ready   → max-heap by yield score: domains available for immediate fetch
  Reserved → currently issued to a worker, waiting for acquire/finalize
  Cooling → min-heap by ready_at: domains waiting for rate-limit cooldown
  Empty   → set: domains with no pending URLs

Transitions:
  Ready → Reserved: after get_next() issues a URL to a worker
  Reserved → Cooling: worker successfully acquires rate slot (mark_acquired)
  Reserved → Ready: worker releases without fetch (release_issued_url)
  Cooling → Ready: when cooldown expires (promoted on next get_next())
  Reserved → Empty: no pending URLs when finalized
  Empty → Ready: add_url() adds a URL to an empty domain
  Cooling/Reserved + add_url: keep current state

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

# Queue1 (Ready) scheduling tiers:
#   New domain (never issued) > Warm-up (<3 issued) > Mature (yield-based)
_NEW_DOMAIN_PRIORITY = 3_000_000.0
_WARMUP_DOMAIN_PRIORITY = 2_000_000.0
_WARMUP_ISSUE_LIMIT = 3
_MAX_YPC_PRIORITY = 1_000_000.0
# Enforce at least 20% dispatches from non-new domains when available.
_NON_NEW_SLOT_EVERY = 5


class MemoryFrontier(AbstractFrontier):
    """In-memory frontier with Reserved-aware scheduling.

    States:
      Ready:   domains with URLs, available for immediate dispatch
      Reserved: domains currently issued to workers
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

        # ── Domain state management ──

        # Ready: max-heap by yield score.
        # Stored as (-yield_score, gen, tie_breaker, domain) for min-heap.
        # gen = per-domain generation to detect stale entries.
        self._ready: list[tuple[float, int, int, str]] = []

        # Cooling: min-heap by ready_at timestamp.
        # Stored as (ready_at, tie_breaker, domain).
        self._cooling: list[tuple[float, int, str]] = []

        # Empty: domains with no pending URLs.
        self._empty: set[str] = set()

        # Reserved: domain -> reservation_id currently issued to a worker.
        self._reserved: dict[str, int] = {}
        self._reservation_counter = 0

        # Track which state each domain is in (for O(1) lookup)
        # Values: "ready", "reserved", "cooling", "empty", or absent (never seen)
        self._domain_state: dict[str, str] = {}

        # Per-domain generation counter: incremented each time domain is
        # pushed to Ready. Entries with old gen are stale and skipped.
        self._domain_ready_gen: dict[str, int] = collections.defaultdict(int)

        # Yield scores: discovered_urls / pages_crawled per domain
        # Higher = more valuable for discovery
        self._domain_yield: dict[str, float] = {}
        # Number of times this domain has been issued from Queue1.
        # Used for explore-first scheduling tiers.
        self._domain_issue_count: dict[str, int] = collections.defaultdict(int)
        self._ready_dispatch_count = 0
        self._needs_ready_rebuild = False

        # Monotonic tie-breaker to keep heap stable
        self._tie_counter = 0

        self._pending_count = 0
        self._lock = asyncio.Lock()

        # Cooldown checker: domain -> estimated wait seconds (0 = ready)
        self._cooldown_checker: Optional[Callable[[str], float]] = None

        # Profiling
        self._cooldown_skips = 0     # for backward compat with profiler
        self._promotions = 0         # cooling → ready transitions
        self._stale_pops = 0         # gen-mismatch stale entry skips
        self._lock_wait_total: float = 0.0
        self._lock_hold_total: float = 0.0
        self._lock_acquisitions: int = 0
        self._urls_dropped: int = 0
        # Track last 10K issued domains for uniqueness measurement
        self._issued_domains: collections.deque = collections.deque(maxlen=10_000)

    # ── Configuration ──

    def set_cooldown_checker(self, checker: Callable[[str], float]) -> None:
        """Inject a function that returns estimated cooldown for a domain."""
        self._cooldown_checker = checker

    def update_domain_priority(self, domain: str, score: float) -> None:
        """Update the yield score for a domain."""
        self._domain_yield[domain] = score
        self._needs_ready_rebuild = True

    def update_domain_priorities(self, scores: dict[str, float]) -> None:
        """Batch update yield scores from profiler."""
        self._domain_yield.update(scores)
        self._needs_ready_rebuild = True

    # ── Heap helpers ──

    def _next_tie(self) -> int:
        self._tie_counter += 1
        return self._tie_counter

    def _effective_priority(self, domain: str) -> float:
        """Compute Queue1 priority with strict tiering.

        Priority order:
          1) New domain (issue_count == 0)
          2) Warm-up domain (issue_count < 3)
          3) Mature domain (yield-based score)
        """
        issue_count = self._domain_issue_count.get(domain, 0)
        score = self._domain_yield.get(domain, 1.0)

        if issue_count == 0:
            return _NEW_DOMAIN_PRIORITY
        if issue_count < _WARMUP_ISSUE_LIMIT:
            return _WARMUP_DOMAIN_PRIORITY
        return min(score, _MAX_YPC_PRIORITY)

    def _push_ready(self, domain: str) -> None:
        """Push domain into the Ready heap."""
        self._reserved.pop(domain, None)
        score = self._effective_priority(domain)
        gen = self._domain_ready_gen[domain] + 1
        self._domain_ready_gen[domain] = gen
        heapq.heappush(self._ready, (-score, gen, self._next_tie(), domain))
        self._domain_state[domain] = "ready"

    def _start_reservation_locked(self, domain: str, item: CrawlURL) -> CrawlURL:
        """Mark domain as Reserved for this issued URL."""
        self._reservation_counter += 1
        rid = self._reservation_counter
        self._reserved[domain] = rid
        self._domain_state[domain] = "reserved"
        item.reservation_id = rid
        return item

    def _finalize_reserved_locked(self, domain: str, *, to_cooling: bool) -> None:
        """Finalize a reserved domain into Cooling or Ready/Empty."""
        self._reserved.pop(domain, None)
        queue = self._domain_queues.get(domain)
        if not queue:
            self._set_empty(domain)
            return

        if to_cooling:
            cooldown = _DEFAULT_COOLDOWN
            if self._cooldown_checker:
                cooldown = max(self._cooldown_checker(domain), _DEFAULT_COOLDOWN)
            self._push_cooling(domain, cooldown)
        else:
            self._push_ready(domain)

    def _issue_from_ready_domain_locked(self, domain: str) -> Optional[CrawlURL]:
        """Issue one URL from a ready domain while holding frontier lock.

        Issuing transitions domain Ready -> Reserved.
        """
        queue = self._domain_queues.get(domain)
        if not queue:
            self._set_empty(domain)
            return None
        if self._domain_state.get(domain) != "ready":
            return None

        item = heapq.heappop(queue)
        item.issue_time = time.monotonic()  # for issue→acquire gap
        self._pending_count -= 1
        self._issued_domains.append(domain)
        self._domain_issue_count[domain] += 1
        self._ready_dispatch_count += 1

        return self._start_reservation_locked(domain, item)

    def _push_cooling(self, domain: str, cooldown_secs: float) -> None:
        """Push domain into the Cooling heap."""
        self._reserved.pop(domain, None)
        ready_at = time.monotonic() + cooldown_secs
        heapq.heappush(self._cooling, (ready_at, self._next_tie(), domain))
        self._domain_state[domain] = "cooling"

    def _set_empty(self, domain: str) -> None:
        """Mark domain as Empty."""
        self._reserved.pop(domain, None)
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

    def _rebuild_ready_heap(self) -> None:
        """Rebuild Ready heap from current ready domains using latest scores."""
        if not self._needs_ready_rebuild:
            return

        entries: list[tuple[float, int, int, str]] = []
        to_empty: list[str] = []

        for domain, state in self._domain_state.items():
            if state != "ready":
                continue
            queue = self._domain_queues.get(domain)
            if not queue:
                to_empty.append(domain)
                continue
            score = self._effective_priority(domain)
            gen = self._domain_ready_gen[domain] + 1
            self._domain_ready_gen[domain] = gen
            entries.append((-score, gen, self._next_tie(), domain))

        for domain in to_empty:
            self._set_empty(domain)

        heapq.heapify(entries)
        self._ready = entries
        self._needs_ready_rebuild = False

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
            return self._add_normalized_locked(normalized, depth=depth)

    async def add_urls(self, urls: list[str], depth: int = 0) -> int:
        """Batch-add URLs under a single lock acquisition."""
        if self._max_depth >= 0 and depth > self._max_depth:
            return 0

        normalized_urls = []
        for url in urls:
            normalized = normalize_url(url)
            if normalized is not None:
                normalized_urls.append(normalized)

        count = 0
        async with self._lock:
            for normalized in normalized_urls:
                if self._add_normalized_locked(normalized, depth=depth):
                    count += 1
        return count

    def _add_normalized_locked(self, normalized: str, depth: int) -> bool:
        """Add a normalized URL while frontier lock is already held."""
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
        # If Cooling/Reserved: stays there.
        # If Ready: stays Ready (already in heap).

        return True

    async def get_next(self) -> Optional[CrawlURL]:
        """Get the next URL to crawl.

        O(log N) — no domain scanning needed:
          1. Promote expired Cooling → Ready
          2. Pop highest-yield domain from Ready heap
          3. Pop URL from that domain's queue
          4. Move domain → Reserved (finalized by worker later)
        """
        t_wait_start = time.monotonic()
        async with self._lock:
            t_acquired = time.monotonic()
            self._lock_wait_total += t_acquired - t_wait_start
            self._lock_acquisitions += 1

            # Step 1: promote expired cooling domains
            self._promote_cooling()
            self._rebuild_ready_heap()

            # Step 2: find a ready domain with URLs
            # (stale entries in heap are possible if yield scores changed)
            force_non_new = ((self._ready_dispatch_count + 1) % _NON_NEW_SLOT_EVERY == 0)
            for pass_idx in range(2):
                enforce_non_new = force_non_new and pass_idx == 0
                skipped_new: list[tuple[float, int, int, str]] = []

                while self._ready:
                    neg_score, gen, tie, domain = heapq.heappop(self._ready)

                    # Skip stale entries: gen must match current domain gen
                    if gen != self._domain_ready_gen.get(domain, 0):
                        self._stale_pops += 1
                        continue

                    if enforce_non_new and self._domain_issue_count.get(domain, 0) == 0:
                        skipped_new.append((neg_score, gen, tie, domain))
                        continue

                    item = self._issue_from_ready_domain_locked(domain)
                    if item is None:
                        continue

                    for entry in skipped_new:
                        heapq.heappush(self._ready, entry)

                    self._lock_hold_total += time.monotonic() - t_acquired
                    return item

                for entry in skipped_new:
                    heapq.heappush(self._ready, entry)

            # No ready domains — check if any cooling domains exist
            # If so, pop the soonest one (minimize worker idle time)
            if self._cooling:
                _, _, domain = heapq.heappop(self._cooling)
                queue = self._domain_queues.get(domain)
                if queue:
                    item = heapq.heappop(queue)
                    item.issue_time = time.monotonic()
                    self._pending_count -= 1
                    self._cooldown_skips += 1  # counts as "forced skip of cooldown"
                    self._issued_domains.append(domain)
                    self._domain_issue_count[domain] += 1
                    self._ready_dispatch_count += 1
                    item = self._start_reservation_locked(domain, item)

                    self._lock_hold_total += time.monotonic() - t_acquired
                    return item

            self._lock_hold_total += time.monotonic() - t_acquired
            return None  # frontier truly empty

    async def requeue_issued_url(self, item: CrawlURL) -> bool:
        """Put an already-issued URL back into the domain queue and release
        the domain reservation so it becomes dispatchable again.

        This is a single atomic operation: it restores the URL to the front
        of the domain queue AND transitions Reserved → Ready.  Callers do
        NOT need a separate release_issued_url() call afterwards.

        This must NOT hit bloom dedup; the URL is already known.
        """
        if item is None or not item.url:
            return False
        if self._max_depth >= 0 and item.depth > self._max_depth:
            return False

        async with self._lock:
            domain = item.domain or get_domain(item.url) or "unknown"
            restored = CrawlURL(
                url=item.url,
                domain=domain,
                depth=item.depth,
                priority=item.priority,
            )
            heapq.heappush(self._domain_queues[domain], restored)
            self._pending_count += 1

            state = self._domain_state.get(domain)
            if state == "reserved":
                # Verify this item still holds the reservation
                rid = self._reserved.get(domain)
                if rid is not None and rid != item.reservation_id:
                    # Stale reservation — another worker owns this domain.
                    # URL was re-enqueued; domain state unchanged.
                    return True
                self._finalize_reserved_locked(domain, to_cooling=False)
            elif state is None or state == "empty":
                self._empty.discard(domain)
                self._push_ready(domain)
            # Cooling or Ready: URL is queued; domain state stays as-is.
            return True

    async def mark_acquired(self, item: CrawlURL) -> bool:
        """Finalize Reserved -> Cooling/Empty after successful acquire()."""
        if item is None or not item.url:
            return False

        async with self._lock:
            domain = item.domain or get_domain(item.url) or "unknown"
            rid = self._reserved.get(domain)
            if rid is None or rid != item.reservation_id:
                return False

            self._finalize_reserved_locked(domain, to_cooling=True)
            return True

    async def release_issued_url(self, item: CrawlURL, *, to_cooling: bool = False) -> bool:
        """Release a Reserved domain without page fetch."""
        if item is None or not item.url:
            return False

        async with self._lock:
            domain = item.domain or get_domain(item.url) or "unknown"
            rid = self._reserved.get(domain)
            if rid is None or rid != item.reservation_id:
                return False

            self._finalize_reserved_locked(domain, to_cooling=to_cooling)
            return True

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
        """Domain state distribution."""
        return {
            "ready": len(self._ready),
            "reserved": len(self._reserved),
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
                "reserved": dict(self._reserved),
                "reservation_counter": self._reservation_counter,
                "domain_state": dict(self._domain_state),
                "domain_yield": dict(self._domain_yield),
                "domain_issue_count": dict(self._domain_issue_count),
                "domain_ready_gen": dict(self._domain_ready_gen),
                "ready_dispatch_count": self._ready_dispatch_count,
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
            self._reserved = dict(state.get("reserved", {}))
            self._reservation_counter = state.get("reservation_counter", 0)
            self._domain_state = state.get("domain_state", {})
            self._domain_yield = state.get("domain_yield", {})
            self._domain_issue_count = collections.defaultdict(
                int, state.get("domain_issue_count", {})
            )
            self._domain_ready_gen = collections.defaultdict(
                int, state.get("domain_ready_gen", {})
            )
            self._ready_dispatch_count = state.get("ready_dispatch_count", 0)
            self._pending_count = state["pending_count"]
            self._tie_counter = state.get("tie_counter", 0)

            # Reserved entries are not durable across process restarts.
            # Recover them to Ready/Empty based on pending queue state.
            if self._reserved:
                for domain in list(self._reserved.keys()):
                    queue = self._domain_queues.get(domain)
                    if queue:
                        self._push_ready(domain)
                    else:
                        self._set_empty(domain)
                self._reserved.clear()

            # Re-promote any cooling domains that expired during downtime
            self._promote_cooling()
