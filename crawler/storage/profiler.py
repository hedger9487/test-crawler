"""Comprehensive crawler profiler and monitoring system.

Tracks every aspect of crawler performance:
  - Worker utilization (active vs idle/waiting)
  - Fetch latency distribution (p50, p95, p99)
  - Status code distribution
  - Domain yield: how many new URLs each domain produces
  - Content type distribution
  - Rate limiter wait time
  - Robots.txt block rate
  - Discovery efficiency: new URLs per page crawled
  - Top producing domains
"""

from __future__ import annotations

import asyncio
import collections
import json
import logging
import math
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, List, Set

logger = logging.getLogger(__name__)


@dataclass
class WorkerSnapshot:
    """A snapshot of what a worker is doing."""
    state: str = "idle"  # idle, waiting_frontier, waiting_robots, waiting_rate_limit, fetching, parsing
    current_url: str = ""
    current_domain: str = ""
    state_since: float = 0.0


class CrawlProfiler:
    """Comprehensive profiling and monitoring for the crawler.

    Collects fine-grained metrics and periodically writes reports.
    """

    def __init__(
        self,
        num_workers: int,
        report_interval: float = 30.0,
        report_path: str = "crawl_profile.jsonl",
    ):
        self._num_workers = num_workers
        self._report_interval = report_interval
        self._report_path = report_path
        self._start_time = time.monotonic()

        # ── Worker state tracking ──
        self._worker_states: Dict[int, WorkerSnapshot] = {
            i: WorkerSnapshot() for i in range(num_workers)
        }

        # ── Counters ──
        self._urls_crawled = 0
        self._urls_success = 0
        self._urls_error = 0
        self._urls_timeout = 0
        self._urls_discovered = 0   # total new unique URLs found
        self._robots_blocked = 0
        self._content_skipped = 0   # non-HTML skipped

        # ── Status code distribution ──
        self._status_counts: Dict[int, int] = collections.defaultdict(int)
        # domain -> status_code -> count
        self._status_by_domain: Dict[str, Dict[int, int]] = collections.defaultdict(
            lambda: collections.defaultdict(int)
        )

        # ── Domain-level stats ──
        # domain -> number of total crawl attempts (success + failure)
        self._domain_crawl_count: Dict[str, int] = collections.defaultdict(int)
        # domain -> new internal URLs discovered (same domain as the crawled page)
        self._domain_yield_internal: Dict[str, int] = collections.defaultdict(int)
        # domain -> new external URLs discovered (different domain from the crawled page)
        self._domain_yield_external: Dict[str, int] = collections.defaultdict(int)
        # domain -> total fetch time in seconds (accumulated across all attempts)
        self._domain_total_time_s: Dict[str, float] = collections.defaultdict(float)
        # domain -> number of errors
        self._domain_error_count: Dict[str, int] = collections.defaultdict(int)
        # domain -> number of robots.txt blocks
        self._domain_robots_blocked: Dict[str, int] = collections.defaultdict(int)
        # domain -> set of distinct domains that have linked to it (in-degree referrers)
        self._domain_in_degree_referrers: Dict[str, Set[str]] = collections.defaultdict(set)

        # ── Latency tracking (in ms) ──
        self._latencies: collections.deque = collections.deque(maxlen=10_000)

        # ── Rate limiter wait tracking (in seconds) ──
        self._total_rate_wait: float = 0.0
        self._rate_wait_count: int = 0

        # ── Issue→acquire gap tracking (seconds) ──
        self._issue_acquire_gaps: collections.deque = collections.deque(maxlen=10_000)

        # ── Frontier scheduling ──
        self._frontier_cooldown_skips: int = 0
        self._frontier_ref = None  # set via set_frontier_ref()

        # ── Time-series for analysis ──
        # List of (timestamp, urls_crawled, urls_discovered, urls_success)
        self._time_series: list[tuple[float, int, int, int]] = []

        self._report_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

    def set_frontier_ref(self, frontier) -> None:
        """Set a reference to the frontier for pulling cooldown skip stats."""
        self._frontier_ref = frontier

    # ── Worker state tracking ──

    def worker_set_state(self, worker_id: int, state: str,
                         url: str = "", domain: str = "") -> None:
        """Update a worker's current state."""
        snap = self._worker_states.get(worker_id)
        if snap:
            snap.state = state
            snap.current_url = url
            snap.current_domain = domain
            snap.state_since = time.monotonic()

    # ── Recording events ──

    def record_fetch(self, domain: str, status: int, latency_ms: float,
                     is_html: bool, error: str = "") -> None:
        """Record the result of a fetch attempt."""
        self._urls_crawled += 1
        self._status_counts[status] += 1
        self._status_by_domain[domain][status] += 1
        self._domain_crawl_count[domain] += 1
        self._domain_total_time_s[domain] += latency_ms / 1000.0

        if status == 200:
            self._urls_success += 1
        elif error == "timeout":
            self._urls_timeout += 1
            self._domain_error_count[domain] += 1
        elif status == 0 or error:
            self._urls_error += 1
            self._domain_error_count[domain] += 1

        if status == 200 and not is_html:
            self._content_skipped += 1

        # Track latency (only for completed requests)
        if latency_ms > 0:
            self._latencies.append(latency_ms)

    def record_discovered(
        self, domain: str, internal_new: int, external_new: int,
    ) -> None:
        """Record URLs discovered from a crawled page.

        Args:
            domain:       The domain whose page was crawled.
            internal_new: New URLs pointing to the same domain (same-site links).
            external_new: New URLs pointing to other domains (cross-site links).
        """
        self._urls_discovered += internal_new + external_new
        self._domain_yield_internal[domain] += internal_new
        self._domain_yield_external[domain] += external_new

    def record_outlinks(
        self, source_domain: str, dest_domains: Set[str],
    ) -> None:
        """Record that source_domain linked to each domain in dest_domains.

        Updates in-degree referrers for each destination, enabling
        authority scoring via log10(10 + in_degree).
        """
        for dest in dest_domains:
            if dest and dest != source_domain:
                self._domain_in_degree_referrers[dest].add(source_domain)

    def record_robots_blocked(self, domain: str) -> None:
        self._robots_blocked += 1
        self._domain_robots_blocked[domain] += 1

    def record_rate_wait(self, wait_seconds: float) -> None:
        self._total_rate_wait += wait_seconds
        self._rate_wait_count += 1

    def record_issue_acquire_gap(self, gap_seconds: float) -> None:
        """Record the time gap between frontier dispatch and rate limiter acquire."""
        self._issue_acquire_gaps.append(gap_seconds)

    # ── Domain scoring (for crawl strategy) ──

    def get_domain_score(self, domain: str) -> float:
        """Compute scheduling priority score for a domain.

        score = Y_weighted / T  ×  log10(10 + I)

          Y_weighted = Y_ext × 10 + Y_int
            External links are weighted 10× higher than internal links:
            a domain that consistently links OUT to other sites is a
            hub — more valuable for broad discovery than a deep-nesting
            site that only links to itself.

          T = total fetch time (seconds) across all attempts
            Failed attempts / timeouts burn T without contributing to Y,
            so error-heavy domains are penalised automatically.

          I = in-degree (number of distinct domains that link to this one)
            log10(10 + I) gives diminishing-return authority boost:
              I=0  → 1.0×,  I=90  → 2.0×,  I=990  → 3.0×

        Returns 1.0 for unexplored domains (neutral / Explore tier entry).
        """
        total_time_s = self._domain_total_time_s.get(domain, 0.0)
        if total_time_s == 0.0:
            return 1.0  # unexplored — neutral score, will enter Explore tier anyway

        y_int = self._domain_yield_internal.get(domain, 0)
        y_ext = self._domain_yield_external.get(domain, 0)
        weighted_yield = y_ext * 10 + y_int

        referrers = self._domain_in_degree_referrers.get(domain, set())
        authority = math.log10(10 + len(referrers))

        return (weighted_yield / total_time_s) * authority

    def get_top_domains(self, n: int = 20) -> List[Dict]:
        """Get top N domains by score (weighted URLs/sec × authority)."""
        all_domains = (
            set(self._domain_crawl_count)
            | set(self._domain_yield_internal)
            | set(self._domain_yield_external)
        )
        scored = []
        for domain in all_domains:
            crawls = self._domain_crawl_count.get(domain, 0)
            y_int = self._domain_yield_internal.get(domain, 0)
            y_ext = self._domain_yield_external.get(domain, 0)
            errors = self._domain_error_count.get(domain, 0)
            in_degree = len(self._domain_in_degree_referrers.get(domain, set()))
            scored.append({
                "domain": domain,
                "crawls": crawls,
                "yield_int": y_int,
                "yield_ext": y_ext,
                "errors": errors,
                "in_degree": in_degree,
                "urls_per_sec": round(self.get_domain_score(domain), 3),
            })
        scored.sort(key=lambda x: x["urls_per_sec"], reverse=True)
        return scored[:n]

    def get_most_crawled_domains(self, n: int = 5) -> List[Dict]:
        """Get top N domains by crawl count."""
        rows = []
        for domain, crawls in self._domain_crawl_count.items():
            success = self._status_by_domain[domain].get(200, 0)
            in_degree = len(self._domain_in_degree_referrers.get(domain, set()))
            rows.append({
                "domain": domain,
                "crawls": crawls,
                "success": success,
                "success_rate": (success / crawls * 100) if crawls > 0 else 0.0,
                "in_degree": in_degree,
                "urls_per_sec": round(self.get_domain_score(domain), 3),
            })
        rows.sort(key=lambda x: x["crawls"], reverse=True)
        return rows[:n]

    # ── Latency percentiles ──

    def _percentile(self, data: list[float], p: float) -> float:
        if not data:
            return 0.0
        sorted_data = sorted(data)
        k = (len(sorted_data) - 1) * (p / 100.0)
        f = int(k)
        c = min(f + 1, len(sorted_data) - 1)
        d = k - f
        return sorted_data[f] + d * (sorted_data[c] - sorted_data[f])

    # ── Reporting ──

    async def start(self) -> None:
        self._start_time = time.monotonic()
        self._report_task = asyncio.create_task(self._report_loop())

    async def stop(self) -> None:
        if self._report_task:
            self._report_task.cancel()
            try:
                await self._report_task
            except asyncio.CancelledError:
                pass
        await self._print_report(final=True)

    async def _report_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._report_interval)
                await self._print_report()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("Profiler report error: %s", e)

    async def _print_report(self, final: bool = False) -> None:
        # Sync frontier stats and push yield scores
        frontier_dropped = 0
        frontier_states = {}
        if self._frontier_ref:
            self._frontier_cooldown_skips = self._frontier_ref.cooldown_skips
            frontier_dropped = getattr(self._frontier_ref, 'urls_dropped', 0)
            frontier_states = getattr(self._frontier_ref, 'state_stats', {})
            if hasattr(frontier_states, '__call__'):
                frontier_states = {}

            # Push yield scores to frontier for priority scheduling
            scores = {}
            for domain in self._domain_crawl_count:
                scores[domain] = self.get_domain_score(domain)
            if scores:
                self._frontier_ref.update_domain_priorities(scores)

        elapsed = time.monotonic() - self._start_time
        hours = int(elapsed // 3600)
        minutes = int((elapsed % 3600) // 60)
        seconds = int(elapsed % 60)

        overall_qps = self._urls_crawled / elapsed if elapsed > 0 else 0.0
        success_rate = (self._urls_success / max(self._urls_crawled, 1)) * 100
        discovery_rate = self._urls_discovered / max(self._urls_success, 1)

        # Worker utilization
        worker_states = collections.Counter(
            snap.state for snap in self._worker_states.values()
        )
        n = max(self._num_workers, 1)
        active_pct = (worker_states.get("fetching", 0) + worker_states.get("parsing", 0)) / n * 100
        waiting_robots_pct = worker_states.get("waiting_robots", 0) / n * 100
        waiting_rate_pct = worker_states.get("waiting_rate_limit", 0) / n * 100
        idle_pct = (worker_states.get("idle", 0) + worker_states.get("waiting_frontier", 0)) / n * 100

        # Latency percentiles
        recent_latencies = list(self._latencies)[-1000:]  # last 1000 for recency
        p50 = self._percentile(recent_latencies, 50)
        p95 = self._percentile(recent_latencies, 95)
        p99 = self._percentile(recent_latencies, 99)

        # Status distribution (top 5)
        status_sorted = sorted(self._status_counts.items(), key=lambda x: -x[1])[:8]
        status_str = "  ".join(f"{s}:{c}" for s, c in status_sorted) if status_sorted else "none"

        # Average rate limiter wait
        avg_rate_wait = self._total_rate_wait / max(self._rate_wait_count, 1)

        # Top 5 domains by yield
        top_domains = self.get_top_domains(5)
        top_crawled = self.get_most_crawled_domains(5)

        header = "═══ FINAL CRAWL PROFILE ═══" if final else "─── CRAWL PROFILE ───"
        sep = "═" * 50 if final else "─" * 50

        report = f"""
{header}
  Time:       {hours:02d}:{minutes:02d}:{seconds:02d}   QPS: {overall_qps:.2f}

  📊 THROUGHPUT
    Crawled:      {self._urls_crawled:>10,}
    Successful:   {self._urls_success:>10,}   ({success_rate:.1f}%)
    Errors:       {self._urls_error:>10,}
    Timeouts:     {self._urls_timeout:>10,}
    Robots blk:   {self._robots_blocked:>10,}

  🔗 DISCOVERY
    New URLs:     {self._urls_discovered:>10,}
    Dropped:      {frontier_dropped:>10,}   (frontier cap reached)
    Yield/page:   {discovery_rate:>10.1f}   (new URLs per successful page)
    Domains:      {len(self._domain_crawl_count):>10,}

  ⏱  LATENCY (ms)
    p50: {p50:>8.0f}   p95: {p95:>8.0f}   p99: {p99:>8.0f}

  👷 WORKERS ({self._num_workers} total)
    Active:       {active_pct:>8.1f}%   (fetching/parsing)
    Robots:       {waiting_robots_pct:>8.1f}%   (waiting for robots.txt)
    Rate wait:    {waiting_rate_pct:>8.1f}%
    Idle:         {idle_pct:>8.1f}%
    Avg wait:     {avg_rate_wait:>8.2f}s per acquire
    CD skips:     {self._frontier_cooldown_skips:>8,}   (forced cooling domain picks)
    Conn errs:    {self._status_counts.get(0, 0):>8,}   (status=0, network failures)

  🔄 FRONTIER STATES
    Ready:        {frontier_states.get('ready', 0):>8,}   (available for fetch)
    Reserved:     {frontier_states.get('reserved', 0):>8,}   (issued to workers)
    Cooling:      {frontier_states.get('cooling', 0):>8,}   (waiting for cooldown)
    Empty:        {frontier_states.get('empty', 0):>8,}   (no pending URLs)
    Promotions:   {frontier_states.get('promotions', 0):>8,}   (cooling → ready)"""

        # Diagnostic section
        diag_parts = []
        if self._frontier_ref:
            issued = getattr(self._frontier_ref, '_issued_domains', [])
            unique_10k = len(set(issued)) if issued else 0
            stale_pops = getattr(self._frontier_ref, '_stale_pops', 0)
            diag_parts.append(f"    Stale pops:   {stale_pops:>8,}   (gen-mismatch skips)")
            diag_parts.append(f"    Unique/10K:   {unique_10k:>8,}   (unique domains in last 10K issued)")
        if self._issue_acquire_gaps:
            gaps = list(self._issue_acquire_gaps)
            avg_gap = sum(gaps) / len(gaps)
            p95_gap = self._percentile(gaps, 95)
            diag_parts.append(f"    Issue→Acq:   {avg_gap:>7.3f}s avg, {p95_gap:.3f}s p95  (frontier→rate_limiter gap)")
        if diag_parts:
            report += "\n\n  🔬 DIAGNOSTICS\n" + "\n".join(diag_parts)

        report += f"""

  📈 STATUS CODES
    {status_str}

  🏆 TOP DOMAINS (by URLs/sec)"""

        for d in top_domains:
            report += (
                f"\n    {d['domain'][:33]:<33} "
                f"ext:{d['yield_ext']:>5}  int:{d['yield_int']:>5}  "
                f"in⇐:{d['in_degree']:>4}  "
                f"crawls:{d['crawls']:>4}  ups:{d['urls_per_sec']:.2f}"
            )

        report += f"""

  🔢 TOP CRAWLED DOMAINS"""
        for d in top_crawled:
            report += (
                f"\n    {d['domain'][:33]:<33} "
                f"crawls:{d['crawls']:>5}  "
                f"succ:{d['success_rate']:>5.1f}%  "
                f"in⇐:{d['in_degree']:>4}  "
                f"ups:{d['urls_per_sec']:>6.2f}"
            )

        report += f"\n{sep}"
        logger.info(report)

        # Record time series point
        self._time_series.append((
            elapsed, self._urls_crawled, self._urls_discovered, self._urls_success
        ))

        # Write JSONL snapshot for post-analysis
        if final:
            await self._write_final_report()

    async def _write_final_report(self) -> None:
        """Write a detailed JSON report for post-crawl analysis."""
        try:
            report = {
                "summary": {
                    "elapsed_seconds": time.monotonic() - self._start_time,
                    "total_crawled": self._urls_crawled,
                    "total_success": self._urls_success,
                    "total_errors": self._urls_error,
                    "total_timeouts": self._urls_timeout,
                    "total_discovered": self._urls_discovered,
                    "total_robots_blocked": self._robots_blocked,
                    "unique_domains": len(self._domain_crawl_count),
                    "success_rate": self._urls_success / max(self._urls_crawled, 1),
                    "yield_per_page": self._urls_discovered / max(self._urls_success, 1),
                },
                "status_distribution": dict(self._status_counts),
                "latency": {
                    "p50": self._percentile(self._latencies, 50),
                    "p95": self._percentile(self._latencies, 95),
                    "p99": self._percentile(self._latencies, 99),
                    "avg": sum(self._latencies) / max(len(self._latencies), 1),
                },
                "rate_limiter": {
                    "total_wait_seconds": self._total_rate_wait,
                    "avg_wait_seconds": self._total_rate_wait / max(self._rate_wait_count, 1),
                    "total_acquires": self._rate_wait_count,
                },
                "top_domains": self.get_top_domains(50),
                "top_crawled_domains": self.get_most_crawled_domains(50),
                "time_series": [
                    {"elapsed": t, "crawled": c, "discovered": d, "success": s}
                    for t, c, d, s in self._time_series
                ],
            }
            path = Path(self._report_path)
            path.write_text(json.dumps(report, indent=2, ensure_ascii=False))
            logger.info("📄 Detailed profile saved to: %s", path)
        except Exception as e:
            logger.error("Failed to write profile report: %s", e)

    @property
    def summary(self) -> dict:
        """Comprehensive summary for benchmark comparison."""
        elapsed = time.monotonic() - self._start_time
        total_crawled = max(self._urls_crawled, 1)
        conn_errors = self._status_counts.get(0, 0)

        # Worker state time-weighted averages (approximate from snapshots)
        worker_states = collections.Counter(
            snap.state for snap in self._worker_states.values()
        )
        n = max(self._num_workers, 1)

        # Frontier lock stats
        frontier_lock = {}
        if self._frontier_ref and hasattr(self._frontier_ref, 'lock_stats'):
            frontier_lock = self._frontier_ref.lock_stats

        return {
            # Throughput
            "crawled": self._urls_crawled,
            "success": self._urls_success,
            "errors": self._urls_error,
            "timeouts": self._urls_timeout,
            "discovered": self._urls_discovered,
            "robots_blocked": self._robots_blocked,
            "elapsed_seconds": elapsed,
            "qps": self._urls_crawled / elapsed if elapsed > 0 else 0.0,
            "success_rate": self._urls_success / total_crawled,
            # Effective QPS = only counting successful fetches
            "effective_qps": self._urls_success / elapsed if elapsed > 0 else 0.0,
            # Latency
            "p50_ms": self._percentile(self._latencies, 50),
            "p95_ms": self._percentile(self._latencies, 95),
            "p99_ms": self._percentile(self._latencies, 99),
            # Connection health
            "conn_errors": conn_errors,
            "conn_error_pct": conn_errors / total_crawled * 100,
            "timeout_pct": self._urls_timeout / total_crawled * 100,
            # Worker utilization
            "active_pct": (worker_states.get("fetching", 0) + worker_states.get("parsing", 0)) / n * 100,
            "robots_pct": worker_states.get("waiting_robots", 0) / n * 100,
            "rate_wait_pct": worker_states.get("waiting_rate_limit", 0) / n * 100,
            "idle_pct": (worker_states.get("idle", 0) + worker_states.get("waiting_frontier", 0)) / n * 100,
            # Rate limiter
            "avg_rate_wait_s": self._total_rate_wait / max(self._rate_wait_count, 1),
            # Frontier lock
            "frontier_lock": frontier_lock,
            "cd_skips": self._frontier_cooldown_skips,
        }
