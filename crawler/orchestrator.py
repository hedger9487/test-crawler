"""Crawl Orchestrator — the main crawl loop.

Manages worker coroutines, coordinates all components, and handles
graceful shutdown with checkpointing.

Integrates the CrawlProfiler for comprehensive monitoring and uses
domain yield data to dynamically prioritize productive domains.
"""

from __future__ import annotations

import asyncio
import collections
import logging
import signal
import time
from pathlib import Path
from typing import Optional

from crawler.fetcher.async_fetcher import AsyncFetcher
from crawler.fetcher.robots_handler import RobotsHandler
from crawler.frontier.base import CrawlURL
from crawler.frontier.memory_frontier import MemoryFrontier
from crawler.parser.html_parser import extract_links
from crawler.parser.url_normalizer import get_domain
from crawler.politeness.manager import PolitenessManager
from crawler.politeness.rate_limiter import RateLimiter
from crawler.storage.sqlite_storage import SQLiteStorage
from crawler.storage.profiler import CrawlProfiler
from crawler.utils.config import Config

logger = logging.getLogger(__name__)


class CrawlOrchestrator:
    """Coordinates all crawler components and manages the crawl lifecycle.

    Features:
      - N async worker coroutines with state tracking
      - Periodic checkpointing for crash recovery
      - Comprehensive profiling via CrawlProfiler
      - Dynamic domain priority adjustment (favor high-yield domains)
    """

    def __init__(self, config: Config):
        self._config = config
        self._shutdown = False

        # Components (initialized in run())
        self._frontier: Optional[MemoryFrontier] = None
        self._fetcher: Optional[AsyncFetcher] = None
        self._politeness: Optional[PolitenessManager] = None
        self._storage: Optional[SQLiteStorage] = None
        self._profiler: Optional[CrawlProfiler] = None

        # Sitemap tracking: domains we've already fetched sitemaps for
        self._sitemap_done: set[str] = set()
        self._sitemap_inflight: set[str] = set()
        self._sitemap_failures: dict[str, int] = collections.defaultdict(int)
        self._sitemap_next_retry_at: dict[str, float] = {}
        self._sitemap_lock = asyncio.Lock()
        self._sitemap_max_attempts = max(1, int(config.sitemap.max_attempts_per_domain))
        self._sitemap_fetch_retries = max(1, int(config.sitemap.fetch_retries_per_sitemap))
        self._sitemap_retry_base_seconds = max(0.0, float(config.sitemap.retry_base_seconds))
        self._sitemap_max_xml_per_domain = max(1, int(config.sitemap.max_xml_per_domain))
        self._sitemap_max_queue_per_sitemap = max(
            1, int(config.sitemap.max_queue_per_sitemap)
        )

        # Backward-compat alias used by existing tests
        self._sitemap_fetched = self._sitemap_done

    async def run(self, seed_urls: list[str], resume: bool = False) -> None:
        """Main entry point: initialize, seed, run workers, shutdown."""
        concurrency = self._config.crawler.concurrency
        logger.info("Starting crawler with %d seeds, concurrency=%d",
                     len(seed_urls), concurrency)

        # ── Initialize components ──
        self._frontier = MemoryFrontier(
            bloom_capacity=self._config.frontier.bloom_capacity,
            bloom_error_rate=self._config.frontier.bloom_error_rate,
            max_depth=self._config.frontier.max_depth,
            max_pending=self._config.frontier.max_pending,
        )

        self._fetcher = AsyncFetcher(
            user_agent=self._config.crawler.user_agent,
            timeout=self._config.fetcher.timeout,
            max_redirects=self._config.fetcher.max_redirects,
            pool_size=self._config.fetcher.connection_pool_size,
            per_host=self._config.fetcher.per_host_connections,
            accept_content_types=self._config.fetcher.accept_content_types,
            ssl_verify=self._config.fetcher.ssl_verify,
        )

        robots = RobotsHandler(
            user_agent=self._config.crawler.user_agent,
            cache_ttl=self._config.politeness.robots_cache_ttl,
            fetch_timeout=self._config.politeness.robots_fetch_timeout,
            ssl_verify=self._config.fetcher.ssl_verify,
        )

        rate_limiter = RateLimiter(max_qps=self._config.politeness.max_qps)

        self._politeness = PolitenessManager(robots, rate_limiter)

        self._storage = SQLiteStorage(
            db_path=self._config.storage.db_path,
            batch_size=self._config.storage.batch_size,
        )

        self._profiler = CrawlProfiler(
            num_workers=concurrency,
            report_interval=self._config.logging.stats_interval,
        )

        # ── Start everything ──
        await self._storage.init()
        await self._fetcher.start()

        # Share the fetcher's session with robots handler
        robots.set_session(self._fetcher.session)

        # Wire cooldown checker: frontier skips domains in cooldown
        self._frontier.set_cooldown_checker(rate_limiter.peek_wait_time)

        await self._politeness.start()
        await self._profiler.start()
        self._profiler.set_frontier_ref(self._frontier)

        # Register signal handlers for graceful shutdown
        loop = asyncio.get_running_loop()
        self._worker_tasks: list[asyncio.Task] = []
        self._got_signal = False

        def _handle_signal():
            if self._got_signal:
                # Second signal → force exit
                logger.warning("⚡ Second signal — forcing exit!")
                import os
                os._exit(1)
            self._got_signal = True
            logger.info("⚡ Shutdown signal — cancelling all workers...")
            self._shutdown = True
            for t in self._worker_tasks:
                t.cancel()

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _handle_signal)

        # ── Load checkpoint or seed ──
        checkpoint_path = self._config.frontier.checkpoint_path
        if resume and Path(checkpoint_path).exists():
            logger.info("Resuming from checkpoint: %s", checkpoint_path)
            await self._frontier.load_checkpoint(checkpoint_path)
            logger.info("Restored frontier: %d pending, %d seen",
                        await self._frontier.size(),
                        await self._frontier.total_seen())
        else:
            added = await self._frontier.add_urls(seed_urls, depth=0)
            logger.info("Seeded frontier with %d URLs (%d unique)", len(seed_urls), added)

        # ── Run workers ──
        max_time = self._config.crawler.max_time_seconds
        start_time = time.monotonic()

        checkpoint_task = asyncio.create_task(
            self._checkpoint_loop(checkpoint_path)
        )

        workers = [
            asyncio.create_task(self._worker(i, start_time, max_time))
            for i in range(concurrency)
        ]
        self._worker_tasks = workers

        try:
            await asyncio.gather(*workers)
        except asyncio.CancelledError:
            pass
        finally:
            checkpoint_task.cancel()
            try:
                await checkpoint_task
            except asyncio.CancelledError:
                pass

        # ── Shutdown ──
        await self._shutdown_all(checkpoint_path)

    async def _worker(self, worker_id: int, start_time: float, max_time: int) -> None:
        """Single worker coroutine: fetch → parse → enqueue.

        Reports its state to the profiler at each phase transition.
        """
        empty_count = 0
        max_empty_waits = 30  # give up after 60s of empty frontier

        while not self._shutdown:
            # Time budget check
            if time.monotonic() - start_time >= max_time:
                logger.info("Worker %d: time limit reached", worker_id)
                break

            # ── Phase: Get URL from frontier ──
            self._profiler.worker_set_state(worker_id, "waiting_frontier")
            crawl_url = await self._frontier.get_next()
            if crawl_url is None:
                self._profiler.worker_set_state(worker_id, "idle")
                empty_count += 1
                if empty_count >= max_empty_waits:
                    logger.info("Worker %d: frontier empty for too long, exiting", worker_id)
                    break
                await asyncio.sleep(2.0)
                continue
            empty_count = 0
            reservation_finalized = False

            try:
                # ── Phase: Robots check ──
                self._profiler.worker_set_state(
                    worker_id, "waiting_robots",
                    url=crawl_url.url, domain=crawl_url.domain
                )
                try:
                    allowed_by_robots = await self._politeness.can_crawl(crawl_url.url)
                except Exception as e:
                    logger.debug("Robots check error for %s: %s", crawl_url.url, e)
                    continue

                # ── Phase: Sitemap discovery (once per domain) ──
                # Skip if we're close to time limit (sitemap can take a while)
                sitemap_attempted = False
                sitemap_did_request = False
                time_left = max_time - (time.monotonic() - start_time)
                if time_left > 30 and await self._reserve_sitemap_attempt(crawl_url.domain):
                    sitemap_attempted = True
                    sitemap_did_request = True
                    try:
                        sitemap_ok = await asyncio.wait_for(
                            self._try_fetch_sitemap(crawl_url.domain, crawl_url.depth),
                            timeout=30.0,
                        )
                    except asyncio.TimeoutError:
                        logger.debug("Sitemap fetch timeout for %s", crawl_url.domain)
                        sitemap_ok = False
                    await self._finalize_sitemap_attempt(crawl_url.domain, sitemap_ok)

                if not allowed_by_robots:
                    self._profiler.record_robots_blocked(crawl_url.domain)
                    await self._frontier.release_issued_url(crawl_url)
                    reservation_finalized = True
                    continue

                # If sitemap work was done for this domain, defer page fetch:
                # put URL back and let worker take a fresh task.
                if sitemap_attempted:
                    await self._frontier.requeue_issued_url(crawl_url)
                    await self._frontier.release_issued_url(
                        crawl_url, to_cooling=sitemap_did_request
                    )
                    reservation_finalized = True
                    continue

                # ── Phase: Rate limit wait ──
                self._profiler.worker_set_state(
                    worker_id, "waiting_rate_limit",
                    url=crawl_url.url, domain=crawl_url.domain
                )
                try:
                    # Record issue→acquire gap (time between frontier dispatch and now)
                    if crawl_url.issue_time > 0:
                        gap = time.monotonic() - crawl_url.issue_time
                        self._profiler.record_issue_acquire_gap(gap)
                    wait_time = await self._politeness.wait_for_slot(crawl_url.url)
                    self._profiler.record_rate_wait(wait_time)
                    await self._frontier.mark_acquired(crawl_url)
                    reservation_finalized = True
                except Exception as e:
                    logger.debug("Rate limit error for %s: %s", crawl_url.url, e)
                    continue

                # ── Phase: Fetch ──
                self._profiler.worker_set_state(
                    worker_id, "fetching",
                    url=crawl_url.url, domain=crawl_url.domain
                )
                try:
                    result = await self._fetcher.fetch(crawl_url.url)
                except Exception as e:
                    logger.debug("Fetch error for %s: %s", crawl_url.url, e)
                    self._profiler.record_fetch(
                        crawl_url.domain, 0, 0, False, error=str(e)
                    )
                    await self._storage.save_result(
                        url=crawl_url.url,
                        domain=crawl_url.domain,
                        status=0,
                        depth=crawl_url.depth,
                        error=str(e),
                    )
                    continue

                # Record fetch result
                self._profiler.record_fetch(
                    domain=crawl_url.domain,
                    status=result.status,
                    latency_ms=result.elapsed_ms,
                    is_html=result.is_html,
                    error=result.error,
                )
                await self._storage.save_result(
                    url=crawl_url.url,
                    domain=crawl_url.domain,
                    status=result.status,
                    depth=crawl_url.depth,
                    content_type=result.content_type,
                    error=result.error,
                    redirect_url=result.redirect_url,
                )

                # ── Phase: Parse & enqueue ──
                if result.is_success and result.is_html and result.html:
                    self._profiler.worker_set_state(
                        worker_id, "parsing",
                        url=crawl_url.url, domain=crawl_url.domain
                    )
                    try:
                        links = extract_links(result.html, crawl_url.url)
                        new_count = await self._frontier.add_urls(
                            links, depth=crawl_url.depth + 1
                        )
                        self._profiler.record_discovered(
                            crawl_url.domain, new_count, len(links)
                        )
                    except Exception as e:
                        logger.debug("Parse/enqueue error for %s: %s", crawl_url.url, e)
            finally:
                if not reservation_finalized:
                    await self._frontier.release_issued_url(crawl_url)

        # Worker done
        self._profiler.worker_set_state(worker_id, "done")

    async def _reserve_sitemap_attempt(self, domain: str) -> bool:
        """Try reserving sitemap work for a domain (dedup + retry throttling)."""
        now = time.monotonic()
        async with self._sitemap_lock:
            if domain in self._sitemap_done or domain in self._sitemap_inflight:
                return False

            failures = self._sitemap_failures.get(domain, 0)
            if failures >= self._sitemap_max_attempts:
                self._sitemap_done.add(domain)
                return False

            retry_at = self._sitemap_next_retry_at.get(domain, 0.0)
            if now < retry_at:
                return False

            self._sitemap_inflight.add(domain)
            return True

    async def _finalize_sitemap_attempt(self, domain: str, success: bool) -> None:
        """Finalize sitemap attempt and apply bounded retry/backoff on failure."""
        now = time.monotonic()
        async with self._sitemap_lock:
            self._sitemap_inflight.discard(domain)
            if success:
                self._sitemap_done.add(domain)
                self._sitemap_next_retry_at.pop(domain, None)
                return

            failures = self._sitemap_failures.get(domain, 0) + 1
            self._sitemap_failures[domain] = failures

            if failures >= self._sitemap_max_attempts:
                self._sitemap_done.add(domain)
                self._sitemap_next_retry_at.pop(domain, None)
            else:
                backoff = self._sitemap_retry_base_seconds * (2 ** (failures - 1))
                self._sitemap_next_retry_at[domain] = now + backoff

    async def _wait_sitemap_slot(self, url: str) -> None:
        """Rate-limit sitemap requests exactly like normal page fetches."""
        wait_time = await self._politeness.wait_for_slot(url)
        self._profiler.record_rate_wait(wait_time)

    async def _try_fetch_sitemap(self, domain: str, current_depth: int) -> bool:
        """Fetch sitemap for a domain if declared in robots.txt.

        Strategy: queue up to 500 URLs per sitemap XML for crawling,
        and mark ALL remaining URLs as seen in bloom so BFS won't
        re-discover them later.
        """
        try:
            robots = self._politeness.robots
            sitemap_xml_urls = robots.get_sitemap_urls(domain)
            if not sitemap_xml_urls:
                return True

            had_failure = False

            for surl in sitemap_xml_urls[:self._sitemap_max_xml_per_domain]:
                page_urls: Optional[list[str]] = None
                for attempt in range(1, self._sitemap_fetch_retries + 1):
                    page_urls = await robots.fetch_sitemap(
                        surl, before_fetch=self._wait_sitemap_slot
                    )
                    if page_urls is not None:
                        break

                    if attempt < self._sitemap_fetch_retries:
                        backoff = self._sitemap_retry_base_seconds * (2 ** (attempt - 1))
                        await asyncio.sleep(backoff)

                if page_urls is None:
                    had_failure = True
                    logger.debug(
                        "Sitemap %s failed after %d attempts",
                        surl, self._sitemap_fetch_retries,
                    )
                    continue

                if not page_urls:
                    continue

                # Step 1: queue the first batch (enters bloom + queue)
                to_queue = page_urls[:self._sitemap_max_queue_per_sitemap]
                added = await self._frontier.add_urls(
                    to_queue, depth=current_depth + 1
                )

                # Step 2: mark the rest as seen in bloom only (no queue)
                remaining = page_urls[self._sitemap_max_queue_per_sitemap:]
                new_in_bloom = 0
                if remaining:
                    new_in_bloom = await self._frontier.mark_seen(remaining)

                total_new = added + new_in_bloom
                self._profiler.record_discovered(
                    domain, total_new, len(page_urls), from_sitemap=True
                )

                logger.debug(
                    "Sitemap %s: %d URLs found, %d queued, %d bloom-only",
                    surl, len(page_urls), added, new_in_bloom,
                )
            return not had_failure
        except Exception as e:
            logger.debug("Sitemap error for %s: %s", domain, e)
            return False


    def _signal_shutdown(self) -> None:
        """Legacy shutdown — prefer the inline handler in run()."""
        logger.info("⚡ Shutdown signal received — cancelling workers...")
        self._shutdown = True
        for t in getattr(self, '_worker_tasks', []):
            t.cancel()

    async def _checkpoint_loop(self, path: str) -> None:
        """Periodically save frontier state."""
        interval = self._config.frontier.checkpoint_interval
        while True:
            try:
                await asyncio.sleep(interval)
                if self._frontier:
                    await self._frontier.save_checkpoint(path)
                    pending = await self._frontier.size()
                    seen = await self._frontier.total_seen()
                    domains = await self._frontier.domain_count()
                    skips = self._frontier.cooldown_skips
                    logger.info(
                        "💾 Checkpoint: %d pending, %d seen, %d domains, %d cooldown skips",
                        pending, seen, domains, skips
                    )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("Checkpoint error: %s", e)

    async def _shutdown_all(self, checkpoint_path: str) -> None:
        """Graceful shutdown: flush, checkpoint, close."""
        logger.info("Shutting down...")

        if self._frontier:
            try:
                await self._frontier.save_checkpoint(checkpoint_path)
                logger.info("Final checkpoint saved.")
            except Exception as e:
                logger.error("Final checkpoint error: %s", e)

        if self._profiler:
            await self._profiler.stop()

        if self._storage:
            await self._storage.close()

        if self._politeness:
            await self._politeness.stop()

        if self._fetcher:
            await self._fetcher.stop()

        logger.info("Crawler shutdown complete.")
