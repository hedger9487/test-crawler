"""Crawl Orchestrator — the main crawl loop.

Manages worker coroutines, coordinates all components, and handles
graceful shutdown with checkpointing.

Integrates the CrawlProfiler for comprehensive monitoring and uses
domain yield data to dynamically prioritize productive domains.
"""

from __future__ import annotations

import asyncio
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

                if not allowed_by_robots:
                    self._profiler.record_robots_blocked(crawl_url.domain)
                    await self._frontier.release_issued_url(crawl_url)
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
