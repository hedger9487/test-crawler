import time

import pytest

from crawler.fetcher.base import CrawlResult
from crawler.frontier.base import CrawlURL
from crawler.orchestrator import CrawlOrchestrator
from crawler.utils.config import Config


class _FakeFrontier:
    def __init__(self, items):
        self._items = list(items)
        self.added = []
        self.seen_only = []
        self.requeued = []
        self.marked_acquired = []
        self.released = []

    async def get_next(self):
        if self._items:
            return self._items.pop(0)
        return None

    async def add_urls(self, urls, depth=0):
        urls = list(urls)
        self.added.append((urls, depth))
        return len(urls)

    async def mark_seen(self, urls):
        urls = list(urls)
        self.seen_only.append(urls)
        return len(urls)

    async def requeue_issued_url(self, item):
        self.requeued.append(item)
        self._items.append(item)
        return True

    async def mark_acquired(self, item):
        self.marked_acquired.append(item)
        return True

    async def release_issued_url(self, item, *, to_cooling=False):
        self.released.append((item, to_cooling))
        return True


class _FakeRobots:
    def __init__(self):
        self.get_sitemap_calls = 0
        self.fetch_sitemap_calls = 0

    def get_sitemap_urls(self, _domain):
        self.get_sitemap_calls += 1
        return []

    async def fetch_sitemap(self, _url, before_fetch=None):
        self.fetch_sitemap_calls += 1
        if before_fetch is not None:
            await before_fetch(_url)
        return []


class _RetryingFakeRobots(_FakeRobots):
    def __init__(self, responses):
        super().__init__()
        self._responses = list(responses)

    def get_sitemap_urls(self, _domain):
        self.get_sitemap_calls += 1
        return ["https://example.com/sitemap.xml"]

    async def fetch_sitemap(self, _url, before_fetch=None):
        self.fetch_sitemap_calls += 1
        if before_fetch is not None:
            await before_fetch(_url)
        return self._responses.pop(0) if self._responses else []


class _SitemapFakeRobots(_FakeRobots):
    def get_sitemap_urls(self, _domain):
        self.get_sitemap_calls += 1
        return ["https://blocked.com/sitemap.xml"]


class _FakePoliteness:
    def __init__(self, orchestrator, events, can_crawl=True, wait=0.0):
        self._orchestrator = orchestrator
        self._events = events
        self._can_crawl = can_crawl
        self._wait = wait
        self.robots = _FakeRobots()

    async def can_crawl(self, url):
        self._events.append(("can_crawl", url))
        if not self._can_crawl:
            self._orchestrator._shutdown = True
        return self._can_crawl

    async def wait_for_slot(self, url):
        self._events.append(("wait_for_slot", url))
        return self._wait


class _FakeFetcher:
    def __init__(self, orchestrator, events, result=None, exc: Exception | None = None):
        self._orchestrator = orchestrator
        self._events = events
        self._result = result
        self._exc = exc
        self.calls = 0

    async def fetch(self, url):
        self.calls += 1
        self._events.append(("fetch", url))
        if self._exc:
            self._orchestrator._shutdown = True
            raise self._exc
        self._orchestrator._shutdown = True
        return self._result


class _FakeStorage:
    def __init__(self):
        self.rows = []

    async def save_result(self, **kwargs):
        self.rows.append(kwargs)


class _FakeProfiler:
    def __init__(self):
        self.states = []
        self.robots_blocked = 0
        self.rate_waits = []
        self.fetch_records = []
        self.discovered = []

    def worker_set_state(self, worker_id, state, url="", domain=""):
        self.states.append((worker_id, state, url, domain))

    def record_robots_blocked(self, _domain):
        self.robots_blocked += 1

    def record_rate_wait(self, wait):
        self.rate_waits.append(wait)

    def record_fetch(self, domain, status, latency_ms, is_html, error=""):
        self.fetch_records.append((domain, status, latency_ms, is_html, error))

    def record_discovered(self, domain, new_count, total_links, from_sitemap=False):
        self.discovered.append((domain, new_count, total_links))


@pytest.mark.asyncio
async def test_worker_success_pipeline_enqueues_links():
    orchestrator = CrawlOrchestrator(Config())
    events = []
    orchestrator._frontier = _FakeFrontier(
        [CrawlURL(url="https://example.com/start", domain="example.com", depth=0)]
    )
    orchestrator._storage = _FakeStorage()
    orchestrator._profiler = _FakeProfiler()
    orchestrator._sitemap_fetched.add("example.com")
    orchestrator._politeness = _FakePoliteness(orchestrator, events, can_crawl=True, wait=0.12)
    orchestrator._fetcher = _FakeFetcher(
        orchestrator,
        events,
        result=CrawlResult(
            url="https://example.com/start",
            status=200,
            content_type="text/html",
            html="<a href='/next'>next</a>",
            elapsed_ms=15,
        ),
    )

    start = time.monotonic()
    await orchestrator._worker(worker_id=0, start_time=start, max_time=60)

    assert [event[0] for event in events] == ["can_crawl", "wait_for_slot", "fetch"]
    assert len(orchestrator._frontier.marked_acquired) == 1
    assert len(orchestrator._storage.rows) == 1
    assert orchestrator._storage.rows[0]["status"] == 200
    assert orchestrator._frontier.added == [(["https://example.com/next"], 1)]
    assert orchestrator._profiler.rate_waits == [0.12]
    assert any(state[1] == "parsing" for state in orchestrator._profiler.states)


@pytest.mark.asyncio
async def test_worker_robots_blocked_skips_fetch():
    orchestrator = CrawlOrchestrator(Config())
    events = []
    orchestrator._frontier = _FakeFrontier(
        [CrawlURL(url="https://blocked.com/a", domain="blocked.com", depth=0)]
    )
    orchestrator._storage = _FakeStorage()
    orchestrator._profiler = _FakeProfiler()
    orchestrator._politeness = _FakePoliteness(orchestrator, events, can_crawl=False)
    orchestrator._fetcher = _FakeFetcher(orchestrator, events, result=CrawlResult(url="x"))

    start = time.monotonic()
    await orchestrator._worker(worker_id=1, start_time=start, max_time=60)

    assert events == [("can_crawl", "https://blocked.com/a")]
    assert orchestrator._fetcher.calls == 0
    assert orchestrator._profiler.robots_blocked == 1
    assert len(orchestrator._frontier.released) == 1
    assert orchestrator._storage.rows == []


@pytest.mark.asyncio
async def test_worker_fetch_exception_saves_status_zero():
    orchestrator = CrawlOrchestrator(Config())
    events = []
    orchestrator._frontier = _FakeFrontier(
        [CrawlURL(url="https://err.com/a", domain="err.com", depth=2)]
    )
    orchestrator._storage = _FakeStorage()
    orchestrator._profiler = _FakeProfiler()
    orchestrator._sitemap_fetched.add("err.com")
    orchestrator._politeness = _FakePoliteness(orchestrator, events, can_crawl=True, wait=0.0)
    orchestrator._fetcher = _FakeFetcher(orchestrator, events, exc=RuntimeError("fetch boom"))

    start = time.monotonic()
    await orchestrator._worker(worker_id=2, start_time=start, max_time=60)

    assert [event[0] for event in events] == ["can_crawl", "wait_for_slot", "fetch"]
    assert len(orchestrator._frontier.marked_acquired) == 1
    assert len(orchestrator._storage.rows) == 1
    row = orchestrator._storage.rows[0]
    assert row["status"] == 0
    assert row["depth"] == 2
    assert "fetch boom" in row["error"]
    assert orchestrator._profiler.fetch_records[0][1] == 0


@pytest.mark.asyncio
async def test_worker_robots_blocked_still_runs_sitemap_once():
    orchestrator = CrawlOrchestrator(Config())
    events = []
    orchestrator._frontier = _FakeFrontier(
        [CrawlURL(url="https://blocked.com/a", domain="blocked.com", depth=0)]
    )
    orchestrator._storage = _FakeStorage()
    orchestrator._profiler = _FakeProfiler()
    orchestrator._politeness = _FakePoliteness(orchestrator, events, can_crawl=False, wait=0.2)
    orchestrator._politeness.robots = _SitemapFakeRobots()
    orchestrator._fetcher = _FakeFetcher(orchestrator, events, result=CrawlResult(url="x"))

    start = time.monotonic()
    await orchestrator._worker(worker_id=3, start_time=start, max_time=60)

    assert orchestrator._politeness.robots.get_sitemap_calls == 1
    assert orchestrator._politeness.robots.fetch_sitemap_calls == 1
    assert ("wait_for_slot", "https://blocked.com/sitemap.xml") in events
    assert orchestrator._profiler.robots_blocked == 1
    assert orchestrator._fetcher.calls == 0
    assert orchestrator._profiler.rate_waits == [0.2]
    assert len(orchestrator._frontier.released) == 1


@pytest.mark.asyncio
async def test_try_fetch_sitemap_retries_and_queues_urls():
    orchestrator = CrawlOrchestrator(Config())
    events = []
    orchestrator._frontier = _FakeFrontier([])
    orchestrator._storage = _FakeStorage()
    orchestrator._profiler = _FakeProfiler()
    orchestrator._politeness = _FakePoliteness(orchestrator, events, can_crawl=True, wait=0.05)
    orchestrator._politeness.robots = _RetryingFakeRobots(
        responses=[None, None, ["https://example.com/a", "https://example.com/b"]]
    )
    orchestrator._fetcher = _FakeFetcher(orchestrator, events, result=CrawlResult(url="x"))

    ok = await orchestrator._try_fetch_sitemap("example.com", current_depth=0)

    assert ok is True
    assert orchestrator._politeness.robots.fetch_sitemap_calls == 3
    assert orchestrator._frontier.added == [
        (["https://example.com/a", "https://example.com/b"], 1)
    ]
    assert orchestrator._profiler.discovered == [("example.com", 2, 2)]
    sitemap_waits = [event for event in events if event[0] == "wait_for_slot"]
    assert len(sitemap_waits) == 3


@pytest.mark.asyncio
async def test_worker_sitemap_then_requeue_then_fetch_page():
    orchestrator = CrawlOrchestrator(Config())
    events = []
    url = "https://example.com/start"
    orchestrator._frontier = _FakeFrontier([CrawlURL(url=url, domain="example.com", depth=0)])
    orchestrator._storage = _FakeStorage()
    orchestrator._profiler = _FakeProfiler()
    orchestrator._politeness = _FakePoliteness(orchestrator, events, can_crawl=True, wait=0.12)
    orchestrator._politeness.robots = _SitemapFakeRobots()
    orchestrator._fetcher = _FakeFetcher(
        orchestrator,
        events,
        result=CrawlResult(
            url=url,
            status=200,
            content_type="text/html",
            html="<a href='/next'>next</a>",
            elapsed_ms=15,
        ),
    )

    start = time.monotonic()
    await orchestrator._worker(worker_id=4, start_time=start, max_time=60)

    assert len(orchestrator._frontier.requeued) == 1
    assert len(orchestrator._frontier.released) == 1
    assert len(orchestrator._frontier.marked_acquired) == 1
    assert orchestrator._fetcher.calls == 1
    assert events.count(("fetch", url)) == 1
    assert ("wait_for_slot", "https://blocked.com/sitemap.xml") in events
