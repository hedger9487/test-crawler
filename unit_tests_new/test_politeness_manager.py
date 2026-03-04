import pytest

from crawler.politeness.manager import PolitenessManager


class _FakeRobots:
    def __init__(self):
        self.allowed = True
        self.delay = 0.0
        self.checked_urls = []
        self.delay_queries = []

    async def is_allowed(self, url: str) -> bool:
        self.checked_urls.append(url)
        return self.allowed

    async def get_crawl_delay(self, domain: str):
        self.delay_queries.append(domain)
        return self.delay


class _FakeLimiter:
    def __init__(self):
        self.calls = []

    async def acquire(self, domain: str, crawl_delay=None):
        self.calls.append((domain, crawl_delay))
        return 0.123

    async def start(self):
        return None

    async def stop(self):
        return None


@pytest.mark.asyncio
async def test_can_crawl_delegates_to_robots():
    robots = _FakeRobots()
    limiter = _FakeLimiter()
    manager = PolitenessManager(robots, limiter)

    allowed = await manager.can_crawl("https://example.com/a")
    assert allowed is True
    assert robots.checked_urls == ["https://example.com/a"]


@pytest.mark.asyncio
async def test_wait_for_slot_passes_domain_and_crawl_delay():
    robots = _FakeRobots()
    robots.delay = 7.0
    limiter = _FakeLimiter()
    manager = PolitenessManager(robots, limiter)

    wait = await manager.wait_for_slot("https://example.com/path")
    assert wait == 0.123
    assert robots.delay_queries == ["example.com"]
    assert limiter.calls == [("example.com", 7.0)]


@pytest.mark.asyncio
async def test_wait_for_slot_invalid_url_returns_zero_and_no_acquire():
    robots = _FakeRobots()
    limiter = _FakeLimiter()
    manager = PolitenessManager(robots, limiter)

    wait = await manager.wait_for_slot("not-a-url")
    assert wait == 0.0
    assert limiter.calls == []
