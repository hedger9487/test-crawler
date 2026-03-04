import asyncio

import pytest

import crawler.fetcher.robots_handler as robots_module
from crawler.fetcher.robots_handler import RobotsHandler


class _FakeResponse:
    def __init__(self, status: int, text: str):
        self.status = status
        self._text = text

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def text(self, errors="replace"):
        return self._text


class _FakeSession:
    def __init__(self, url_map=None, exc=None):
        self.url_map = url_map or {}
        self.exc = exc
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append(url)
        if self.exc:
            raise self.exc
        status, text = self.url_map[url]
        return _FakeResponse(status, text)


@pytest.mark.asyncio
async def test_no_session_disallows_all():
    robots = RobotsHandler(user_agent="UA", session=None)
    allowed = await robots.is_allowed("https://example.com/path")
    assert allowed is False


@pytest.mark.asyncio
async def test_4xx_robots_allows_all():
    session = _FakeSession(
        {"https://example.com/robots.txt": (404, "not found")}
    )
    robots = RobotsHandler(user_agent="UA", session=session)
    assert await robots.is_allowed("https://example.com/anything") is True


@pytest.mark.asyncio
async def test_5xx_robots_disallows_all():
    session = _FakeSession(
        {"https://example.com/robots.txt": (503, "server error")}
    )
    robots = RobotsHandler(user_agent="UA", session=session)
    assert await robots.is_allowed("https://example.com/anything") is False


@pytest.mark.asyncio
async def test_parse_rules_crawl_delay():
    robots_text = "\n".join(
        [
            "User-agent: *",
            "Disallow: /private",
            "Crawl-delay: 7",
        ]
    )
    session = _FakeSession(
        {"https://example.com/robots.txt": (200, robots_text)}
    )
    robots = RobotsHandler(user_agent="MyBot", session=session)

    assert await robots.is_allowed("https://example.com/public") is True
    assert await robots.is_allowed("https://example.com/private/x") is False
    assert await robots.get_crawl_delay("example.com") == 7.0


@pytest.mark.asyncio
async def test_cache_reuses_first_fetch():
    session = _FakeSession(
        {"https://example.com/robots.txt": (200, "User-agent: *\nAllow: /")}
    )
    robots = RobotsHandler(user_agent="UA", cache_ttl=99999, session=session)

    assert await robots.is_allowed("https://example.com/a") is True
    assert await robots.is_allowed("https://example.com/b") is True
    assert session.calls.count("https://example.com/robots.txt") == 1


@pytest.mark.asyncio
async def test_timeout_disallows_all():
    session = _FakeSession(exc=asyncio.TimeoutError())
    robots = RobotsHandler(user_agent="UA", session=session)
    assert await robots.is_allowed("https://example.com/x") is False


@pytest.mark.asyncio
async def test_cache_ttl_expiry_triggers_refetch(monkeypatch):
    now = [1000.0]

    def fake_monotonic():
        return now[0]

    monkeypatch.setattr(robots_module.time, "monotonic", fake_monotonic)

    session = _FakeSession(
        {"https://example.com/robots.txt": (200, "User-agent: *\nAllow: /")}
    )
    robots = RobotsHandler(user_agent="UA", cache_ttl=10, session=session)

    assert await robots.is_allowed("https://example.com/a") is True
    now[0] += 5.0
    assert await robots.is_allowed("https://example.com/b") is True
    now[0] += 11.0
    assert await robots.is_allowed("https://example.com/c") is True

    assert session.calls.count("https://example.com/robots.txt") == 2


@pytest.mark.asyncio
async def test_extract_crawl_delay_respects_user_agent_blocks():
    """Crawl-delay parser should use the wildcard block when our UA
    doesn't match a specific block (not blindly take the first match)."""
    robots_text = "\n".join(
        [
            "User-agent: other",
            "Crawl-delay: 12",
            "User-agent: *",
            "Crawl-delay: 3",
        ]
    )
    session = _FakeSession(
        {"https://example.com/robots.txt": (200, robots_text)}
    )
    robots = RobotsHandler(user_agent="UA", session=session)
    await robots.is_allowed("https://example.com/a")
    # Our UA doesn't match "other", so fall back to wildcard (*) → 3.0
    assert await robots.get_crawl_delay("example.com") == 3.0
