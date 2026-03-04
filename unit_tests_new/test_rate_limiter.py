import asyncio

import pytest

from crawler.politeness.rate_limiter import RateLimiter


@pytest.mark.asyncio
async def test_same_domain_second_acquire_waits():
    limiter = RateLimiter(max_qps=20.0)  # 50ms interval
    first = await limiter.acquire("example.com")
    second = await limiter.acquire("example.com")

    assert first < 0.03
    assert second >= 0.03


@pytest.mark.asyncio
async def test_different_domains_do_not_block_each_other():
    limiter = RateLimiter(max_qps=0.5)

    waits = await asyncio.gather(
        limiter.acquire("a.com"),
        limiter.acquire("b.com"),
    )
    assert waits[0] < 0.05
    assert waits[1] < 0.05


@pytest.mark.asyncio
async def test_crawl_delay_is_stricter_than_qps():
    limiter = RateLimiter(max_qps=10.0)  # 100ms default interval

    first = await limiter.acquire("delay.com", crawl_delay=0.25)
    second = await limiter.acquire("delay.com", crawl_delay=0.25)

    assert first < 0.05
    assert second >= 0.20


@pytest.mark.asyncio
async def test_peek_wait_time_reflects_recent_acquire():
    limiter = RateLimiter(max_qps=20.0)  # 50ms interval

    assert limiter.peek_wait_time("peek.com") == 0.0
    await limiter.acquire("peek.com")
    wait = limiter.peek_wait_time("peek.com")

    assert wait > 0.0
    assert wait <= 0.06


@pytest.mark.asyncio
async def test_idle_cleanup_removes_old_domain_buckets():
    limiter = RateLimiter(
        max_qps=100.0,
        idle_cleanup_seconds=0.05,
        cleanup_interval=0.02,
    )
    await limiter.start()

    await limiter.acquire("cleanup.com")
    assert limiter.active_domains == 1

    await asyncio.sleep(0.12)
    assert limiter.active_domains == 0

    await limiter.stop()
