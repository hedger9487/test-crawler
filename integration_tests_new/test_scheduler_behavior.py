import asyncio
import time
from collections import defaultdict
from pathlib import Path

import pytest

from crawler.frontier.memory_frontier import MemoryFrontier
from crawler.politeness.rate_limiter import RateLimiter


@pytest.mark.asyncio
async def test_multi_worker_respects_per_domain_rate_limit():
    frontier = MemoryFrontier(
        bloom_capacity=10_000,
        bloom_error_rate=1e-9,
        max_pending=1000,
    )
    limiter = RateLimiter(max_qps=20.0)  # 50ms per domain
    frontier.set_cooldown_checker(limiter.peek_wait_time)

    total = 8
    await frontier.add_urls([f"https://one.com/{i}" for i in range(total)], depth=0)

    timestamps = defaultdict(list)
    processed = 0
    guard = asyncio.Lock()

    async def worker():
        nonlocal processed
        while True:
            async with guard:
                if processed >= total:
                    return
            item = await frontier.get_next()
            if item is None:
                await asyncio.sleep(0.001)
                continue
            await limiter.acquire(item.domain)
            await frontier.mark_acquired(item)
            timestamps[item.domain].append(time.monotonic())
            async with guard:
                processed += 1
                if processed >= total:
                    return

    await asyncio.gather(*[asyncio.create_task(worker()) for _ in range(4)])

    assert len(timestamps["one.com"]) == total
    diffs = [
        curr - prev
        for prev, curr in zip(timestamps["one.com"], timestamps["one.com"][1:])
    ]
    assert all(diff >= 0.035 for diff in diffs)


@pytest.mark.asyncio
async def test_scheduler_picks_all_ready_domains_before_forced_cooling():
    frontier = MemoryFrontier(
        bloom_capacity=10_000,
        bloom_error_rate=1e-9,
        max_pending=1000,
    )
    await frontier.add_urls(
        [
            "https://a.com/1",
            "https://b.com/1",
            "https://c.com/1",
            "https://a.com/2",
            "https://b.com/2",
            "https://c.com/2",
        ]
    )

    first_three = [await frontier.get_next() for _ in range(3)]
    for item in first_three:
        if item is not None:
            await frontier.mark_acquired(item)
    first_domains = {item.domain for item in first_three if item is not None}
    assert first_domains == {"a.com", "b.com", "c.com"}


@pytest.mark.asyncio
async def test_parallel_producers_trigger_frontier_backpressure():
    frontier = MemoryFrontier(
        bloom_capacity=10_000,
        bloom_error_rate=1e-9,
        max_pending=50,
    )

    async def producer(idx: int):
        urls = [f"https://d{idx}.com/{j}" for j in range(40)]
        await frontier.add_urls(urls)

    await asyncio.gather(*[producer(i) for i in range(6)])  # 240 unique URLs

    assert await frontier.size() == 50
    assert await frontier.total_seen() == 240
    assert frontier.urls_dropped == 190


@pytest.mark.asyncio
async def test_checkpoint_resume_preserves_pending_work(tmp_path: Path):
    checkpoint = tmp_path / "sched.pkl"
    frontier = MemoryFrontier(
        bloom_capacity=10_000,
        bloom_error_rate=1e-9,
        max_pending=1000,
    )
    await frontier.add_urls([f"https://r.com/{i}" for i in range(10)])

    consumed = []
    for _ in range(3):
        item = await frontier.get_next()
        assert item is not None
        consumed.append(item)
        await frontier.mark_acquired(item)
    pending_before = await frontier.size()

    await frontier.save_checkpoint(str(checkpoint))

    restored = MemoryFrontier(
        bloom_capacity=10_000,
        bloom_error_rate=1e-9,
        max_pending=1000,
    )
    await restored.load_checkpoint(str(checkpoint))

    assert await restored.size() == pending_before
    resumed = 0
    while True:
        item = await restored.get_next()
        if item is None:
            break
        await restored.mark_acquired(item)
        resumed += 1

    assert resumed == pending_before
