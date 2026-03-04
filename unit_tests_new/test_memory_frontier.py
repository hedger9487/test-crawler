from pathlib import Path

import pytest

import crawler.frontier.memory_frontier as frontier_module
from crawler.frontier.memory_frontier import MemoryFrontier


@pytest.mark.asyncio
async def test_add_url_deduplicates_and_tracks_seen():
    frontier = MemoryFrontier(bloom_capacity=1_000, max_pending=100)
    assert await frontier.add_url("https://example.com/a") is True
    assert await frontier.add_url("https://example.com/a#frag") is False

    assert await frontier.size() == 1
    assert await frontier.total_seen() == 1


@pytest.mark.asyncio
async def test_three_state_transition_and_forced_cooling_pick():
    frontier = MemoryFrontier(bloom_capacity=1_000, max_pending=100)
    frontier.set_cooldown_checker(lambda domain: 5.0)

    await frontier.add_urls(
        ["https://example.com/1", "https://example.com/2"], depth=0
    )

    first = await frontier.get_next()
    assert first is not None
    stats_after_first = frontier.state_stats
    assert stats_after_first["reserved"] == 1

    assert await frontier.mark_acquired(first) is True
    assert frontier.state_stats["cooling"] == 1

    second = await frontier.get_next()
    assert second is not None
    assert second.url != first.url
    assert frontier.cooldown_skips >= 1
    assert frontier.state_stats["reserved"] == 1


@pytest.mark.asyncio
async def test_empty_domain_reactivates_to_ready_when_new_url_arrives():
    frontier = MemoryFrontier(bloom_capacity=1_000, max_pending=100)
    await frontier.add_url("https://reactivate.com/1")
    item = await frontier.get_next()
    assert item is not None
    assert await frontier.release_issued_url(item) is True
    assert frontier.state_stats["empty"] >= 1

    await frontier.add_url("https://reactivate.com/2")
    assert frontier.state_stats["empty"] == 0
    assert frontier.state_stats["ready"] >= 1


@pytest.mark.asyncio
async def test_max_pending_drops_new_urls_but_counts_as_seen():
    frontier = MemoryFrontier(bloom_capacity=1_000, max_pending=1)
    assert await frontier.add_url("https://a.com/1") is True
    assert await frontier.add_url("https://b.com/2") is True

    assert await frontier.size() == 1
    assert await frontier.total_seen() == 2
    assert frontier.urls_dropped == 1


@pytest.mark.asyncio
async def test_ready_heap_prefers_higher_domain_score():
    frontier = MemoryFrontier(bloom_capacity=1_000, max_pending=100)
    frontier.update_domain_priorities({"a.com": 1.0, "b.com": 10.0})
    frontier._domain_issue_count["a.com"] = 3
    frontier._domain_issue_count["b.com"] = 3

    await frontier.add_url("https://a.com/1")
    await frontier.add_url("https://b.com/1")

    first = await frontier.get_next()
    assert first is not None
    assert first.domain == "b.com"


@pytest.mark.asyncio
async def test_priority_update_reorders_existing_ready_domains():
    frontier = MemoryFrontier(bloom_capacity=1_000, max_pending=100)
    await frontier.add_url("https://a.com/1")
    await frontier.add_url("https://b.com/1")
    frontier._domain_issue_count["a.com"] = 3
    frontier._domain_issue_count["b.com"] = 3

    # Ready entries already exist in heap; update priorities afterward.
    frontier.update_domain_priorities({"a.com": 50.0, "b.com": 1.0})

    first = await frontier.get_next()
    assert first is not None
    assert first.domain == "a.com"


@pytest.mark.asyncio
async def test_checkpoint_roundtrip_restores_frontier(tmp_path: Path):
    checkpoint = tmp_path / "frontier.pkl"

    frontier = MemoryFrontier(bloom_capacity=1_000, max_pending=100)
    await frontier.add_urls(["https://a.com/1", "https://b.com/1"])
    await frontier.save_checkpoint(str(checkpoint))

    restored = MemoryFrontier(bloom_capacity=1_000, max_pending=100)
    await restored.load_checkpoint(str(checkpoint))

    assert await restored.size() == 2
    assert await restored.total_seen() == 2
    assert await restored.get_next() is not None


@pytest.mark.asyncio
async def test_cooling_promotes_back_to_ready_when_time_passes(monkeypatch):
    current_time = [1000.0]

    def fake_monotonic():
        return current_time[0]

    monkeypatch.setattr(frontier_module.time, "monotonic", fake_monotonic)

    frontier = MemoryFrontier(bloom_capacity=1_000, max_pending=100)
    frontier.set_cooldown_checker(lambda domain: 2.0)
    await frontier.add_urls(["https://promo.com/1", "https://promo.com/2"])

    first = await frontier.get_next()
    assert first is not None
    assert await frontier.mark_acquired(first) is True
    assert frontier.state_stats["cooling"] == 1

    current_time[0] += 2.1
    second = await frontier.get_next()
    assert second is not None
    assert second.url != first.url
    assert frontier.state_stats["promotions"] >= 1


@pytest.mark.asyncio
async def test_new_domain_priority_is_above_mature_ypc_domains():
    frontier = MemoryFrontier(bloom_capacity=1_000, max_pending=100)

    # mature.com has high ypc but already out of warm-up.
    frontier._domain_issue_count["mature.com"] = 3
    frontier.update_domain_priorities({"mature.com": 999999.0})

    await frontier.add_url("https://mature.com/1")
    await frontier.add_url("https://new.com/1")

    first = await frontier.get_next()
    assert first is not None
    assert first.domain == "new.com"


@pytest.mark.asyncio
async def test_warmup_priority_is_above_any_mature_ypc():
    frontier = MemoryFrontier(bloom_capacity=1_000, max_pending=100)

    # warm.com has <3 issued so it should beat any mature ypc domain.
    frontier._domain_issue_count["warm.com"] = 1
    frontier._domain_issue_count["mature.com"] = 3
    frontier.update_domain_priorities({"warm.com": -1.0, "mature.com": 999999.0})

    await frontier.add_url("https://warm.com/1")
    await frontier.add_url("https://mature.com/1")

    first = await frontier.get_next()
    assert first is not None
    assert first.domain == "warm.com"


@pytest.mark.asyncio
async def test_requeue_issued_url_restores_pending_work():
    frontier = MemoryFrontier(bloom_capacity=1_000, max_pending=100)
    await frontier.add_url("https://retry.com/a")

    item = await frontier.get_next()
    assert item is not None
    assert await frontier.size() == 0

    ok = await frontier.requeue_issued_url(item)
    assert ok is True
    assert await frontier.release_issued_url(item) is True
    assert await frontier.size() == 1

    again = await frontier.get_next()
    assert again is not None
    assert again.url == item.url


@pytest.mark.asyncio
async def test_ready_scheduler_reserves_20_percent_for_non_new_domains():
    frontier = MemoryFrontier(bloom_capacity=1_000, max_pending=100)
    frontier._domain_issue_count["warm.com"] = 1
    frontier.update_domain_priorities({"warm.com": 0.1})

    await frontier.add_url("https://warm.com/1")
    await frontier.add_urls(
        [f"https://new{i}.com/1" for i in range(1, 6)],
        depth=0,
    )

    first_five = [await frontier.get_next() for _ in range(5)]
    domains = [item.domain for item in first_five if item is not None]

    assert len(domains) == 5
    assert domains[-1] == "warm.com"
    assert "warm.com" not in domains[:4]


@pytest.mark.asyncio
async def test_release_reserved_to_ready_when_not_acquired():
    frontier = MemoryFrontier(bloom_capacity=1_000, max_pending=100)
    await frontier.add_urls(["https://release.com/1", "https://release.com/2"])

    first = await frontier.get_next()
    assert first is not None
    assert frontier.state_stats["reserved"] == 1

    assert await frontier.release_issued_url(first) is True
    assert frontier.state_stats["ready"] >= 1


@pytest.mark.asyncio
async def test_mark_acquired_transitions_reserved_to_cooling():
    frontier = MemoryFrontier(bloom_capacity=1_000, max_pending=100)
    frontier.set_cooldown_checker(lambda _domain: 2.0)
    await frontier.add_urls(["https://acq.com/1", "https://acq.com/2"])

    first = await frontier.get_next()
    assert first is not None
    assert frontier.state_stats["reserved"] == 1

    assert await frontier.mark_acquired(first) is True
    assert frontier.state_stats["cooling"] == 1


@pytest.mark.asyncio
async def test_mark_acquired_rejects_mismatched_reservation():
    frontier = MemoryFrontier(bloom_capacity=1_000, max_pending=100)
    await frontier.add_url("https://mismatch.com/1")

    first = await frontier.get_next()
    assert first is not None

    forged = type(first)(
        url=first.url,
        domain=first.domain,
        depth=first.depth,
        priority=first.priority,
        issue_time=first.issue_time,
        reservation_id=first.reservation_id + 999,
    )
    assert await frontier.mark_acquired(forged) is False
    assert frontier.state_stats["reserved"] == 1
