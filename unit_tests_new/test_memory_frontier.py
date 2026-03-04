from pathlib import Path

import pytest
import time

import crawler.frontier.memory_frontier as frontier_module
from crawler.frontier.memory_frontier import (
    MemoryFrontier,
    _NEW_DOMAIN_PRIORITY,
    _WARMUP_DOMAIN_PRIORITY,
    _MAX_YPC_PRIORITY,
    _WARMUP_ISSUE_LIMIT,
    _NON_NEW_SLOT_EVERY,
)


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
    frontier.set_cooldown_checker(lambda _: 0.0)
    await frontier.add_url("https://retry.com/a")

    item = await frontier.get_next()
    assert item is not None
    assert await frontier.size() == 0

    # requeue is atomic: re-enqueues URL AND releases reservation
    ok = await frontier.requeue_issued_url(item)
    assert ok is True
    assert await frontier.size() == 1
    # domain must now be Ready (not Reserved) after requeue
    assert frontier.state_stats.get("reserved", 0) == 0

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


# ──────────────────────────────────────────────────────────────────────────────
# Priority tier correctness (from log anomalies)
# ──────────────────────────────────────────────────────────────────────────────

def test_effective_priority_tier_boundaries():
    """_effective_priority must return exact tier values at all boundaries."""
    frontier = MemoryFrontier(bloom_capacity=1_000, max_pending=100)
    frontier._domain_yield["d"] = 500.0

    # New: issue_count == 0 → always 3M regardless of ypc
    frontier._domain_issue_count["d"] = 0
    assert frontier._effective_priority("d") == _NEW_DOMAIN_PRIORITY

    # Warmup lower bound: issue_count == 1 → flat 2M
    frontier._domain_issue_count["d"] = 1
    assert frontier._effective_priority("d") == _WARMUP_DOMAIN_PRIORITY

    # Warmup upper bound: issue_count == _WARMUP_ISSUE_LIMIT - 1 → flat 2M
    frontier._domain_issue_count["d"] = _WARMUP_ISSUE_LIMIT - 1
    assert frontier._effective_priority("d") == _WARMUP_DOMAIN_PRIORITY

    # Mature entry: issue_count == _WARMUP_ISSUE_LIMIT → ypc score (capped at 1M)
    frontier._domain_issue_count["d"] = _WARMUP_ISSUE_LIMIT
    assert frontier._effective_priority("d") == min(500.0, _MAX_YPC_PRIORITY)

    # Mature far: high issue count, ypc below cap → raw ypc returned
    frontier._domain_issue_count["d"] = 172
    frontier._domain_yield["d"] = 28.9
    assert frontier._effective_priority("d") == 28.9

    # Mature: ypc above cap → clamped to 1M
    frontier._domain_yield["d"] = 2_000_000.0
    assert frontier._effective_priority("d") == _MAX_YPC_PRIORITY


def test_effective_priority_unknown_domain_defaults_to_new():
    """A domain with no issue_count entry is treated as New (3M)."""
    frontier = MemoryFrontier(bloom_capacity=1_000, max_pending=100)
    assert frontier._effective_priority("never.seen.com") == _NEW_DOMAIN_PRIORITY


def test_mature_ypc_cap_is_below_warmup():
    """Even the maximum possible Mature priority must be below Warmup."""
    assert _MAX_YPC_PRIORITY < _WARMUP_DOMAIN_PRIORITY


def test_warmup_is_below_new():
    """Warmup priority must be below New."""
    assert _WARMUP_DOMAIN_PRIORITY < _NEW_DOMAIN_PRIORITY


@pytest.mark.asyncio
async def test_mature_domains_ordered_by_ypc():
    """Within the Mature tier, higher YPC score must be dispatched first."""
    frontier = MemoryFrontier(bloom_capacity=1_000, max_pending=100)

    for domain, issue_count in [("low.com", 10), ("high.com", 50)]:
        frontier._domain_issue_count[domain] = issue_count

    frontier.update_domain_priorities({"low.com": 5.0, "high.com": 80.0})

    await frontier.add_url("https://low.com/1")
    await frontier.add_url("https://high.com/1")

    first = await frontier.get_next()
    assert first is not None
    assert first.domain == "high.com", (
        "high.com has Mature YPC=80 vs low.com YPC=5; high.com must win"
    )


@pytest.mark.asyncio
async def test_warmup_beats_high_ypc_mature_regardless_of_score():
    """
    A Warmup domain (issue_count=1, ypc=0.001) must be dispatched
    before a Mature domain with the maximum possible YPC score.
    This is by design: exploration over exploitation in early crawl.
    """
    frontier = MemoryFrontier(bloom_capacity=1_000, max_pending=100)

    frontier._domain_issue_count["warmup.com"] = 1
    frontier._domain_issue_count["mature.com"] = 172
    frontier.update_domain_priorities({
        "warmup.com": 0.001,
        "mature.com": _MAX_YPC_PRIORITY,  # max possible mature priority
    })

    await frontier.add_url("https://warmup.com/1")
    await frontier.add_url("https://mature.com/1")

    first = await frontier.get_next()
    assert first is not None
    assert first.domain == "warmup.com"


# ──────────────────────────────────────────────────────────────────────────────
# footystats scenario: domain with internal links stays active
# ──────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_domain_with_internal_links_stays_active():
    """
    Reproduces the footystats pattern:
    - Domain has a large internal URL graph
    - Each crawl cycle adds more same-domain URLs
    - Domain never goes Empty, keeps cycling Cooling → Ready
    """
    frontier = MemoryFrontier(bloom_capacity=10_000, max_pending=1_000)
    frontier.set_cooldown_checker(lambda _domain: 0.0)  # instant cooldown

    # Seed 3 internal footystats URLs → New tier
    await frontier.add_urls([
        "https://footystats.org/page1",
        "https://footystats.org/page2",
        "https://footystats.org/page3",
    ])

    for crawl_n in range(5):
        item = await frontier.get_next()
        assert item is not None
        assert item.domain == "footystats.org"

        # Worker acquires slot
        await frontier.mark_acquired(item)

        # Simulate: each crawl discovers 3 more footystats pages
        await frontier.add_urls([
            f"https://footystats.org/internal{crawl_n}-{i}"
            for i in range(3)
        ])

    # Domain should still have pending URLs after 5 crawls
    stats = frontier.state_stats
    assert stats.get("empty", 0) == 0 or "footystats.org" not in frontier._empty, (
        "footystats.org must NOT be Empty while internal URLs exist"
    )
    assert await frontier.size() > 0


@pytest.mark.asyncio
async def test_high_crawl_count_mature_domain_ordered_by_ypc():
    """
    footystats at crawl_n=172, ypc≈29 must rank BELOW
    a domain with ypc=90 in the Mature tier.
    """
    frontier = MemoryFrontier(bloom_capacity=10_000, max_pending=1_000)

    frontier._domain_issue_count["footystats.org"] = 172
    frontier._domain_issue_count["better.com"] = 10
    frontier.update_domain_priorities({
        "footystats.org": 28.9,
        "better.com": 90.0,
    })

    await frontier.add_url("https://footystats.org/x")
    await frontier.add_url("https://better.com/x")

    first = await frontier.get_next()
    assert first is not None
    assert first.domain == "better.com", (
        "better.com (Mature ypc=90) must beat footystats (Mature ypc=28.9)"
    )


# ──────────────────────────────────────────────────────────────────────────────
# pw.live scenario: external-link-only crawl empties the domain queue
# ──────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_domain_goes_empty_when_discovered_urls_are_external():
    """
    Reproduces the pw.live pattern:
    - pw.live has exactly 1 seed URL
    - Crawling it discovers 137 URLs on OTHER domains
    - pw.live's own queue is empty → state = Empty
    - High YPC score has no effect while domain is Empty
    """
    frontier = MemoryFrontier(bloom_capacity=10_000, max_pending=1_000)
    frontier.set_cooldown_checker(lambda _domain: 0.0)

    await frontier.add_url("https://pw.live/home")

    # Issue the seed URL
    item = await frontier.get_next()
    assert item is not None
    assert item.domain == "pw.live"

    # Record 137 external discoveries — none on pw.live
    await frontier.add_urls([
        f"https://external{i}.com/page"
        for i in range(137)
    ])

    # Finalize pw.live reservation (no more pw.live URLs → goes Empty)
    await frontier.mark_acquired(item)

    # pw.live must now be Empty (its own queue is drained)
    assert "pw.live" in frontier._empty, (
        "pw.live must be Empty after crawling its only page "
        "and discovering only external links"
    )

    # Push a high YPC score for pw.live
    frontier._domain_issue_count["pw.live"] = 1
    frontier.update_domain_priorities({"pw.live": 500.0})

    # pw.live should NOT appear next — it is Empty
    # (137 external domains are New tier, one of them should be dispatched)
    next_item = await frontier.get_next()
    assert next_item is not None
    assert next_item.domain != "pw.live", (
        "pw.live must NOT be dispatched while its queue is Empty, "
        "even with a high YPC score"
    )


@pytest.mark.asyncio
async def test_empty_domain_reactivates_only_when_self_link_added():
    """
    pw.live can only come back if a page on ANOTHER domain links back to it.
    Adding an external-domain URL does NOT reactivate pw.live.
    """
    frontier = MemoryFrontier(bloom_capacity=10_000, max_pending=1_000)
    frontier.set_cooldown_checker(lambda _domain: 0.0)

    await frontier.add_url("https://pw.live/home")
    item = await frontier.get_next()
    assert item is not None
    await frontier.mark_acquired(item)

    # pw.live is Empty
    assert "pw.live" in frontier._empty

    # Adding a URL on a different domain does NOT reactivate pw.live
    await frontier.add_url("https://other.com/page")
    assert "pw.live" in frontier._empty

    # Adding a pw.live URL DOES reactivate it
    await frontier.add_url("https://pw.live/new-page")
    assert "pw.live" not in frontier._empty
    stats = frontier.state_stats
    assert stats.get("ready", 0) >= 1 or stats.get("cooling", 0) >= 1


# ──────────────────────────────────────────────────────────────────────────────
# Non-new slot enforcement
# ──────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_non_new_slot_targets_issue_count_zero_only():
    """
    The 1-in-N enforcement skips domains with issue_count==0 (New tier).
    It must NOT skip Warmup domains (issue_count 1..2).
    This tests that warmup domains are NOT penalised by the enforcement.
    """
    frontier = MemoryFrontier(bloom_capacity=1_000, max_pending=100)

    # One warmup domain
    frontier._domain_issue_count["warmup.com"] = 1

    # Fill with New domains to trigger the enforcement
    new_domains = [f"new{i}.com" for i in range(_NON_NEW_SLOT_EVERY * 2)]
    for d in new_domains:
        await frontier.add_url(f"https://{d}/1")
    await frontier.add_url("https://warmup.com/1")

    dispatched = []
    for _ in range(_NON_NEW_SLOT_EVERY * 2 + 1):
        item = await frontier.get_next()
        if item is None:
            break
        dispatched.append(item.domain)
        # Immediately release so domain re-enters the pool
        await frontier.release_issued_url(item)

    # warmup.com must appear within the first _NON_NEW_SLOT_EVERY dispatches
    first_batch = set(dispatched[:_NON_NEW_SLOT_EVERY])
    assert "warmup.com" in first_batch, (
        f"warmup.com must appear in first {_NON_NEW_SLOT_EVERY} dispatches; "
        f"got: {dispatched}"
    )


@pytest.mark.asyncio
async def test_non_new_slot_does_not_stall_when_only_new_domains_exist():
    """
    If ALL ready domains are New (issue_count==0), the 1-in-N slot enforcement
    must fall back and still dispatch one — not return None.
    """
    frontier = MemoryFrontier(bloom_capacity=1_000, max_pending=100)

    for i in range(10):
        await frontier.add_url(f"https://new{i}.com/1")
    # All are New (issue_count==0)

    # Even on the forced-non-new dispatch slot, something must be returned
    # (no non-new domains available, so it falls back to new)
    for _ in range(10):
        item = await frontier.get_next()
        assert item is not None, "Must not return None when New domains are available"
        await frontier.release_issued_url(item)


# ──────────────────────────────────────────────────────────────────────────────
# Issue→Acquire gap tracking
# ──────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_issue_time_is_set_at_dispatch():
    """
    get_next() must stamp item.issue_time with the current monotonic clock.
    issue_time==0 means 'never dispatched'; any dispatched item must have
    issue_time > 0.
    """
    frontier = MemoryFrontier(bloom_capacity=1_000, max_pending=100)
    await frontier.add_url("https://timed.com/1")

    before = time.monotonic()
    item = await frontier.get_next()
    after = time.monotonic()

    assert item is not None
    assert item.issue_time > 0, "issue_time must be set by get_next()"
    assert before <= item.issue_time <= after, (
        "issue_time must be within the get_next() call window"
    )


@pytest.mark.asyncio
async def test_requeued_url_gets_fresh_issue_time(monkeypatch):
    """
    A requeued URL is re-dispatched with a fresh issue_time.
    The old issue_time from the original dispatch must be replaced.
    """
    current_time = [1000.0]
    monkeypatch.setattr(frontier_module.time, "monotonic", lambda: current_time[0])

    frontier = MemoryFrontier(bloom_capacity=1_000, max_pending=100)
    frontier.set_cooldown_checker(lambda _: 0.0)
    await frontier.add_url("https://reissue.com/1")

    # First dispatch at t=1000
    first = await frontier.get_next()
    assert first is not None
    assert first.issue_time == 1000.0

    # Requeue the URL
    await frontier.requeue_issued_url(first)

    # Advance time
    current_time[0] = 2000.0

    # Re-dispatch must use new timestamp
    second = await frontier.get_next()
    assert second is not None
    assert second.url == first.url
    assert second.issue_time == 2000.0, (
        "Requeued URL must receive a fresh issue_time on re-dispatch"
    )
