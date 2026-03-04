"""Unit tests for CrawlProfiler — scoring, recording, and domain ranking."""

import pytest

from crawler.storage.profiler import CrawlProfiler


def _make_profiler() -> CrawlProfiler:
    """Return a fresh profiler suitable for unit testing (reporter not started)."""
    return CrawlProfiler(num_workers=4, report_interval=9999.0)


# ──────────────────────────────────────────────────────────────────────────────
# get_domain_score
# ──────────────────────────────────────────────────────────────────────────────

def test_score_for_unknown_domain_is_neutral():
    """Unexplored domains return the neutral score 1.0 (Explore tier entry)."""
    p = _make_profiler()
    assert p.get_domain_score("never-seen.com") == 1.0


def test_score_is_zero_when_domain_has_attempts_but_no_yield():
    """A domain that was crawled but never produced new URLs scores 0."""
    p = _make_profiler()
    p.record_fetch("barren.com", status=200, latency_ms=500.0, is_html=True)
    # no record_discovered call → crawl_yield stays 0
    assert p.get_domain_score("barren.com") == 0.0


def test_score_equals_yield_divided_by_total_time():
    """score = total_discovered_urls / total_fetch_time_seconds."""
    p = _make_profiler()
    # 3 attempts × 1 000 ms each → total_time_s = 3.0
    for _ in range(3):
        p.record_fetch("a.com", status=200, latency_ms=1_000.0, is_html=True)
    # 6 new URLs discovered total
    p.record_discovered("a.com", new_count=6, total_links=10)

    expected = 6 / 3.0  # = 2.0
    assert p.get_domain_score("a.com") == pytest.approx(expected)


def test_faster_domain_scores_higher_than_slower_equal_yield_domain():
    """
    Two domains with identical yield but different latency:
    the faster one must score higher.
    """
    p = _make_profiler()

    # fast.com: 1 crawl in 100 ms, discovers 10 URLs → score = 10 / 0.1 = 100
    p.record_fetch("fast.com", status=200, latency_ms=100.0, is_html=True)
    p.record_discovered("fast.com", new_count=10, total_links=15)

    # slow.com: 1 crawl in 2 000 ms, same 10 URLs → score = 10 / 2.0 = 5
    p.record_fetch("slow.com", status=200, latency_ms=2_000.0, is_html=True)
    p.record_discovered("slow.com", new_count=10, total_links=15)

    assert p.get_domain_score("fast.com") > p.get_domain_score("slow.com")


def test_timeout_burns_time_without_yield_and_lowers_score():
    """
    A domain that first had good yield, then starts timing out:
    subsequent timeouts accumulate time without yield → score falls.
    """
    p = _make_profiler()

    # Good crawl: 200 ms, 5 URLs
    p.record_fetch("spotty.com", status=200, latency_ms=200.0, is_html=True)
    p.record_discovered("spotty.com", new_count=5, total_links=8)
    score_before = p.get_domain_score("spotty.com")

    # Timeout: 10 000 ms, no yield
    p.record_fetch("spotty.com", status=0, latency_ms=10_000.0,
                   is_html=False, error="timeout")
    score_after = p.get_domain_score("spotty.com")

    assert score_after < score_before, (
        "Timeout must reduce score by increasing denominator without adding yield"
    )


def test_failed_requests_penalise_score_via_denominator():
    """
    Two domains: identical yield, but domain B has extra failed requests.
    B should score lower because total_time_s is higher for the same yield.
    """
    p = _make_profiler()

    # clean.com: 1 successful crawl, 5 URLs, 500 ms
    p.record_fetch("clean.com", status=200, latency_ms=500.0, is_html=True)
    p.record_discovered("clean.com", new_count=5, total_links=8)

    # spotty.com: same 1 successful crawl + 1 failure (additional 500 ms)
    p.record_fetch("spotty.com", status=200, latency_ms=500.0, is_html=True)
    p.record_discovered("spotty.com", new_count=5, total_links=8)
    p.record_fetch("spotty.com", status=503, latency_ms=500.0, is_html=False)

    assert p.get_domain_score("clean.com") > p.get_domain_score("spotty.com")


# ──────────────────────────────────────────────────────────────────────────────
# record_fetch / record_discovered counters
# ──────────────────────────────────────────────────────────────────────────────

def test_record_fetch_increments_status_200_as_success():
    p = _make_profiler()
    p.record_fetch("ok.com", status=200, latency_ms=100.0, is_html=True)
    assert p.summary["success"] == 1
    assert p.summary["crawled"] == 1
    assert p.summary["errors"] == 0


def test_record_fetch_increments_error_for_status_0():
    p = _make_profiler()
    p.record_fetch("fail.com", status=0, latency_ms=50.0,
                   is_html=False, error="connection refused")
    assert p.summary["errors"] == 1
    assert p.summary["success"] == 0


def test_record_fetch_increments_timeout_counter():
    p = _make_profiler()
    p.record_fetch("slow.com", status=0, latency_ms=10_000.0,
                   is_html=False, error="timeout")
    assert p.summary["timeouts"] == 1
    assert p.summary["errors"] == 0  # timeouts are counted separately


def test_record_discovered_accumulates_across_multiple_calls():
    p = _make_profiler()
    p.record_fetch("multi.com", status=200, latency_ms=200.0, is_html=True)
    p.record_discovered("multi.com", new_count=3, total_links=5)
    p.record_discovered("multi.com", new_count=7, total_links=10)

    # total discovered = 10
    assert p._domain_crawl_yield["multi.com"] == 10
    assert p.summary["discovered"] == 10


def test_record_fetch_accumulates_time_across_attempts():
    p = _make_profiler()
    p.record_fetch("timed.com", status=200, latency_ms=400.0, is_html=True)
    p.record_fetch("timed.com", status=200, latency_ms=600.0, is_html=True)

    # total = 1.0 s
    assert p._domain_total_time_s["timed.com"] == pytest.approx(1.0)


# ──────────────────────────────────────────────────────────────────────────────
# get_top_domains / get_most_crawled_domains
# ──────────────────────────────────────────────────────────────────────────────

def test_get_top_domains_sorted_by_urls_per_sec_descending():
    p = _make_profiler()

    # a.com: 5 URLs in 1 s  → ups = 5.0
    p.record_fetch("a.com", status=200, latency_ms=1_000.0, is_html=True)
    p.record_discovered("a.com", new_count=5, total_links=8)

    # b.com: 20 URLs in 1 s → ups = 20.0
    p.record_fetch("b.com", status=200, latency_ms=1_000.0, is_html=True)
    p.record_discovered("b.com", new_count=20, total_links=25)

    # c.com: 1 URL in 1 s  → ups = 1.0
    p.record_fetch("c.com", status=200, latency_ms=1_000.0, is_html=True)
    p.record_discovered("c.com", new_count=1, total_links=3)

    results = p.get_top_domains(10)
    domains = [r["domain"] for r in results]
    assert domains.index("b.com") < domains.index("a.com") < domains.index("c.com")


def test_get_top_domains_row_contains_expected_keys():
    p = _make_profiler()
    p.record_fetch("x.com", status=200, latency_ms=500.0, is_html=True)
    p.record_discovered("x.com", new_count=2, total_links=3)

    row = p.get_top_domains(1)[0]
    assert set(row.keys()) == {"domain", "crawls", "yield", "errors", "urls_per_sec"}


def test_get_top_domains_n_limits_results():
    p = _make_profiler()
    for i in range(10):
        p.record_fetch(f"d{i}.com", status=200, latency_ms=100.0, is_html=True)
        p.record_discovered(f"d{i}.com", new_count=i + 1, total_links=20)

    assert len(p.get_top_domains(3)) == 3


def test_get_most_crawled_domains_sorted_by_crawl_count_descending():
    p = _make_profiler()

    for _ in range(5):
        p.record_fetch("heavy.com", status=200, latency_ms=200.0, is_html=True)
    for _ in range(2):
        p.record_fetch("light.com", status=200, latency_ms=200.0, is_html=True)

    results = p.get_most_crawled_domains(10)
    domains = [r["domain"] for r in results]
    assert domains[0] == "heavy.com"
    assert domains[1] == "light.com"


def test_get_most_crawled_domains_row_contains_expected_keys():
    p = _make_profiler()
    p.record_fetch("k.com", status=200, latency_ms=300.0, is_html=True)

    row = p.get_most_crawled_domains(1)[0]
    assert set(row.keys()) == {"domain", "crawls", "success", "success_rate", "urls_per_sec"}


def test_get_most_crawled_domains_success_rate_calculation():
    p = _make_profiler()
    p.record_fetch("sr.com", status=200, latency_ms=100.0, is_html=True)
    p.record_fetch("sr.com", status=200, latency_ms=100.0, is_html=True)
    p.record_fetch("sr.com", status=500, latency_ms=100.0, is_html=False)

    row = p.get_most_crawled_domains(1)[0]
    assert row["crawls"] == 3
    assert row["success"] == 2
    assert row["success_rate"] == pytest.approx(200 / 3.0)
