"""Benchmark: test concurrency × pool_size matrix.

Runs short crawls with varying configurations and produces
a comparison report to find the optimal settings and identify
hardware bottlenecks.

Usage:
    python -m crawler.benchmark --config config.yaml --seeds seeds.txt
    python -m crawler.benchmark --seeds seeds.txt --duration 90
    python -m crawler.benchmark --seeds seeds.txt --concurrency 10,30,50,100,200 --pools 20,60,120,220
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import json
import logging
import os
import sys
import time
from pathlib import Path

from crawler.orchestrator import CrawlOrchestrator
from crawler.utils.config import load_config


def setup_logging():
    fmt = "%(asctime)s │ %(levelname)-5s │ %(name)-20s │ %(message)s"
    logging.basicConfig(
        level=logging.WARNING,  # Quiet for benchmark
        format=fmt,
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )
    logging.getLogger("aiohttp").setLevel(logging.ERROR)
    logging.getLogger("aiosqlite").setLevel(logging.ERROR)
    logging.getLogger("crawler").setLevel(logging.WARNING)


def load_seeds(path: str) -> list[str]:
    p = Path(path)
    if not p.exists():
        print(f"❌ Seeds file not found: {path}")
        sys.exit(1)
    urls = []
    for line in p.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            urls.append(line)
    return urls


async def run_single(
    config, seeds: list[str], concurrency: int, pool_size: int,
    duration: int, run_id: int
) -> dict:
    """Run a single benchmark with given settings."""
    db_path = f"/tmp/bench_{run_id}.db"
    ckpt_path = f"/tmp/bench_{run_id}.pkl"
    profile_path = f"/tmp/bench_{run_id}_profile.jsonl"

    # Clean
    for f in [db_path, ckpt_path, profile_path]:
        try:
            os.unlink(f)
        except FileNotFoundError:
            pass

    # Override config
    cfg = dataclasses.replace(
        config,
        crawler=dataclasses.replace(
            config.crawler,
            concurrency=concurrency,
            max_time_seconds=duration,
        ),
        fetcher=dataclasses.replace(
            config.fetcher,
            connection_pool_size=pool_size,
        ),
        storage=dataclasses.replace(config.storage, db_path=db_path),
        frontier=dataclasses.replace(config.frontier, checkpoint_path=ckpt_path),
        logging=dataclasses.replace(config.logging, stats_interval=999999),
    )

    orchestrator = CrawlOrchestrator(cfg)

    t0 = time.monotonic()
    try:
        await orchestrator.run(seeds, resume=False)
    except Exception as e:
        print(f"  ⚠ Run failed: {e}")
    elapsed = time.monotonic() - t0

    # Collect summary from profiler
    s = orchestrator._profiler.summary if orchestrator._profiler else {}

    # Clean up files
    for f in [db_path, ckpt_path, profile_path]:
        try:
            os.unlink(f)
        except FileNotFoundError:
            pass

    lock = s.get("frontier_lock", {})

    return {
        "concurrency": concurrency,
        "pool_size": pool_size,
        "elapsed": round(elapsed, 1),
        # Throughput
        "crawled": s.get("crawled", 0),
        "success": s.get("success", 0),
        "qps": round(s.get("qps", 0), 1),
        "eff_qps": round(s.get("effective_qps", 0), 1),
        "success_pct": round(s.get("success_rate", 0) * 100, 1),
        # Errors
        "errors": s.get("errors", 0),
        "timeouts": s.get("timeouts", 0),
        "conn_errs": s.get("conn_errors", 0),
        "conn_err_pct": round(s.get("conn_error_pct", 0), 1),
        "timeout_pct": round(s.get("timeout_pct", 0), 1),
        # Latency
        "p50": round(s.get("p50_ms", 0)),
        "p95": round(s.get("p95_ms", 0)),
        "p99": round(s.get("p99_ms", 0)),
        # Lock contention
        "lock_wait_ms": round(lock.get("avg_lock_wait_ms", 0), 2),
        "lock_hold_ms": round(lock.get("avg_lock_hold_ms", 0), 2),
        "cd_skips": s.get("cd_skips", 0),
        # Worker state
        "active_pct": round(s.get("active_pct", 0), 1),
        "robots_pct": round(s.get("robots_pct", 0), 1),
        "rate_wait_pct": round(s.get("rate_wait_pct", 0), 1),
        "avg_rate_wait": round(s.get("avg_rate_wait_s", 0), 3),
    }


def print_report(results: list[dict]):
    """Print diagnostic benchmark report."""
    print()
    print("=" * 120)
    print("  🏁 BENCHMARK RESULTS — 找出硬體極限")
    print("=" * 120)

    # Table 1: Throughput
    print()
    print("  ▸ THROUGHPUT (效率)")
    print(f"  {'Workers':>7}  {'Pool':>5}  {'Crawled':>8}  {'Success':>8}  "
          f"{'Succ%':>6}  {'QPS':>6}  {'Eff.QPS':>7}  {'T/O':>5}  "
          f"{'ConnErr':>7}  {'CE%':>5}  {'TO%':>5}")
    print("  " + "-" * 105)

    best_eff = max(r["eff_qps"] for r in results)
    for r in results:
        star = " ⭐" if r["eff_qps"] == best_eff else "   "
        print(
            f"  {r['concurrency']:>7}  {r['pool_size']:>5}  "
            f"{r['crawled']:>8,}  {r['success']:>8,}  "
            f"{r['success_pct']:>5.1f}%  {r['qps']:>6.1f}  "
            f"{r['eff_qps']:>7.1f}  {r['timeouts']:>5}  "
            f"{r['conn_errs']:>7}  {r['conn_err_pct']:>4.1f}%  "
            f"{r['timeout_pct']:>4.1f}%{star}"
        )

    # Table 2: Latency + Lock
    print()
    print("  ▸ BOTTLENECK DIAGNOSIS (瓶頸診斷)")
    print(f"  {'Workers':>7}  {'Pool':>5}  {'p50':>6}  {'p95':>6}  {'p99':>6}  "
          f"{'Lock⏳':>7}  {'Lock🔒':>7}  {'CDskip':>8}  "
          f"{'Active%':>7}  {'Robot%':>7}  {'Rate%':>7}  {'AvgWait':>7}")
    print("  " + "-" * 105)

    for r in results:
        # Color indicators for lock
        lock_warn = "⚠" if r["lock_wait_ms"] > 5.0 else " "
        print(
            f"  {r['concurrency']:>7}  {r['pool_size']:>5}  "
            f"{r['p50']:>5}ms {r['p95']:>5}ms {r['p99']:>5}ms  "
            f"{r['lock_wait_ms']:>6.2f}ms{lock_warn}"
            f"{r['lock_hold_ms']:>6.2f}ms  "
            f"{r['cd_skips']:>8,}  "
            f"{r['active_pct']:>6.1f}%  {r['robots_pct']:>6.1f}%  "
            f"{r['rate_wait_pct']:>6.1f}%  {r['avg_rate_wait']:>6.3f}s"
        )

    # Best config
    print()
    print("=" * 120)
    best = max(results, key=lambda r: r["eff_qps"])
    print(f"  ⭐ Best: concurrency={best['concurrency']}, pool={best['pool_size']}")
    print(f"     → {best['eff_qps']} effective QPS, {best['success']:,} successful in {best['elapsed']:.0f}s")

    # Diagnosis
    print()
    print("  🔍 DIAGNOSIS:")
    for r in results:
        tags = []
        if r["conn_err_pct"] > 5:
            tags.append(f"🔴 conn_err={r['conn_err_pct']:.0f}% (network/OS saturated)")
        if r["timeout_pct"] > 10:
            tags.append(f"🟡 timeout={r['timeout_pct']:.0f}% (many dead connections)")
        if r["lock_wait_ms"] > 5:
            tags.append(f"🟠 lock_wait={r['lock_wait_ms']:.1f}ms (frontier contention)")
        if r["rate_wait_pct"] > 10:
            tags.append(f"🟡 rate_wait={r['rate_wait_pct']:.0f}% (too few domains)")
        if r["p50"] > 5000:
            tags.append(f"🔴 p50={r['p50']}ms (overall network saturated)")
        if not tags:
            tags.append("✅ healthy")
        label = f"  c={r['concurrency']:>3} p={r['pool_size']:>3}: "
        print(label + "; ".join(tags))

    print("=" * 120)
    print()


def main():
    parser = argparse.ArgumentParser(description="Crawler Benchmark Matrix")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--seeds", default="seeds.txt")
    parser.add_argument("--duration", type=int, default=120,
                        help="Duration per test in seconds (default: 120)")
    parser.add_argument("--concurrency", type=str, default="10,30,50,100,200",
                        help="Comma-separated concurrency values")
    parser.add_argument("--pools", type=str, default=None,
                        help="Comma-separated pool sizes (default: auto = max(20, c*1.1))")
    args = parser.parse_args()

    setup_logging()
    config = load_config(args.config)
    seeds = load_seeds(args.seeds)

    concurrencies = [int(x) for x in args.concurrency.split(",")]

    # Build test matrix
    tests = []
    for c in concurrencies:
        if args.pools:
            pools = [int(x) for x in args.pools.split(",")]
        else:
            pools = [max(20, int(c * 1.1 + 10))]
        for p in pools:
            tests.append((c, p))

    n = len(tests)
    total_min = n * args.duration / 60
    print(f"\n🏁 Benchmark: {n} tests × {args.duration}s = {total_min:.0f} min total")
    print(f"   Seeds: {len(seeds)}, Timeout: {config.fetcher.timeout}s")
    print(f"   Matrix: {tests}\n")

    results = []
    for i, (c, p) in enumerate(tests):
        print(f"━━━ [{i+1}/{n}] concurrency={c}, pool={p}, {args.duration}s ━━━")
        result = asyncio.run(run_single(config, seeds, c, p, args.duration, i))
        results.append(result)
        print(f"    → Eff.QPS={result['eff_qps']}, "
              f"success={result['success']:,} ({result['success_pct']}%), "
              f"conn_err={result['conn_err_pct']}%, "
              f"lock_wait={result['lock_wait_ms']:.2f}ms")

    print_report(results)

    # Save JSON
    report_path = Path("benchmark_results.json")
    report_path.write_text(json.dumps(results, indent=2))
    print(f"📄 Full results: {report_path}")


if __name__ == "__main__":
    main()
