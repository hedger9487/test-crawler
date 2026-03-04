"""Benchmark: test concurrency × pool_size matrix.

Runs short crawls with varying configurations and produces
a comparison report to find the optimal settings and identify
hardware bottlenecks.

Usage:
    python -m crawler.benchmark --config config.yaml --seeds seeds.txt
    python -m crawler.benchmark --seeds seeds.txt --duration 90
    python -m crawler.benchmark --seeds seeds.txt --concurrency 10,30,50,100,200 --pools 20,60,120,220
    python -m crawler.benchmark --auto --duration 60 --auto-start 40 --auto-max 1200
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

import yaml

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

    async def _progress_ticker():
        """Print progress every 10s so the terminal doesn't look stuck."""
        t0_inner = time.monotonic()
        try:
            while True:
                await asyncio.sleep(10)
                elapsed_inner = time.monotonic() - t0_inner
                crawled = orchestrator._profiler._urls_crawled if orchestrator._profiler else 0
                print(f"    ... {elapsed_inner:.0f}s elapsed, {crawled:,} crawled", flush=True)
        except asyncio.CancelledError:
            pass

    t0 = time.monotonic()
    try:
        ticker = asyncio.ensure_future(_progress_ticker())
        await orchestrator.run(seeds, resume=False)
        ticker.cancel()
        try:
            await ticker
        except asyncio.CancelledError:
            pass
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


def _row_score(r: dict) -> float:
    """Throughput score penalized by connection/timeout instability."""
    health = 1.0 - 0.7 * (r["conn_err_pct"] / 100.0) - 0.3 * (r["timeout_pct"] / 100.0)
    health = max(0.0, health)
    return r["eff_qps"] * health


def _auto_pool_size(concurrency: int, ratio: float, bias: int) -> int:
    return max(20, int(concurrency * ratio + bias))


def pick_best_result(results: list[dict]) -> tuple[dict, bool]:
    """Pick best benchmark row. Returns (best_row, is_representative)."""
    if not results:
        raise ValueError("results cannot be empty")

    valid = [r for r in results if r["crawled"] > 0]
    if not valid:
        # Keep deterministic fallback while flagging this run as non-representative
        best = max(
            results,
            key=lambda r: (r["eff_qps"], -r["conn_err_pct"], -r["timeout_pct"]),
        )
        return best, False

    best = max(
        valid,
        key=lambda r: (_row_score(r), r["eff_qps"], -r["lock_wait_ms"]),
    )
    return best, True


def write_recommended_config(
    base_config_path: str,
    output_path: str,
    best: dict,
) -> None:
    """Write a config file with recommended worker/pool values."""
    p = Path(base_config_path)
    raw = {}
    if p.exists():
        raw = yaml.safe_load(p.read_text()) or {}

    raw.setdefault("crawler", {})
    raw.setdefault("fetcher", {})

    raw["crawler"]["concurrency"] = int(best["concurrency"])
    raw["fetcher"]["connection_pool_size"] = int(best["pool_size"])

    out = Path(output_path)
    out.write_text(yaml.safe_dump(raw, sort_keys=False, allow_unicode=True))
    print(f"🧩 Recommended config written to: {out}")


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
    parser.add_argument(
        "--auto",
        action="store_true",
        help="Auto-increase concurrency until performance plateaus/degrades",
    )
    parser.add_argument("--auto-start", type=int, default=30,
                        help="Auto mode start concurrency")
    parser.add_argument("--auto-max", type=int, default=1200,
                        help="Auto mode max concurrency cap")
    parser.add_argument("--auto-factor", type=float, default=1.6,
                        help="Auto mode growth factor per step")
    parser.add_argument("--auto-max-runs", type=int, default=10,
                        help="Max benchmark runs in auto mode")
    parser.add_argument("--auto-min-improve", type=float, default=0.05,
                        help="Minimum relative score improvement to count as meaningful")
    parser.add_argument("--auto-drop-stop", type=float, default=0.10,
                        help="Stop if score drops below best by this ratio")
    parser.add_argument("--auto-max-conn-err", type=float, default=8.0,
                        help="Stop if connection error rate exceeds this percent")
    parser.add_argument("--auto-max-timeout", type=float, default=25.0,
                        help="Stop if timeout rate exceeds this percent")
    parser.add_argument("--auto-pool-ratio", type=float, default=1.1,
                        help="Pool size formula in auto mode: c * ratio + bias")
    parser.add_argument("--auto-pool-bias", type=int, default=10,
                        help="Pool size formula in auto mode: c * ratio + bias")
    parser.add_argument(
        "--write-config",
        type=str,
        default=None,
        help="Write a tuned config YAML to this path",
    )
    args = parser.parse_args()

    setup_logging()
    config = load_config(args.config)
    seeds = load_seeds(args.seeds)

    results = []
    if args.auto:
        start = max(1, args.auto_start)
        current = start
        tested: set[int] = set()
        best_score = -1.0
        plateau_count = 0

        print(
            f"\n🏁 Auto Benchmark: duration={args.duration}s, start={start}, "
            f"max={args.auto_max}, factor={args.auto_factor}"
        )
        print(f"   Seeds: {len(seeds)}, Timeout: {config.fetcher.timeout}s\n")

        run_idx = 0
        while run_idx < args.auto_max_runs and current <= args.auto_max:
            if current in tested:
                break
            tested.add(current)

            pool = _auto_pool_size(current, args.auto_pool_ratio, args.auto_pool_bias)
            print(
                f"━━━ [{run_idx+1}] auto concurrency={current}, pool={pool}, "
                f"{args.duration}s ━━━"
            )
            result = asyncio.run(
                run_single(config, seeds, current, pool, args.duration, run_idx)
            )
            results.append(result)
            print(
                f"    → Eff.QPS={result['eff_qps']}, "
                f"success={result['success']:,} ({result['success_pct']}%), "
                f"conn_err={result['conn_err_pct']}%, timeout={result['timeout_pct']}%"
            )

            run_idx += 1

            if result["crawled"] <= 0:
                print(
                    "    ⚠ No crawled URLs observed; stopping auto-search "
                    "(environment likely blocked)."
                )
                break

            score = _row_score(result)
            if score > best_score:
                if best_score > 0 and score < best_score * (1 + args.auto_min_improve):
                    plateau_count += 1
                else:
                    plateau_count = 0
                best_score = score
            else:
                plateau_count += 1

            if result["conn_err_pct"] > args.auto_max_conn_err:
                print(
                    f"    ⚠ Stop: conn_err={result['conn_err_pct']}% "
                    f"> {args.auto_max_conn_err}%"
                )
                break

            if result["timeout_pct"] > args.auto_max_timeout:
                print(
                    f"    ⚠ Stop: timeout={result['timeout_pct']}% "
                    f"> {args.auto_max_timeout}%"
                )
                break

            if best_score > 0 and score < best_score * (1 - args.auto_drop_stop):
                print(
                    f"    ⚠ Stop: score dropped > {args.auto_drop_stop*100:.0f}% "
                    "from best"
                )
                break

            if plateau_count >= 2:
                print("    ⚠ Stop: score plateau detected (2 consecutive weak steps)")
                break

            next_c = min(
                args.auto_max,
                max(current + 1, int(current * args.auto_factor)),
            )
            if next_c <= current:
                break
            current = next_c
    else:
        concurrencies = [int(x) for x in args.concurrency.split(",")]

        # Build test matrix
        tests = []
        for c in concurrencies:
            if args.pools:
                pools = [int(x) for x in args.pools.split(",")]
            else:
                pools = [_auto_pool_size(c, 1.1, 10)]
            for p in pools:
                tests.append((c, p))

        n = len(tests)
        total_min = n * args.duration / 60
        print(f"\n🏁 Benchmark: {n} tests × {args.duration}s = {total_min:.0f} min total")
        print(f"   Seeds: {len(seeds)}, Timeout: {config.fetcher.timeout}s")
        print(f"   Matrix: {tests}\n")

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

    best, representative = pick_best_result(results)
    print(
        f"🛠 Recommended: concurrency={best['concurrency']}, "
        f"connection_pool_size={best['pool_size']}"
    )
    if not representative:
        print(
            "⚠ Benchmark is not representative (no successful fetches / crawls). "
            "Likely outbound network is blocked in this environment."
        )

    if args.write_config:
        write_recommended_config(args.config, args.write_config, best)


if __name__ == "__main__":
    main()
