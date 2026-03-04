# HW1 Web Crawler

Scalable async web crawler for Architecture Design course.

## Quick Start

```bash
# Install dependencies
pip install -e ".[dev]"

# Run with example seeds
python -m crawler.main --config config.yaml --seeds seeds.txt

# Resume from checkpoint
python -m crawler.main --resume

# Run with custom settings
python -m crawler.main --seeds seeds.txt --concurrency 300 --max-time 3600
```

## Architecture

```
Orchestrator → N async workers
  ├─ Frontier (four-state scheduler + per-domain queues + Bloom filter dedup)
  ├─ Fetcher (aiohttp connection pool + DNS cache)
  ├─ Politeness (robots.txt cache + per-domain rate limiter)
  ├─ Parser (selectolax link extraction + URL normalization)
  └─ Storage (SQLite WAL + batch writes + stats)
```

## Key Design Decisions

- **Four-state domain scheduler**: Ready / Reserved / Cooling / Empty with O(log N) `get_next()`
- **Tiered priority**: New domains (3M) → Warmup ≤3 fetches (2M) → Mature YPC-scored (1M cap). Every 5th pick reserves a non-new domain slot to prevent starvation.
- **Organic BFS only**: No sitemap pre-seeding — bulk injection breaks the tier invariants and wastes frontier cap. All discovery is driven by link extraction from crawled pages.
- **0.5 QPS rate limit**: Token bucket per domain; `Crawl-delay` from robots.txt overrides if stricter
- **Bloom filter**: O(1) dedup, 100M capacity ≈ 160 MB
- **Checkpoint/Resume**: Frontier state saved every 5 min
- **Abstract interfaces**: Swap SQLite → Redis without touching orchestrator

## Config

See `config.yaml` for all tunable parameters.

## Graceful Shutdown

Press `Ctrl+C` → saves checkpoint → resumes later with `--resume`.


保存log跑法 mac:
script -q main_crawl.log python -m crawler.main --config config.yaml --seeds seeds.txt --concurrency 50

保存log跑法 linux:
script -q main_crawl.log -c "python -m crawler.main --config config.yaml --seeds seeds.txt --concurrency 50"
