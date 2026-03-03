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
  ├─ Frontier (per-domain queues + Bloom filter dedup)
  ├─ Fetcher (aiohttp connection pool + DNS cache)
  ├─ Politeness (robots.txt cache + per-domain rate limiter)
  ├─ Parser (selectolax link extraction + URL normalization)
  └─ Storage (SQLite WAL + batch writes + stats)
```

## Key Design Decisions

- **Per-domain round-robin**: Fair scheduling across domains
- **0.5 QPS rate limit**: Token bucket per domain
- **Bloom filter**: O(1) dedup for 50M+ URLs
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
