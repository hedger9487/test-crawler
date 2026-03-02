"""CLI entry point for the web crawler."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from crawler.orchestrator import CrawlOrchestrator
from crawler.utils.config import load_config


def setup_logging(level: str = "INFO") -> None:
    """Configure structured logging."""
    fmt = "%(asctime)s │ %(levelname)-5s │ %(name)-20s │ %(message)s"
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=fmt,
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )
    # Quiet down noisy libraries
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("aiosqlite").setLevel(logging.WARNING)


def load_seeds(path: str) -> list[str]:
    """Load seed URLs from file (one per line)."""
    p = Path(path)
    if not p.exists():
        logging.error("Seeds file not found: %s", path)
        sys.exit(1)
    urls = []
    for line in p.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            urls.append(line)
    logging.info("Loaded %d seed URLs from %s", len(urls), path)
    return urls


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="HW1 Web Crawler — Architecture Design Course",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m crawler.main --config config.yaml --seeds seeds.txt
  python -m crawler.main --resume
  python -m crawler.main --seeds seeds.txt --concurrency 300 --max-time 3600
        """,
    )
    parser.add_argument(
        "--config", default="config.yaml",
        help="Path to YAML config file (default: config.yaml)",
    )
    parser.add_argument(
        "--seeds", default=None,
        help="Path to seed URLs file (overrides config)",
    )
    parser.add_argument(
        "--resume", action="store_true",
        help="Resume from last checkpoint",
    )
    parser.add_argument(
        "--concurrency", type=int, default=None,
        help="Override concurrency level",
    )
    parser.add_argument(
        "--max-time", type=int, default=None,
        help="Override max crawl time (seconds)",
    )
    return parser.parse_args()


def cli_main() -> None:
    args = parse_args()

    # Load config
    config = load_config(args.config)

    # Apply CLI overrides
    if args.concurrency:
        # We need to create a new config with the override
        from crawler.utils.config import CrawlerConfig
        import dataclasses
        new_crawler = dataclasses.replace(config.crawler, concurrency=args.concurrency)
        config = dataclasses.replace(config, crawler=new_crawler)

    if args.max_time:
        from crawler.utils.config import CrawlerConfig
        import dataclasses
        new_crawler = dataclasses.replace(config.crawler, max_time_seconds=args.max_time)
        config = dataclasses.replace(config, crawler=new_crawler)

    # Setup logging
    setup_logging(config.logging.level)

    # Load seeds
    seeds_path = args.seeds or config.crawler.seeds_file
    seed_urls = load_seeds(seeds_path)

    # Run crawler
    orchestrator = CrawlOrchestrator(config)

    try:
        asyncio.run(orchestrator.run(seed_urls, resume=args.resume))
    except KeyboardInterrupt:
        logging.info("Interrupted.")


if __name__ == "__main__":
    cli_main()
