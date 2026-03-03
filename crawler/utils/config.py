"""Configuration loader — reads YAML and provides typed access."""

from __future__ import annotations

import yaml
from dataclasses import dataclass, field
from pathlib import Path
from typing import List


@dataclass(frozen=True)
class CrawlerConfig:
    concurrency: int = 200
    max_time_seconds: int = 172_800
    user_agent: str = "HW1-Crawler/0.1"
    seeds_file: str = "seeds.txt"


@dataclass(frozen=True)
class FetcherConfig:
    timeout: int = 30
    max_redirects: int = 5
    connection_pool_size: int = 100
    per_host_connections: int = 2
    accept_content_types: List[str] = field(
        default_factory=lambda: ["text/html", "application/xhtml+xml"]
    )


@dataclass(frozen=True)
class PolitenessConfig:
    max_qps: float = 0.5
    robots_cache_ttl: int = 86_400
    robots_fetch_timeout: int = 10


@dataclass(frozen=True)
class FrontierConfig:
    bloom_capacity: int = 100_000_000
    bloom_error_rate: float = 0.001
    max_depth: int = -1
    checkpoint_interval: int = 300
    checkpoint_path: str = "checkpoint.pkl"
    max_pending: int = 5_000_000  # frontier cap


@dataclass(frozen=True)
class StorageConfig:
    db_path: str = "crawl_data.db"
    batch_size: int = 100


@dataclass(frozen=True)
class LoggingConfig:
    level: str = "INFO"
    stats_interval: int = 30


@dataclass(frozen=True)
class NotificationConfig:
    """Crash notification settings (Discord webhook)."""
    enabled: bool = False
    discord_webhook: str = ""


@dataclass(frozen=True)
class Config:
    crawler: CrawlerConfig = field(default_factory=CrawlerConfig)
    fetcher: FetcherConfig = field(default_factory=FetcherConfig)
    politeness: PolitenessConfig = field(default_factory=PolitenessConfig)
    frontier: FrontierConfig = field(default_factory=FrontierConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    notification: NotificationConfig = field(default_factory=NotificationConfig)


def _make_dc(cls, data: dict | None):
    """Instantiate a dataclass from a dict, ignoring unknown keys."""
    if not data:
        return cls()
    import dataclasses
    known = {f.name for f in dataclasses.fields(cls)}
    return cls(**{k: v for k, v in data.items() if k in known})


def load_config(path: str | Path) -> Config:
    """Load configuration from a YAML file."""
    path = Path(path)
    if not path.exists():
        return Config()
    with open(path, "r") as f:
        raw = yaml.safe_load(f) or {}
    return Config(
        crawler=_make_dc(CrawlerConfig, raw.get("crawler")),
        fetcher=_make_dc(FetcherConfig, raw.get("fetcher")),
        politeness=_make_dc(PolitenessConfig, raw.get("politeness")),
        frontier=_make_dc(FrontierConfig, raw.get("frontier")),
        storage=_make_dc(StorageConfig, raw.get("storage")),
        logging=_make_dc(LoggingConfig, raw.get("logging")),
        notification=_make_dc(NotificationConfig, raw.get("notification")),
    )
