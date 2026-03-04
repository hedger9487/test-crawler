from pathlib import Path

from crawler.utils.config import load_config


def test_load_config_returns_defaults_when_file_missing(tmp_path: Path):
    config = load_config(tmp_path / "missing.yaml")
    assert config.crawler.concurrency == 200
    assert config.frontier.max_pending == 5_000_000
    assert config.notification.enabled is False
    assert config.sitemap.max_queue_per_sitemap == 500


def test_load_config_parses_known_keys_and_ignores_unknown(tmp_path: Path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
crawler:
  concurrency: 321
  unknown_field: ignored
frontier:
  max_pending: 999
notification:
  enabled: true
  discord_webhook: "https://example.com/webhook"
sitemap:
  max_queue_per_sitemap: 321
  fetch_retries_per_sitemap: 4
  unknown_sitemap_field: ignored
nonexistent:
  x: y
""".strip()
    )

    config = load_config(config_path)
    assert config.crawler.concurrency == 321
    assert config.frontier.max_pending == 999
    assert config.notification.enabled is True
    assert config.notification.discord_webhook == "https://example.com/webhook"
    assert config.sitemap.max_queue_per_sitemap == 321
    assert config.sitemap.fetch_retries_per_sitemap == 4
