"""Crash notification via Discord webhook.

Sends a Discord message when the crawler crashes unexpectedly.
No authentication needed — just a webhook URL.
"""

from __future__ import annotations

import json
import logging
import socket
import traceback
import urllib.request
import urllib.error
from datetime import datetime

logger = logging.getLogger(__name__)


def send_crash_notification(
    error: Exception,
    webhook_url: str,
    extra_info: str = "",
) -> bool:
    """Send a crash notification to Discord.

    Args:
        error: The exception that caused the crash.
        webhook_url: Discord webhook URL.
        extra_info: Optional extra context to include.

    Returns:
        True if notification was sent successfully.
    """
    if not webhook_url:
        logger.warning(
            "Cannot send crash notification: no webhook URL configured. "
            "Set 'notification.discord_webhook' in config.yaml"
        )
        return False

    hostname = socket.gethostname()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    tb = traceback.format_exception(type(error), error, error.__traceback__)
    tb_str = "".join(tb)

    # Truncate traceback if too long for Discord (2000 char limit per field)
    if len(tb_str) > 1500:
        tb_str = tb_str[:750] + "\n... (truncated) ...\n" + tb_str[-750:]

    payload = {
        "embeds": [{
            "title": "🚨 Crawler CRASHED",
            "color": 0xFF0000,  # Red
            "fields": [
                {"name": "🖥 Host", "value": hostname, "inline": True},
                {"name": "🕐 Time", "value": now, "inline": True},
                {
                    "name": "❌ Error",
                    "value": f"`{type(error).__name__}: {str(error)[:200]}`",
                    "inline": False,
                },
                {
                    "name": "📋 Traceback",
                    "value": f"```python\n{tb_str}\n```",
                    "inline": False,
                },
            ],
            "footer": {"text": "HW1-Crawler Auto Notification"},
        }]
    }

    if extra_info:
        payload["embeds"][0]["fields"].insert(3, {
            "name": "📝 Extra Info",
            "value": extra_info[:1000],
            "inline": False,
        })

    data = json.dumps(payload).encode("utf-8")

    try:
        req = urllib.request.Request(
            webhook_url,
            data=data,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "HW1-Crawler/1.0",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status in (200, 204):
                logger.info("📢 Crash notification sent to Discord")
                return True
            else:
                logger.error("Discord webhook returned status %d", resp.status)
                return False
    except Exception as e:
        logger.error("Failed to send Discord notification: %s", e)
        return False


def send_status_notification(
    webhook_url: str,
    title: str,
    message: str,
    color: int = 0x00FF00,  # Green
) -> bool:
    """Send a general status notification to Discord."""
    if not webhook_url:
        return False

    payload = {
        "embeds": [{
            "title": title,
            "description": message,
            "color": color,
            "footer": {"text": "HW1-Crawler"},
        }]
    }

    data = json.dumps(payload).encode("utf-8")

    try:
        req = urllib.request.Request(
            webhook_url,
            data=data,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "HW1-Crawler/1.0",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10):
            return True
    except Exception:
        return False
